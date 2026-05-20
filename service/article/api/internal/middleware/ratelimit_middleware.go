package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const rateLimitScript = `
local tokens_key = KEYS[1]
local timestamp_key = KEYS[2]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])

local fill_time = capacity/rate
local ttl = math.floor(fill_time*2)
if ttl < 1 then
  ttl = 1
end

local last_tokens = tonumber(redis.call("get", tokens_key))
if last_tokens == nil then
  last_tokens = capacity
end

local last_refreshed = tonumber(redis.call("get", timestamp_key))
if last_refreshed == nil then
  last_refreshed = 0
end

local delta_ms = math.max(0, now-last_refreshed)
local delta_sec = delta_ms / 1000.0
local filled_tokens = math.min(capacity, last_tokens+(delta_sec*rate))
local allowed = filled_tokens >= requested

if allowed then
  redis.call("setex", tokens_key, ttl, filled_tokens - requested)
  redis.call("setex", timestamp_key, ttl, now)
  return {1, 0}
else
  local wait_sec = math.ceil((requested - filled_tokens) / rate)
  return {0, wait_sec}
end
`

type RateLimitMiddleware struct {
	Redis     redis.UniversalClient
	Rate      float64
	Burst     int
	scriptSHA string
}

func NewRateLimitMiddleware(rate float64, burst int, redisClient redis.UniversalClient) (*RateLimitMiddleware, error) {
	sha, err := redisClient.ScriptLoad(context.Background(), rateLimitScript).Result()
	if err != nil {
		return nil, fmt.Errorf("load rate limit script: %w", err)
	}
	return &RateLimitMiddleware{
		Redis:     redisClient,
		Rate:      rate,
		Burst:     burst,
		scriptSHA: sha,
	}, nil
}

func (m *RateLimitMiddleware) Handle(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 严格基于 JWT 解析出的 userId 进行限流，不进行 IP 降级
		var userIdStr string
		if uid := r.Context().Value("userId"); uid != nil {
			switch value := uid.(type) {
			case string:
				userIdStr = value
			default:
				userIdStr = fmt.Sprintf("%v", value)
			}
		}

		if userIdStr == "" {
			// 如果没有拿到 userId（理论上被 jwt 中间件拦截，这里是兜底防御）
			w.Header().Set("Content-Type", "application/json;charset=utf-8")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"code":401, "msg":"未授权访问"}`))
			return
		}

		identifier := fmt.Sprintf("user:%v", userIdStr)
		tokensKey := fmt.Sprintf("req_limit:tokens:%s", identifier)
		timestampKey := fmt.Sprintf("req_limit:ts:%s", identifier)

		now := time.Now().UnixMilli()

		result, err := m.evalScript(r.Context(), []string{tokensKey, timestampKey}, m.Rate, m.Burst, now, 1)
		if err != nil {
			// Redis 执行失败（如网络抖动、哨兵切换），不降级放行，直接拒绝请求
			logx.WithContext(r.Context()).Errorf("rate limit eval error (fail closed): %v", err)
			w.Header().Set("Content-Type", "application/json;charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":500, "msg":"系统繁忙，请稍后再试"}`))
			return
		}

		res, ok := result.([]interface{})
		if !ok || len(res) < 2 {
			logx.WithContext(r.Context()).Errorf("rate limit unexpected result: %T %v", result, result)
			w.Header().Set("Content-Type", "application/json;charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"code":500, "msg":"系统繁忙，请稍后再试"}`))
			return
		}

		if res[0].(int64) == 1 {
			next(w, r)
		} else {
			waitSec := res[1].(int64)
			w.Header().Set("Content-Type", "application/json;charset=utf-8")
			w.Header().Set("Retry-After", strconv.FormatInt(waitSec, 10))
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"code":429, "msg":"请求过于频繁，请稍后再试"}`))
		}
	}
}

// evalScript 优先走 EvalSha（减少网络传输），仅在脚本被 Redis 驱逐时回退到 Eval。
func (m *RateLimitMiddleware) evalScript(ctx context.Context, keys []string, args ...interface{}) (interface{}, error) {
	result, err := m.Redis.EvalSha(ctx, m.scriptSHA, keys, args...).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		return m.Redis.Eval(ctx, rateLimitScript, keys, args...).Result()
	}
	return result, err
}
