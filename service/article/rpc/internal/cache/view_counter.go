package cache

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

type ViewCountRepo interface {
	AddViewCount(ctx context.Context, id string, delta int64) error
}

type ViewCounter struct {
	rdb redis.UniversalClient
}

func NewViewCounter(rdb redis.UniversalClient) *ViewCounter {
	return &ViewCounter{rdb: rdb}
}

func (c *ViewCounter) Incr(ctx context.Context, articleId string) (int64, error) {
	if c == nil || c.rdb == nil || articleId == "" {
		return 0, nil
	}
	pipe := c.rdb.Pipeline()
	incr := pipe.Incr(ctx, viewDeltaKey(articleId))
	pipe.Expire(ctx, viewDeltaKey(articleId), 24*time.Hour)
	pipe.SAdd(ctx, viewDirtySetKey(), articleId)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	return incr.Val(), nil
}

func (c *ViewCounter) GetDelta(ctx context.Context, articleId string) (int64, error) {
	if c == nil || c.rdb == nil || articleId == "" {
		return 0, nil
	}
	val, err := c.rdb.Get(ctx, viewDeltaKey(articleId)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, nil
	}
	return n, nil
}

func (c *ViewCounter) StartFlusher(ctx context.Context, repo ViewCountRepo, invalidate func(context.Context, string)) {
	if c == nil || c.rdb == nil || repo == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		interval := 5 * time.Second
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// 优雅停机：退出前尝试最后一次刷新
				_, _ = c.flushBatch(context.Background(), repo, invalidate)
				return
			case <-ticker.C:
				for {
					hasMore, err := c.flushBatch(ctx, repo, invalidate)
					if err != nil {
						// DB 故障：指数退避，避免无效重试击穿数据库
						interval *= 2
						if interval > 2*time.Minute {
							interval = 2 * time.Minute
						}
						ticker.Reset(interval)
						break
					}
					// 成功：重置为正常间隔
					if interval != 5*time.Second {
						interval = 5 * time.Second
						ticker.Reset(interval)
					}
					if !hasMore {
						break
					}
				}
			}
		}
	}()
}

func (c *ViewCounter) flushBatch(ctx context.Context, repo ViewCountRepo, invalidate func(context.Context, string)) (bool, error) {
	const batchSize = 200
	ids, err := c.rdb.SPopN(ctx, viewDirtySetKey(), batchSize).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}

	for _, id := range ids {
		// 第一步：读取增量（不删除 delta key）
		delta, err := c.readDelta(ctx, id)
		if err != nil {
			logx.Errorf("readDelta failed, restoring dirty set, id: %s, err: %v", id, err)
			c.rdb.SAdd(ctx, viewDirtySetKey(), id)
			continue
		}
		if delta <= 0 {
			// 无增量，清理脏标记
			c.rdb.SRem(ctx, viewDirtySetKey(), id)
			continue
		}
		// 第二步：写数据库（数据安全，可重试）
		if err := repo.AddViewCount(ctx, id, delta); err != nil {
			logx.Errorf("db update failed, will retry next tick, id: %s, delta: %d, err: %v", id, delta, err)
			// 不删 delta，不删 dirty，下个 tick 自动重试
			continue
		}
		// 第三步：DB 成功后，原子清理 delta + dirty（Lua 保证一致性）
		if err := c.ackDelta(ctx, id, delta); err != nil {
			logx.Errorf("ackDelta failed, id: %s, delta: %d, err: %v", id, delta, err)
			// 不致命：下次 flush 会重复写入，业务上浏览量 +1 是幂等可接受的
		}
		if invalidate != nil {
			invalidate(ctx, id)
		}
	}
	return len(ids) == batchSize, nil
}

// readDelta 仅读取增量值，不删除 delta key。
// 即使进程崩溃，delta 仍留在 Redis 中，下次 tick 可重试。
func (c *ViewCounter) readDelta(ctx context.Context, articleId string) (int64, error) {
	val, err := c.rdb.Get(ctx, viewDeltaKey(articleId)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, nil
		}
		return 0, err
	}
	n, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, nil
	}
	return n, nil
}

// ackDelta 在 DB 写入成功后调用，原子地从 delta key 中扣除已持久化的增量，并清理 dirty 标记。
// 使用 Lua 脚本保证：如果在 read 和 ack 之间有新 Incr，剩余增量不会丢失。
var ackDeltaLua = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then
  redis.call('SREM', KEYS[2], ARGV[2])
  return 0
end
local cur = tonumber(v)
local delta = tonumber(ARGV[1])
if cur <= delta then
  redis.call('DEL', KEYS[1])
else
  redis.call('DECRBY', KEYS[1], delta)
end
redis.call('SREM', KEYS[2], ARGV[2])
return cur - delta
`)

func (c *ViewCounter) ackDelta(ctx context.Context, articleId string, delta int64) error {
	_, err := ackDeltaLua.Run(ctx, c.rdb, []string{viewDeltaKey(articleId), viewDirtySetKey()}, delta, articleId).Result()
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

func viewDeltaKey(articleId string) string {
	return "article:view:delta:" + articleId
}

func viewDirtySetKey() string {
	return "article:view:dirty"
}
