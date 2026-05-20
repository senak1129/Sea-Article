// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package svc

import (
	"context"
	"sea-try-go/service/article/api/internal/config"
	"sea-try-go/service/article/api/internal/middleware"
	"sea-try-go/service/article/rpc/articleservice"
	"sea-try-go/service/security/rpc/client/imagesecurityservice"
	"sea-try-go/service/user/user/rpc/userservice"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type ServiceContext struct {
	Config              config.Config
	ArticleRpc          articleservice.ArticleService
	SecurityRpc         imagesecurityservice.ImageSecurityService
	UserRpc             userservice.UserService
	RateLimitMiddleware rest.Middleware
}

func NewServiceContext(c config.Config) *ServiceContext {
	var rds redis.UniversalClient
	if c.BizRedis.Mode == "sentinel" && len(c.BizRedis.Sentinel) > 0 {
		// Redis Sentinel with go-redis
		rds = redis.NewFailoverClusterClient(&redis.FailoverOptions{
			MasterName:    c.BizRedis.Master,
			SentinelAddrs: c.BizRedis.Sentinel,
			Password:      c.BizRedis.Pass,
			RouteByLatency: true, // Enable read/write splitting
		})
	} else {
		// Single node fallback
		rds = redis.NewClient(&redis.Options{
			Addr:     c.BizRedis.Host,
			Password: c.BizRedis.Pass,
		})
	}

	// Verify connection
	if err := rds.Ping(context.Background()).Err(); err != nil {
		panic("Failed to connect to Redis: " + err.Error())
	}

	rl, err := middleware.NewRateLimitMiddleware(c.RateLimit.Rate, c.RateLimit.Burst, rds)
	if err != nil {
		panic("Failed to load rate limit script: " + err.Error())
	}

	return &ServiceContext{
		Config:              c,
		ArticleRpc:          articleservice.NewArticleService(zrpc.MustNewClient(c.ArticleRpcConf)),
		SecurityRpc:         imagesecurityservice.NewImageSecurityService(zrpc.MustNewClient(c.SecurityRpcConf)),
		UserRpc:             userservice.NewUserService(zrpc.MustNewClient(c.UserRpcConf)),
		RateLimitMiddleware: rl.Handle,
	}
}
