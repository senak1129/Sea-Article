package svc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sea-try-go/service/user/user/rpc/internal/config"
	"sea-try-go/service/user/user/rpc/internal/model"

	"github.com/redis/go-redis/v9"
)

type ServiceContext struct {
	Config    config.Config
	UserModel *model.UserModel
	BizRedis  redis.UniversalClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	dbConfig := model.DBConf{
		Host:     c.Postgres.Host,
		Port:     c.Postgres.Port,
		User:     c.Postgres.User,
		Password: c.Postgres.Password,
		DBName:   c.Postgres.DBName,
		Mode:     c.Postgres.Mode,
	}
	db := model.InitDB(dbConfig)
	return &ServiceContext{
		Config:    c,
		UserModel: model.NewUserModel(db),
		BizRedis:  mustInitRedis(c),
	}
}

func mustInitRedis(c config.Config) redis.UniversalClient {
	rdb, err := initRedis(c)
	if err != nil {
		panic(err)
	}
	return rdb
}

func initRedis(c config.Config) (redis.UniversalClient, error) {
	mode := strings.ToLower(strings.TrimSpace(c.BizRedis.Mode))
	switch mode {
	case "", "standalone", "single":
		host := strings.TrimSpace(c.BizRedis.Host)
		if host == "" {
			return nil, fmt.Errorf("redis host is empty")
		}
		rdb := redis.NewClient(&redis.Options{Addr: host, Password: strings.TrimSpace(c.BizRedis.Pass)})
		if err := ping(rdb); err != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("redis ping failed, host=%s: %w", host, err)
		}
		return rdb, nil
	case "sentinel":
		var addrs []string
		for _, addr := range c.BizRedis.Sentinel {
			if s := strings.TrimSpace(addr); s != "" {
				addrs = append(addrs, s)
			}
		}
		master := strings.TrimSpace(c.BizRedis.Master)
		if len(addrs) == 0 {
			return nil, fmt.Errorf("redis sentinel addrs is empty")
		}
		if master == "" {
			return nil, fmt.Errorf("redis sentinel master name is empty")
		}
		rdb := redis.NewFailoverClusterClient(&redis.FailoverOptions{
			MasterName:     master,
			SentinelAddrs:  addrs,
			Password:       strings.TrimSpace(c.BizRedis.Pass),
			RouteByLatency: true,
		})
		if err := ping(rdb); err != nil {
			_ = rdb.Close()
			return nil, fmt.Errorf("redis ping failed, mode=sentinel, master=%s, sentinel=%v: %w", master, addrs, err)
		}
		return rdb, nil
	default:
		return nil, fmt.Errorf("unsupported BizRedis.Mode: %s", c.BizRedis.Mode)
	}
}

func ping(rdb redis.UniversalClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return rdb.Ping(ctx).Err()
}
