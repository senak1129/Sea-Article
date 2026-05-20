package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func InitRedisStandalone(host, password string) (redis.UniversalClient, error) {
	if host == "" {
		return nil, fmt.Errorf("redis host is empty")
	}
	rdb := redis.NewClient(&redis.Options{Addr: host, Password: password})
	if err := ping(rdb); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping failed, host=%s: %w", host, err)
	}
	return rdb, nil
}

func InitRedisSentinel(sentinelAddrs []string, masterName, password string, readOnly bool) (redis.UniversalClient, error) {
	if len(sentinelAddrs) == 0 {
		return nil, fmt.Errorf("redis sentinel addrs is empty")
	}
	if masterName == "" {
		return nil, fmt.Errorf("redis sentinel master name is empty")
	}
	rdb := redis.NewFailoverClusterClient(&redis.FailoverOptions{
		MasterName:     masterName,
		SentinelAddrs:  sentinelAddrs,
		Password:       password,
		RouteByLatency: true,
	})
	if err := ping(rdb); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis ping failed, mode=sentinel, master=%s, sentinel=%v: %w", masterName, sentinelAddrs, err)
	}
	return rdb, nil
}

func ping(rdb redis.UniversalClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return rdb.Ping(ctx).Err()
}
