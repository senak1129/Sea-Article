package cache

import (
	"context"
	"sync"
	"time"
)

type entry[T any] struct {
	val    T
	expire time.Time
}

type TTLCache[T any] struct {
	m sync.Map
}

func (c *TTLCache[T]) Get(key string) (T, bool) {
	v, ok := c.m.Load(key)
	if !ok {
		var zero T
		return zero, false
	}
	en := v.(entry[T])
	if time.Now().After(en.expire) {
		c.m.Delete(key)
		var zero T
		return zero, false
	}
	return en.val, true
}

func (c *TTLCache[T]) Set(key string, val T, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	c.m.Store(key, entry[T]{val: val, expire: time.Now().Add(ttl)})
}

func (c *TTLCache[T]) Del(key string) {
	c.m.Delete(key)
}

func (c *TTLCache[T]) Clear() {
	c.m = sync.Map{}
}

// StartCleanup 启动后台定时清理任务，解决过期 Key 永远不被访问导致的内存泄漏
func (c *TTLCache[T]) StartCleanup(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				now := time.Now()
				c.m.Range(func(key, value any) bool {
					en := value.(entry[T])
					if now.After(en.expire) {
						c.m.Delete(key)
					}
					return true
				})
			}
		}
	}()
}
