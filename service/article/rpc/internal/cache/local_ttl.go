package cache

import (
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// TTLCache is a generic local cache backed by ristretto with per-key TTL support.
type TTLCache[T any] struct {
	c *ristretto.Cache[string, T]
}

// NewTTLCache creates a ristretto-backed TTL cache.
// maxSize is the maximum number of items the cache can hold.
func NewTTLCache[T any](maxSize int64) (*TTLCache[T], error) {
	c, err := ristretto.NewCache(&ristretto.Config[string, T]{
		NumCounters: maxSize * 10, // recommended 10x the max number of keys
		MaxCost:     maxSize,      // each item costs 1 by default
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}
	return &TTLCache[T]{c: c}, nil
}

func (c *TTLCache[T]) Get(key string) (T, bool) {
	return c.c.Get(key)
}

// Set writes to the cache asynchronously. The item may not be immediately visible
// to subsequent reads due to ristretto's write buffer.
func (c *TTLCache[T]) Set(key string, val T, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	c.c.SetWithTTL(key, val, 1, ttl)
}

// SetSync writes to the cache and blocks until the item is visible to reads.
// Use this when read-after-write consistency is required (e.g. backfilling from Redis).
func (c *TTLCache[T]) SetSync(key string, val T, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	c.c.SetWithTTL(key, val, 1, ttl)
	c.c.Wait()
}

func (c *TTLCache[T]) Del(key string) {
	c.c.Del(key)
}

func (c *TTLCache[T]) Clear() {
	c.c.Clear()
}

func (c *TTLCache[T]) Close() {
	c.c.Close()
}
