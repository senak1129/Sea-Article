package cache

import (
	"testing"
	"time"
)

// ristretto 的 Set 是异步写入，测试里用底层 Wait() 等写入刷新生效，避免偶发 miss。
func newTestCache[T any](t *testing.T, maxSize int64) *TTLCache[T] {
	t.Helper()

	c, err := NewTTLCache[T](maxSize)
	if err != nil {
		t.Fatalf("NewTTLCache() error = %v", err)
	}
	t.Cleanup(c.Close)

	return c
}

func TestTTLCacheSetGet(t *testing.T) {
	c := newTestCache[string](t, 100)

	if _, ok := c.Get("missing"); ok {
		t.Error("读取未写入的 key 应 miss")
	}

	c.Set("k1", "v1", time.Minute)
	c.c.Wait()

	got, ok := c.Get("k1")
	if !ok {
		t.Fatal("刚写入的 key 应命中")
	}
	if got != "v1" {
		t.Errorf("Get(%q) = %q, want %q", "k1", got, "v1")
	}
}

// ttl <= 0 时内部按默认 5 分钟处理，条目仍应立即可读。
func TestTTLCacheZeroTTLFallsBackToDefault(t *testing.T) {
	c := newTestCache[int](t, 100)

	c.Set("k", 42, 0)
	c.c.Wait()

	got, ok := c.Get("k")
	if !ok || got != 42 {
		t.Errorf("Get(%q) = (%d, %v), want (42, true)", "k", got, ok)
	}
}

func TestTTLCacheExpires(t *testing.T) {
	c := newTestCache[string](t, 100)

	c.Set("k", "v", 30*time.Millisecond)
	c.c.Wait()

	if _, ok := c.Get("k"); !ok {
		t.Fatal("TTL 未到期前应命中")
	}

	time.Sleep(150 * time.Millisecond)

	if _, ok := c.Get("k"); ok {
		t.Error("TTL 到期后应 miss")
	}
}

func TestTTLCacheDel(t *testing.T) {
	c := newTestCache[string](t, 100)

	c.Set("k", "v", time.Minute)
	c.c.Wait()
	if _, ok := c.Get("k"); !ok {
		t.Fatal("删除前应命中")
	}

	c.Del("k")
	c.c.Wait()

	if _, ok := c.Get("k"); ok {
		t.Error("删除后应 miss")
	}
}

func TestTTLCacheClear(t *testing.T) {
	c := newTestCache[string](t, 100)

	c.Set("a", "1", time.Minute)
	c.Set("b", "2", time.Minute)
	c.c.Wait()

	c.Clear()

	if _, ok := c.Get("a"); ok {
		t.Error("Clear 后 key a 应 miss")
	}
	if _, ok := c.Get("b"); ok {
		t.Error("Clear 后 key b 应 miss")
	}
}

func TestNewTTLCacheInvalidSize(t *testing.T) {
	// NumCounters = maxSize*10 必须为正，非法尺寸应返回错误而非 panic。
	if _, err := NewTTLCache[string](0); err == nil {
		t.Error("NewTTLCache(0) 期望返回错误")
	}
}
