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
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// 优雅停机：退出前尝试最后一次刷新
				_, _ = c.flushBatch(context.Background(), repo, invalidate)
				return
			case <-ticker.C:
				for {
					hasMore, _ := c.flushBatch(ctx, repo, invalidate)
					if !hasMore {
						break
					}
					// 如果还有更多数据，不等待 ticker，直接处理下一批以提高吞吐量
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
		delta, err := c.popDelta(ctx, id)
		if err != nil {
			// popDelta 失败，但 ID 已经从 SSet 中 SPop 出来了
			// 为了不丢失这个 ID，如果 delta 还是 0，尝试加回去
			if saddErr := c.rdb.SAdd(ctx, viewDirtySetKey(), id).Err(); saddErr != nil {
				logx.Errorf("popDelta failed and restore dirty set failed, id: %s, err: %v, saddErr: %v", id, err, saddErr)
			} else {
				logx.Errorf("popDelta failed, restored to dirty set, id: %s, err: %v", id, err)
			}
			continue
		}
		if delta <= 0 {
			continue
		}
		if err := repo.AddViewCount(ctx, id, delta); err != nil {
			// 数据库更新失败，回退数据到 Redis
			logx.Errorf("db update failed, attempting rollback to redis, id: %s, err: %v", id, err)
			if incrErr := c.rdb.IncrBy(ctx, viewDeltaKey(id), delta).Err(); incrErr != nil {
				logx.Errorf("rollback incr failed, id: %s, delta: %d, incrErr: %v", id, delta, incrErr)
			}
			if saddErr := c.rdb.SAdd(ctx, viewDirtySetKey(), id).Err(); saddErr != nil {
				logx.Errorf("rollback sadd failed, id: %s, saddErr: %v", id, saddErr)
			}
			continue
		}
		if invalidate != nil {
			invalidate(ctx, id)
		}
	}
	return len(ids) == batchSize, nil
}

var popDeltaLua = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if (not v) or (tonumber(v) == 0) then
  redis.call('DEL', KEYS[1])
  redis.call('SREM', KEYS[2], ARGV[1])
  return 0
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[1])
return tonumber(v)
`)

/*
GET 增量值
    ↓
是空？或是 0？
    ↓
是的 → DEL key + SREM 集合 → return 0
    ↓
不是 → DEL key + SREM 集合 → return 真实增量
*/

func (c *ViewCounter) popDelta(ctx context.Context, articleId string) (int64, error) {
	res, err := popDeltaLua.Run(ctx, c.rdb, []string{viewDeltaKey(articleId), viewDirtySetKey()}, articleId).Result()
	if err != nil {
		return 0, err
	}
	switch v := res.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n, nil
	default:
		return 0, nil
	}
}

func viewDeltaKey(articleId string) string {
	return "article:view:delta:" + articleId
}

func viewDirtySetKey() string {
	return "article:view:dirty"
}
