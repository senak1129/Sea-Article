package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	pb "sea-try-go/service/article/rpc/pb"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
	"golang.org/x/sync/singleflight"
)

const (
	defaultDetailTTL = 5 * time.Minute
	defaultListTTL   = 60 * time.Second
	defaultSearchTTL = 30 * time.Second
)
const invalidateChannel = "article:cache:invalidate"

type ArticleCache struct {
	rdb         redis.UniversalClient
	sf          singleflight.Group
	localDetail *TTLCache[*pb.Article]
	localList   *TTLCache[*pb.ListArticlesResponse]
	cancel      context.CancelFunc
}

func NewArticleCache(ctx context.Context, rdb redis.UniversalClient) (*ArticleCache, error) {
	detailCache, err := NewTTLCache[*pb.Article](10000)
	if err != nil {
		return nil, fmt.Errorf("init detail cache: %w", err)
	}
	listCache, err := NewTTLCache[*pb.ListArticlesResponse](5000)
	if err != nil {
		return nil, fmt.Errorf("init list cache: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	subCtx, cancel := context.WithCancel(ctx)
	c := &ArticleCache{
		rdb:         rdb,
		localDetail: detailCache,
		localList:   listCache,
		cancel:      cancel,
	}
	c.startInvalidateSubscriber(subCtx)
	return c, nil
}

func detailKey(id string) string {
	return fmt.Sprintf("article:detail:%s", id)
}

func listKey(in *pb.ListArticlesRequest) string {
	return fmt.Sprintf("article:list:%s:%s:%s:%d:%d:%s:%t",
		in.GetManualTypeTag(), in.GetSecondaryTag(), in.GetAuthorId(),
		in.GetPage(), in.GetPageSize(), in.GetSortBy(), in.GetDesc(),
	)
}

func (c *ArticleCache) GetDetail(ctx context.Context, id string) (*pb.Article, error) {
	if c == nil || c.rdb == nil || id == "" {
		return nil, nil
	}
	if v, ok := c.localDetail.Get(detailKey(id)); ok {
		return v, nil
	}
	if ok, err := c.IsNilDetail(ctx, id); ok {
		return nil, nil
	} else if err != nil {
		logx.Errorf("IsNilDetail failed, id: %s, err: %v", id, err)
	}
	val, err := c.rdb.Get(ctx, detailKey(id)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	var a pb.Article
	if err := json.Unmarshal([]byte(val), &a); err != nil {
		return nil, err
	}
	c.localDetail.SetSync(detailKey(id), &a, defaultDetailTTL)
	return &a, nil
}

func (c *ArticleCache) SetDetail(ctx context.Context, id string, a *pb.Article, ttl time.Duration) error {
	if c == nil || c.rdb == nil || id == "" || a == nil {
		logx.Errorf("SetDetail aborted due to invalid args: c is nil? %v, rdb is nil? %v, id empty? %v, a is nil? %v", c == nil, c != nil && c.rdb == nil, id == "", a == nil)
		return nil
	}
	if ttl <= 0 {
		ttl = defaultDetailTTL
	}
	b, err := json.Marshal(a)
	if err != nil {
		logx.Errorf("SetDetail json marshal failed for id: %s, err: %v", id, err)
		return err
	}
	if err := c.rdb.Set(ctx, detailKey(id), b, ttl).Err(); err != nil {
		logx.Errorf("SetDetail redis set failed for id: %s, err: %v", id, err)
		return err
	}
	c.localDetail.Set(detailKey(id), a, ttl)
	// 通知其他实例清除本地缓存
	if err := c.rdb.Publish(ctx, invalidateChannel, detailKey(id)).Err(); err != nil {
		logx.Errorf("publish invalidate detail channel failed, id: %s, err: %v", id, err)
	}
	return nil
}

func (c *ArticleCache) DelDetail(ctx context.Context, id string) {
	if c == nil || c.rdb == nil || id == "" {
		return
	}
	if err := c.rdb.Del(ctx, detailKey(id)).Err(); err != nil {
		logx.Errorf("del detail cache failed, id: %s, err: %v", id, err)
	}
	c.localDetail.Del(detailKey(id))
	if err := c.rdb.Publish(ctx, invalidateChannel, detailKey(id)).Err(); err != nil {
		logx.Errorf("publish invalidate detail channel failed, id: %s, err: %v", id, err)
	}
}

func (c *ArticleCache) GetList(ctx context.Context, in *pb.ListArticlesRequest) (*pb.ListArticlesResponse, error) {
	if c == nil || c.rdb == nil || in == nil {
		return nil, nil
	}
	key := listKey(in)
	if v, ok := c.localList.Get(key); ok {
		return v, nil
	}
	val, err := c.rdb.Get(ctx, listKey(in)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	var resp pb.ListArticlesResponse
	if err := json.Unmarshal([]byte(val), &resp); err != nil {
		return nil, err
	}
	c.localList.SetSync(key, &resp, defaultListTTL)
	return &resp, nil
}

func (c *ArticleCache) SetList(ctx context.Context, in *pb.ListArticlesRequest, resp *pb.ListArticlesResponse, ttl time.Duration) error {
	if c == nil || c.rdb == nil || in == nil || resp == nil {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultListTTL
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	key := listKey(in)
	if err := c.rdb.Set(ctx, key, b, ttl).Err(); err != nil {
		return err
	}
	c.localList.Set(key, resp, ttl)
	// 通知其他实例清除本地缓存
	if err := c.rdb.Publish(ctx, invalidateChannel, key).Err(); err != nil {
		logx.Errorf("publish invalidate list channel failed, key: %s, err: %v", key, err)
	}
	return nil
}

func (c *ArticleCache) DelList(ctx context.Context, in *pb.ListArticlesRequest) {
	if c == nil || c.rdb == nil || in == nil {
		return
	}
	key := listKey(in)
	if err := c.rdb.Del(ctx, key).Err(); err != nil {
		logx.Errorf("del list cache failed, key: %s, err: %v", key, err)
	}
	c.localList.Del(key)
	if err := c.rdb.Publish(ctx, invalidateChannel, key).Err(); err != nil {
		logx.Errorf("publish invalidate list channel failed, key: %s, err: %v", key, err)
	}
}

func (c *ArticleCache) GetOrLoadDetail(ctx context.Context, id string, ttl time.Duration, loader func() (*pb.Article, error)) (*pb.Article, error) {
	if v, err := c.GetDetail(ctx, id); err == nil && v != nil {
		return v, nil
	}
	key := "detail:" + id
	v, err, _ := c.sf.Do(key, func() (interface{}, error) {
		if v, err := c.GetDetail(ctx, id); err == nil && v != nil {
			return v, nil
		}
		if ok, err := c.IsNilDetail(ctx, id); ok {
			return nil, nil
		} else if err != nil {
			logx.Errorf("IsNilDetail failed in GetOrLoadDetail, id: %s, err: %v", id, err)
		}
		a, e := loader()
		if e != nil {
			return nil, e
		}
		if a == nil {
			if err := c.SetNilDetail(ctx, id, 10*time.Second); err != nil {
				logx.Errorf("set nil detail cache failed, id: %s, err: %v", id, err)
			}
			return nil, nil
		}
		jitterTTL := jitter(ttl)
		if err := c.SetDetail(ctx, id, a, jitterTTL); err != nil {
			logx.Errorf("set detail cache failed, id: %s, err: %v", id, err)
		} else {
			logx.Infof("set detail cache SUCCESS for id: %s, ttl: %v", id, jitterTTL)
		}
		return a, nil
	})
	if err != nil {
		logx.Errorf("GetOrLoadDetail singleflight do failed, id: %s, err: %v", id, err)
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	a, ok := v.(*pb.Article)
	if !ok {
		return nil, fmt.Errorf("type assert failed")
	}
	return a, nil
}

func (c *ArticleCache) SetNilList(ctx context.Context, in *pb.ListArticlesRequest, ttl time.Duration) error {
	if c == nil || c.rdb == nil || in == nil {
		return nil
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return c.rdb.Set(ctx, listKey(in)+":nil", "1", ttl).Err()
}

func (c *ArticleCache) IsNilList(ctx context.Context, in *pb.ListArticlesRequest) (bool, error) {
	if c == nil || c.rdb == nil || in == nil {
		return false, nil
	}
	val, err := c.rdb.Get(ctx, listKey(in)+":nil").Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return val == "1", nil
}

func (c *ArticleCache) GetOrLoadList(ctx context.Context, in *pb.ListArticlesRequest, ttl time.Duration, loader func() (*pb.ListArticlesResponse, error)) (*pb.ListArticlesResponse, error) {
	if v, err := c.GetList(ctx, in); err == nil && v != nil {
		return v, nil
	}
	if ok, err := c.IsNilList(ctx, in); ok {
		return nil, nil
	} else if err != nil {
		logx.Errorf("IsNilList failed, in: %v, err: %v", in, err)
	}
	key := "list:" + listKey(in)
	v, err, _ := c.sf.Do(key, func() (interface{}, error) {
		if v, err := c.GetList(ctx, in); err == nil && v != nil {
			return v, nil
		}
		if ok, err := c.IsNilList(ctx, in); ok {
			return nil, nil
		} else if err != nil {
			logx.Errorf("IsNilList failed in GetOrLoadList, in: %v, err: %v", in, err)
		}
		resp, e := loader()
		if e != nil {
			return nil, e
		}
		if resp == nil {
			if err := c.SetNilList(ctx, in, 10*time.Second); err != nil {
				logx.Errorf("set nil list cache failed, in: %v, err: %v", in, err)
			}
			return nil, nil
		}
		jitterTTL := jitter(ttl)
		if err := c.SetList(ctx, in, resp, jitterTTL); err != nil {
			logx.Errorf("set list cache failed, err: %v", err)
		}
		return resp, nil
	})
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	resp, ok := v.(*pb.ListArticlesResponse)
	if !ok {
		return nil, fmt.Errorf("type assert failed")
	}
	return resp, nil
}

func (c *ArticleCache) SetNilDetail(ctx context.Context, id string, ttl time.Duration) error {
	if c == nil || c.rdb == nil || id == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = 10 * time.Second
	}
	return c.rdb.Set(ctx, detailKey(id)+":nil", "1", ttl).Err()
}

func (c *ArticleCache) IsNilDetail(ctx context.Context, id string) (bool, error) {
	if c == nil || c.rdb == nil || id == "" {
		return false, nil
	}
	val, err := c.rdb.Get(ctx, detailKey(id)+":nil").Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, err
	}
	return val == "1", nil
}

func (c *ArticleCache) Close() {
	if c == nil {
		return
	}
	if c.cancel != nil {
		c.cancel()
	}
	c.localDetail.Close()
	c.localList.Close()
}

func (c *ArticleCache) startInvalidateSubscriber(ctx context.Context) {
	if c.rdb == nil {
		return
	}

	go func() {
		pubsub := c.rdb.Subscribe(ctx, invalidateChannel)
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case m, ok := <-ch:
				if !ok {
					return
				}
				keys := strings.Split(m.Payload, ",")
				for _, k := range keys {
					if strings.HasPrefix(k, "article:detail:") {
						c.localDetail.Del(k)
					} else if strings.HasPrefix(k, "article:list:") {
						c.localList.Del(k)
					}
				}
			}
		}
	}()
}

func jitter(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return ttl
	}
	p := 0.1
	delta := time.Duration(float64(ttl) * p)
	if delta <= 0 {
		return ttl
	}
	return ttl + time.Duration(rand.Int63n(int64(delta))) - delta/2
}
