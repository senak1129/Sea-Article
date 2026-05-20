package svc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sea-try-go/service/common/logger"
	"sea-try-go/service/common/snowflake"
	"sea-try-go/service/user/admin/rpc/internal/config"
	"sea-try-go/service/user/admin/rpc/internal/model"
	"sea-try-go/service/user/common/cryptx"

	"github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	defaultAdminUsername = "admin"
	defaultAdminPassword = "admin"
)

type ServiceContext struct {
	Config     config.Config
	AdminModel *model.AdminModel
	BizRedis   redis.UniversalClient
}

func NewServiceContext(c config.Config) *ServiceContext {
	db := model.InitDB(c.DataSource)
	svcCtx := &ServiceContext{
		Config:     c,
		AdminModel: model.NewAdminModel(db),
		BizRedis:   mustInitRedis(c),
	}
	logx.Must(svcCtx.ensureDefaultAdmin(context.Background()))
	return svcCtx
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

func (s *ServiceContext) ensureDefaultAdmin(ctx context.Context) error {
	_, err := s.AdminModel.FindOneAdminByUsername(ctx, defaultAdminUsername)
	if err == nil {
		return nil
	}
	if err != model.ErrorNotFound {
		return err
	}

	password, err := cryptx.PasswordEncrypt(defaultAdminPassword)
	if err != nil {
		return err
	}

	uid, err := snowflake.GetID()
	if err != nil {
		return err
	}

	admin := &model.Admin{
		Uid:      uid,
		Username: defaultAdminUsername,
		Password: password,
	}
	if err = s.AdminModel.InsertOneAdmin(ctx, admin); err != nil {
		if model.IsUniqueViolation(err) {
			_, lookupErr := s.AdminModel.FindOneAdminByUsername(ctx, defaultAdminUsername)
			if lookupErr == nil {
				return nil
			}
		}
		return err
	}

	logger.LogInfo(ctx, "default admin account bootstrapped")
	return nil
}
