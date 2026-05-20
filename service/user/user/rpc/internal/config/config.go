package config

import (
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	Postgres struct {
		Host     string
		Port     string
		User     string
		Password string
		DBName   string
		Mode     string
	}
	BizRedis struct {
		Mode     string   `json:",optional"`
		Host     string   `json:",optional"`
		Sentinel []string `json:",optional"`
		Master   string   `json:",optional"`
		Pass     string   `json:",optional"`
		ReadOnly bool     `json:",optional"`
	}
	UserAuth struct {
		AccessSecret string
		AccessExpire int64
	}
}
