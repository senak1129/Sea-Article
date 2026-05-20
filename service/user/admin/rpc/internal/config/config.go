package config

import (
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	zrpc.RpcServerConf
	DataSource string
	System     struct {
		DefaultPassword string
	}
	BizRedis struct {
		Mode     string   `json:",optional"`
		Host     string   `json:",optional"`
		Sentinel []string `json:",optional"`
		Master   string   `json:",optional"`
		Pass     string   `json:",optional"`
		ReadOnly bool     `json:",optional"`
	}
	AdminAuth struct {
		AccessSecret string
		AccessExpire int64
	}
}
