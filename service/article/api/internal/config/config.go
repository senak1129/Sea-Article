// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package config

import (
	"github.com/zeromicro/go-zero/rest"
	"github.com/zeromicro/go-zero/zrpc"
)

type Config struct {
	rest.RestConf
	Auth struct {
		AccessSecret string
		AccessExpire int64
	}
	ArticleRpcConf  zrpc.RpcClientConf
	SecurityRpcConf zrpc.RpcClientConf
	UserRpcConf     zrpc.RpcClientConf
	BizRedis        struct {
		Mode     string   `json:",optional"`
		Host     string   `json:",optional"`
		Sentinel []string `json:",optional"`
		Master   string   `json:",optional"`
		Pass     string   `json:",optional"`
		ReadOnly bool     `json:",optional"`
	}
	RateLimit struct {
		Rate  float64 `json:",default=10"`
		Burst int     `json:",default=20"`
	}
}
