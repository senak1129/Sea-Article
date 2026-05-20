package logic

import (
	"context"
	"fmt"
	"time"

	"sea-try-go/service/common/logger"
	"sea-try-go/service/user/common/errmsg"
	"sea-try-go/service/user/user/rpc/internal/svc"
	"sea-try-go/service/user/user/rpc/pb"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type LogoutLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewLogoutLogic(ctx context.Context, svcCtx *svc.ServiceContext) *LogoutLogic {
	return &LogoutLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *LogoutLogic) Logout(in *pb.LogoutReq) (*pb.LogoutResp, error) {
	if in.Token == "" {
		logger.LogBusinessErr(l.ctx, errmsg.ErrorTokenTypeWrong, fmt.Errorf("Token为空"))
		return nil, status.Error(codes.InvalidArgument, "Token不能为空")
	}
	blackListKey := fmt.Sprintf("user:jwt_blacklist:%s", in.Token)
	expireTime := l.svcCtx.Config.UserAuth.AccessExpire
	err := l.svcCtx.BizRedis.Set(l.ctx, blackListKey, "banned", time.Duration(expireTime)*time.Second).Err()
	if err != nil {
		logger.LogBusinessErr(l.ctx, errmsg.ErrorRedisUpdate, err)
		return nil, status.Error(codes.Internal, "注销失败,Redis写入异常")
	}
	logger.LogInfo(l.ctx, "logout success,token blacklisted!")
	return &pb.LogoutResp{}, nil
}
