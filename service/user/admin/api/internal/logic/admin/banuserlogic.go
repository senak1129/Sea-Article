// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package admin

import (
	"context"
	"encoding/json"
	"sea-try-go/service/common/logger"
	"sea-try-go/service/user/admin/api/internal/metrics"
	"sea-try-go/service/user/admin/api/internal/svc"
	"sea-try-go/service/user/admin/api/internal/types"
	"sea-try-go/service/user/admin/rpc/pb"
	"sea-try-go/service/user/common/errmsg"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BanuserLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewBanuserLogic(ctx context.Context, svcCtx *svc.ServiceContext) *BanuserLogic {
	return &BanuserLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *BanuserLogic) Banuser(req *types.BanUserReq) (resp *types.BanUserResp, code int) {
	var adminUid int64
	if uidNumber, ok := l.ctx.Value("uid").(json.Number); ok {
		adminUid, _ = uidNumber.Int64()
	}
	uid := req.Uid
	rpcReq := &pb.BanUserReq{
		Uid:        uid,
		OperatorId: adminUid,
	}
	rpcResp, err := l.svcCtx.AdminRpc.BanUser(l.ctx, rpcReq)
	if err != nil {
		metrics.AdminApiInterceptCount.WithLabelValues("/admin/banuser", "rpc_error").Inc()
		logger.LogBusinessErr(l.ctx, errmsg.Error, err)
		st, _ := status.FromError(err)
		switch st.Code() {
		case codes.NotFound:
			return nil, errmsg.ErrorUserNotExist
		case codes.Internal:
			return nil, errmsg.ErrorServerCommon
		default:
			return nil, errmsg.CodeServerBusy
		}
	}
	metrics.AdminApiInterceptCount.WithLabelValues("/admin/banuser", "success").Inc()
	return &types.BanUserResp{
		Success: rpcResp.Success,
	}, errmsg.Success
}
