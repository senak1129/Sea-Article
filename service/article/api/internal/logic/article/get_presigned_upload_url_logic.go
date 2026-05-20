// Code scaffolded by goctl. Safe to edit.
// goctl 1.9.2

package article

import (
	"context"
	"fmt"

	"sea-try-go/service/article/api/internal/svc"
	"sea-try-go/service/article/api/internal/types"
	"sea-try-go/service/article/common/errmsg"
	"sea-try-go/service/article/rpc/articleservice"
	"sea-try-go/service/common/logger"

	"github.com/zeromicro/go-zero/core/logx"
)

type GetPresignedUploadUrlLogic struct {
	logx.Logger
	ctx    context.Context
	svcCtx *svc.ServiceContext
}

func NewGetPresignedUploadUrlLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPresignedUploadUrlLogic {
	return &GetPresignedUploadUrlLogic{
		Logger: logx.WithContext(ctx),
		ctx:    ctx,
		svcCtx: svcCtx,
	}
}

func (l *GetPresignedUploadUrlLogic) GetPresignedUploadUrl(req *types.GetPresignedUploadUrlReq) (resp *types.GetPresignedUploadUrlResp, code int) {
	authorId, code := extractCurrentUserID(l.ctx)
	if code != errmsg.Success {
		logger.LogBusinessErr(l.ctx, code, fmt.Errorf("missing login userId in upload url context"))
		return nil, code
	}

	rpcResp, err := l.svcCtx.ArticleRpc.GetPresignedUploadUrl(l.ctx, &articleservice.GetPresignedUploadUrlRequest{
		FileName:    req.FileName,
		ContentType: req.ContentType,
		AuthorId:    authorId,
	})
	if err != nil {
		logger.LogBusinessErr(l.ctx, errmsg.ErrorServerCommon, err)
		return nil, errmsg.ErrorServerCommon
	}

	return &types.GetPresignedUploadUrlResp{
		UploadUrl:  rpcResp.UploadUrl,
		ObjectName: rpcResp.ObjectName,
	}, errmsg.Success
}
