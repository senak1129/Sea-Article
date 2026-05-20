package logic

import (
	"context"
	"fmt"
	"time"
	"path/filepath"

	"sea-try-go/service/article/rpc/internal/svc"
	__ "sea-try-go/service/article/rpc/pb"
	"sea-try-go/service/common/snowflake"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GetPresignedUploadUrlLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetPresignedUploadUrlLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetPresignedUploadUrlLogic {
	return &GetPresignedUploadUrlLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetPresignedUploadUrlLogic) GetPresignedUploadUrl(in *__.GetPresignedUploadUrlRequest) (*__.GetPresignedUploadUrlResponse, error) {
	// 生成一个雪花ID作为文件名的前缀，防止冲突
	idInt, err := snowflake.GetID()
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to generate unique id")
	}

	ext := filepath.Ext(in.FileName)
	if ext == "" {
		if in.ContentType == "text/markdown" {
			ext = ".md"
		} else {
			ext = ".txt"
		}
	}

	objectName := fmt.Sprintf("%s%d%s", l.svcCtx.Config.MinIO.ArticlePath, idInt, ext)
	expiry := time.Minute * 10

	presignedURL, err := l.svcCtx.MinioClient.PresignedPutObject(
		l.ctx,
		l.svcCtx.Config.MinIO.BucketName,
		objectName,
		expiry,
	)
	if err != nil {
		l.Errorf("failed to generate presigned url: %v", err)
		return nil, status.Error(codes.Internal, "failed to generate presigned url")
	}

	return &__.GetPresignedUploadUrlResponse{
		UploadUrl:  presignedURL.String(),
		ObjectName: objectName,
	}, nil
}
