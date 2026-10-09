package logic

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"

	"gorm.io/gorm"

	"github.com/zeromicro/go-zero/core/logx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sea-try-go/service/article/common/errmsg"
	"sea-try-go/service/article/rpc/internal/model"
	"sea-try-go/service/article/rpc/internal/mqs"
	"sea-try-go/service/article/rpc/internal/svc"
	"sea-try-go/service/article/rpc/metrics"
	__ "sea-try-go/service/article/rpc/pb"
	"sea-try-go/service/common/logger"
	"sea-try-go/service/common/snowflake"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type CreateArticleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewCreateArticleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *CreateArticleLogic {
	return &CreateArticleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *CreateArticleLogic) CreateArticle(in *__.CreateArticleRequest) (*__.CreateArticleResponse, error) {
	tracer := otel.Tracer("article-rpc")
	//jaeger
	ctx, span := tracer.Start(l.ctx, "CreateArticle", trace.WithAttributes(
		attribute.String("author_id", in.AuthorId),
		attribute.String("title", in.Title),
	))
	defer span.End()

	idInt, err := snowflake.GetID()
	if err != nil {
		span.RecordError(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	articleID := fmt.Sprintf("%d", idInt)
	span.SetAttributes(attribute.String("article_id", articleID))

	// 提前检查客户端是否已经主动断开连接
	if err := l.ctx.Err(); err != nil {
		logger.LogInfo(l.ctx, "client disconnected before processing", logger.WithArticleID(articleID))
		return nil, status.Error(codes.Canceled, err.Error())
	}

	brief := ""
	if in.Brief != nil {
		brief = *in.Brief
	}
	coverImageURL := ""
	if in.CoverImageUrl != nil {
		coverImageURL = *in.CoverImageUrl
	}

	newArticle := &model.Article{
		ID:            articleID,
		Title:         in.Title,
		Brief:         brief,
		Content:       in.ContentPath,
		CoverImageURL: coverImageURL,
		ManualTypeTag: in.ManualTypeTag,
		SecondaryTags: model.StringArray(in.SecondaryTags),
		AuthorID:      in.AuthorId,
		Status:        int32(__.ArticleStatus_REVIEWING),
		ExtInfo: model.JSONMap{
			mqs.ExtPublishStage:      "queued",
			mqs.ExtRecoSyncState:     "pending_review",
			mqs.ExtLastSyncError:     "",
			mqs.ExtLastSyncEventID:   "",
			mqs.ExtLastSyncVersion:   "0",
			mqs.ExtPendingSyncReason: mqs.ArticleSyncReasonCreate,
			mqs.ExtLastSyncReason:    mqs.ArticleSyncReasonCreate,
		},
	}

	msg := mqs.ArticleReviewMessage{
		ArticleID:   articleID,
		AuthorID:    in.AuthorId,
		ContentPath: in.ContentPath,
	}
	msgBytes, _ := sonic.Marshal(msg)

	eventIdInt, err := snowflake.GetID()
	if err != nil {
		span.RecordError(err)
		return nil, status.Error(codes.Internal, fmt.Sprintf("generate event id failed: %v", err))
	}

	outboxEvent := &model.ArticleSyncOutboxEvent{
		EventID:     fmt.Sprintf("%d", eventIdInt),
		EventKey:    fmt.Sprintf("review_%s", articleID),
		EventType:   mqs.ArticleOutboxEventTypeReview,
		AggregateID: articleID,
		Payload:     string(msgBytes),
		Status:      model.ArticleSyncOutboxStatusPending,
	}

	// 再次检查 context，避免在执行耗时的数据库事务前客户端已经断开
	if err := l.ctx.Err(); err != nil {
		logger.LogInfo(l.ctx, "client disconnected before db transaction", logger.WithArticleID(articleID))
		return nil, status.Error(codes.Canceled, err.Error())
	}

	_, dbSpan := tracer.Start(ctx, "DB.InsertArticle",
		trace.WithAttributes(
			attribute.String("db.operation", "RunInTx"),
			attribute.String("db.table", "article and Outbox"),
		),
	)

	if err := l.svcCtx.ArticleRepo.RunInTx(ctx, func(tx *gorm.DB) error {
		if err := l.svcCtx.ArticleRepo.InsertTx(ctx, tx, newArticle); err != nil {
			return err
		}
		if err := l.svcCtx.ArticleSyncOutbox.CreateTx(ctx, tx, outboxEvent); err != nil {
			return err
		}
		return nil
	}); err != nil {
		span.RecordError(err)
		logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, err, logger.WithArticleID(articleID), logger.WithUserID(in.AuthorId))
		return nil, status.Error(codes.Internal, err.Error())
	}
	dbSpan.End()
	
	// 最后检查 context，防止后续指标增加和空跑响应
	if err := l.ctx.Err(); err != nil {
		logger.LogInfo(l.ctx, "client disconnected after db transaction", logger.WithArticleID(articleID))
		return nil, status.Error(codes.Canceled, err.Error())
	}

	metrics.ArticleTotal.WithLabelValues("create").Inc()
	metrics.ArticleStatusTotal.WithLabelValues("reviewing").Inc()

	return &__.CreateArticleResponse{ArticleId: articleID}, nil
}
