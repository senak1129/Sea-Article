package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sea-try-go/service/article/common/errmsg"
	"sea-try-go/service/article/rpc/internal/model"
	"sea-try-go/service/article/rpc/internal/mqs"
	"sea-try-go/service/article/rpc/internal/svc"
	"sea-try-go/service/article/rpc/metrics"
	__ "sea-try-go/service/article/rpc/pb"
	"sea-try-go/service/common/logger"
	"sea-try-go/service/common/snowflake"

	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

type UpdateArticleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewUpdateArticleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *UpdateArticleLogic {
	return &UpdateArticleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *UpdateArticleLogic) UpdateArticle(in *__.UpdateArticleRequest) (*__.UpdateArticleResponse, error) {
	tracer := otel.Tracer("article-rpc")
	ctx, span := tracer.Start(l.ctx, "UpdateArticle", trace.WithAttributes(
		attribute.String("article_id", in.ArticleId),
	))
	defer span.End()

	// 提前检查客户端是否已断开连接
	if err := l.ctx.Err(); err != nil {
		logger.LogInfo(l.ctx, "client disconnected before updating", logger.WithArticleID(in.ArticleId))
		return nil, status.Error(codes.Canceled, err.Error())
	}

	article, err := l.svcCtx.ArticleRepo.FindOne(ctx, in.ArticleId)
	if err != nil {
		span.RecordError(err)
		logger.LogBusinessErr(ctx, errmsg.ErrorDbSelect, err, logger.WithArticleID(in.ArticleId))
		return nil, err
	}
	if article == nil {
		err = status.Error(codes.NotFound, "article not found")
		span.RecordError(err)
		return nil, err
	}

	prevStatus := __.ArticleStatus(article.Status)
	sourceChanged, err := l.applySourceUpdates(ctx, article, in)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	// 更新数据库前再次检查 context
	if err := l.ctx.Err(); err != nil {
		logger.LogInfo(l.ctx, "client disconnected before db update", logger.WithArticleID(in.ArticleId))
		return nil, status.Error(codes.Canceled, err.Error())
	}

	requestedStatus := prevStatus
	if in.Status != nil && *in.Status != __.ArticleStatus_ARTICLE_STATUS_UNSPECIFIED {
		requestedStatus = *in.Status
	}

	now := time.Now()
	prevPublished := prevStatus == __.ArticleStatus_PUBLISHED

	switch {
	case prevPublished && in.Status != nil && *in.Status != __.ArticleStatus_ARTICLE_STATUS_UNSPECIFIED && requestedStatus != __.ArticleStatus_PUBLISHED:
		if err := l.updatePublishedToSourceOnly(ctx, article, requestedStatus, now); err != nil {
			span.RecordError(err)
			return nil, err
		}
	case prevPublished && sourceChanged:
		mqs.SetSyncState(article, "queued", "pending_review", mqs.ArticleSyncReasonUpdate, "", now.UnixMilli(), "")
		article.Status = int32(__.ArticleStatus_REVIEWING)
		if err := l.updateToReviewingTx(ctx, article); err != nil {
			span.RecordError(err)
			return nil, err
		}
	default:
		if in.Status != nil && *in.Status != __.ArticleStatus_ARTICLE_STATUS_UNSPECIFIED && requestedStatus != __.ArticleStatus_PUBLISHED {
			article.Status = int32(requestedStatus)
		}
		needsReview := __.ArticleStatus(article.Status) == __.ArticleStatus_REVIEWING && (sourceChanged || prevStatus != __.ArticleStatus_REVIEWING)
		if needsReview {
			if err := l.updateToReviewingTx(ctx, article); err != nil {
				span.RecordError(err)
				return nil, err
			}
		} else {
			if err := l.svcCtx.ArticleRepo.Update(ctx, article); err != nil {
				span.RecordError(err)
				logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, err, logger.WithArticleID(in.ArticleId))
				return nil, err
			}
		}
	}

	metrics.ArticleTotal.WithLabelValues("update").Inc()

	// 失效缓存
	if l.svcCtx.ArticleCache != nil {
		l.svcCtx.ArticleCache.DelDetail(ctx, in.ArticleId)
	}

	return &__.UpdateArticleResponse{Success: true}, nil
}

func (l *UpdateArticleLogic) applySourceUpdates(ctx context.Context, article *model.Article, in *__.UpdateArticleRequest) (bool, error) {
	sourceChanged := false

	if in.Title != nil {
		article.Title = *in.Title
		sourceChanged = true
	}
	if in.Brief != nil {
		article.Brief = *in.Brief
		sourceChanged = true
	}
	if in.ContentPath != nil {
		article.Content = *in.ContentPath
		sourceChanged = true
	}
	if in.CoverImageUrl != nil {
		article.CoverImageURL = *in.CoverImageUrl
		sourceChanged = true
	}
	if in.ManualTypeTag != nil {
		article.ManualTypeTag = *in.ManualTypeTag
		sourceChanged = true
	}
	if len(in.SecondaryTags) > 0 {
		article.SecondaryTags = append(model.StringArray(nil), in.SecondaryTags...)
		sourceChanged = true
	}

	return sourceChanged, nil
}

func (l *UpdateArticleLogic) updatePublishedToSourceOnly(ctx context.Context, article *model.Article, requestedStatus __.ArticleStatus, now time.Time) error {
	eventID, err := l.newEventID()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	versionMs := now.UnixMilli()
	event := mqs.NewArticleSyncEvent(article, "", mqs.ArticleSyncOpDelete, mqs.ArticleSyncReasonStatusChange, eventID, versionMs)
	outbox := &model.ArticleSyncOutboxEvent{
		EventID:     event.EventID,
		EventKey:    mqs.ArticleSyncEventKey(event.ArticleID, event.Op, event.EventID),
		EventType:   "article_sync",
		AggregateID: event.ArticleID,
		Payload:     mqs.MustMarshalSyncEvent(event),
		Status:      model.ArticleSyncOutboxStatusPending,
	}

	article.Status = int32(requestedStatus)
	mqs.SetSyncState(article, "source_only", "pending", mqs.ArticleSyncReasonStatusChange, eventID, versionMs, "")

	if err := l.svcCtx.ArticleRepo.RunInTx(ctx, func(tx *gorm.DB) error {
		if err := l.svcCtx.ArticleRepo.UpdateTx(ctx, tx, article); err != nil {
			return err
		}
		return l.svcCtx.ArticleSyncOutbox.CreateTx(ctx, tx, outbox)
	}); err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, fmt.Errorf("update published article status failed: %w", err), logger.WithArticleID(article.ID))
		return err
	}

	return nil
}

func (l *UpdateArticleLogic) buildReviewOutbox(article *model.Article) (*model.ArticleSyncOutboxEvent, error) {
	id, err := snowflake.GetID()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	msgBytes, _ := json.Marshal(mqs.ArticleReviewMessage{
		ArticleID:   article.ID,
		AuthorID:    article.AuthorID,
		ContentPath: article.Content,
	})
	return &model.ArticleSyncOutboxEvent{
		EventID:     fmt.Sprintf("%d", id),
		EventKey:    fmt.Sprintf("review_%s", article.ID),
		EventType:   mqs.ArticleOutboxEventTypeReview,
		AggregateID: article.ID,
		Payload:     string(msgBytes),
		Status:      model.ArticleSyncOutboxStatusPending,
	}, nil
}

func (l *UpdateArticleLogic) updateToReviewingTx(ctx context.Context, article *model.Article) error {
	outbox, err := l.buildReviewOutbox(article)
	if err != nil {
		return err
	}
	if err := l.svcCtx.ArticleRepo.RunInTx(ctx, func(tx *gorm.DB) error {
		if err := l.svcCtx.ArticleRepo.UpdateTx(ctx, tx, article); err != nil {
			return err
		}
		return l.svcCtx.ArticleSyncOutbox.CreateTx(ctx, tx, outbox)
	}); err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, fmt.Errorf("update article to reviewing failed: %w", err), logger.WithArticleID(article.ID))
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

func (l *UpdateArticleLogic) newEventID() (string, error) {
	id, err := snowflake.GetID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", id), nil
}
