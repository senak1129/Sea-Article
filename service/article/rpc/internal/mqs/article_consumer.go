package mqs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"sea-try-go/service/article/common/errmsg"
	"sea-try-go/service/article/rpc/internal/model"
	"sea-try-go/service/article/rpc/internal/svc"
	pb "sea-try-go/service/article/rpc/pb"
	"sea-try-go/service/common/logger"
	"sea-try-go/service/common/snowflake"

	"github.com/minio/minio-go/v7"
	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/gorm"
)

type ArticleConsumer struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewArticleConsumer(ctx context.Context, svcCtx *svc.ServiceContext) *ArticleConsumer {
	return &ArticleConsumer{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *ArticleConsumer) Consume(ctx context.Context, key, val string) error {
	return nil
	logger.LogInfo(ctx, "article review consumer received message")

	var msg ArticleReviewMessage
	if err := json.Unmarshal([]byte(val), &msg); err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorServerCommon, fmt.Errorf("unmarshal review message failed: %w", err))
		return nil
	}

	// 策略1: 前置 Redis 防并发锁 (拦截并发与高频重试，保护 Token 和 MinIO)
	lockKey := fmt.Sprintf("lock:article_review:%s", msg.ArticleID)
	// 设置 5 分钟超时，防止服务宕机死锁
	acquired, err := l.svcCtx.RedisClient.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err == nil && !acquired {
		logger.LogInfo(ctx, "article review is already processing or recently processed", logger.WithArticleID(msg.ArticleID))
		return nil // 获取锁失败，说明正在处理，直接丢弃或由上游重试
	}
	if err == nil {
		defer l.svcCtx.RedisClient.Del(ctx, lockKey)
	}

	object, err := l.svcCtx.MinioClient.GetObject(ctx, l.svcCtx.Config.MinIO.BucketName, msg.ContentPath, minio.GetObjectOptions{})
	if err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorMinioDownload, fmt.Errorf("failed to get content from minio: %w", err), logger.WithArticleID(msg.ArticleID))
		_ = l.markArticleRejected(ctx, msg.ArticleID, "minio_download_failed")
		return nil // 遇到文件错误直接放弃并标记失败，避免 Kafka 消息无限重试导致堆积
	}
	defer object.Close()

	contentBytes, err := io.ReadAll(object)
	if err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorMinioDownload, fmt.Errorf("failed to read minio content: %w", err), logger.WithArticleID(msg.ArticleID))
		_ = l.markArticleRejected(ctx, msg.ArticleID, "minio_read_failed")
		return nil // 同理，避免无限堆积
	}
	articleContent := string(contentBytes)

	article, err := l.svcCtx.ArticleRepo.FindOne(ctx, msg.ArticleID)
	if err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorDbSelect, fmt.Errorf("failed to find article %s: %w", msg.ArticleID, err), logger.WithArticleID(msg.ArticleID), logger.WithUserID(msg.AuthorID))
		return err
	}
	if article == nil {
		return nil
	}
	if article.Status != int32(pb.ArticleStatus_REVIEWING) {
		logger.LogInfo(ctx, "article review skipped because status changed", logger.WithArticleID(msg.ArticleID), logger.WithUserID(msg.AuthorID))
		return nil
	}

	EnsureExtInfo(article)
	SetSyncState(article, "security_checking", "pending_review", article.ExtInfo[ExtPendingSyncReason], "", article.UpdatedAt.UnixMilli(), "")
	if err := l.svcCtx.ArticleRepo.UpdateExtInfo(ctx, article.ID, article.ExtInfo); err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, fmt.Errorf("failed to persist publish stage: %w", err), logger.WithArticleID(msg.ArticleID))
		return err
	}

	if err := l.auditArticle(ctx, article, articleContent, msg.AuthorID); err != nil {
		// ← 审核调用失败，但文章状态已经是 "security_checking"
		//如果 auditArticle 失败，文章会一直卡在 security_checking 状态。建议考虑回滚状态或增加超时兜底。
		return err
	}
	if article.Status == int32(pb.ArticleStatus_REJECTED) {
		logger.LogInfo(ctx, "article rejected by security check", logger.WithArticleID(msg.ArticleID), logger.WithUserID(msg.AuthorID))
		return nil
	}

	// 策略2: 核心状态机 (如果审核通过，为了防止重复投递，必须有幂等标记)
	// 由于真正的 "已发布(PUBLISHED)" 状态可能需要推荐系统等下游确认，
	// 这里不直接修改 Status，而是通过修改 ExtInfo 里的 PublishStage 来做幂等标记。
	if article.ExtInfo[ExtPublishStage] == "reco_queued" {
		logger.LogInfo(ctx, "article review skipped: already reco_queued", logger.WithArticleID(msg.ArticleID))
		return nil
	}

	syncReason := strings.TrimSpace(article.ExtInfo[ExtPendingSyncReason])
	if syncReason == "" {
		syncReason = ArticleSyncReasonCreate
	}

	eventID, err := l.newSyncEventID()
	if err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorServerCommon, fmt.Errorf("generate article sync event id failed: %w", err), logger.WithArticleID(msg.ArticleID))
		return err
	}
	versionMs := time.Now().UnixMilli()

	// 策略3: 状态流转与确定性凭证 (使用业务确定的 EventKey 配合 DB 唯一索引兜底去重)

	deterministicEventKey := ArticleSyncEventKey(msg.ArticleID, ArticleSyncOpUpsert, "review_passed")

	event := NewArticleSyncEvent(article, articleContent, ArticleSyncOpUpsert, syncReason, eventID, versionMs)
	outbox := &model.ArticleSyncOutboxEvent{
		EventID:     event.EventID,
		EventKey:    deterministicEventKey,
		EventType:   ArticleOutboxEventTypeSync,
		AggregateID: event.ArticleID,
		Payload:     MustMarshalSyncEvent(event),
		Status:      model.ArticleSyncOutboxStatusPending,
	}

	SetSyncState(article, "reco_queued", "pending", syncReason, eventID, versionMs, "")
	if err := l.svcCtx.ArticleRepo.RunInTx(ctx, func(tx *gorm.DB) error {
		if err := l.svcCtx.ArticleRepo.UpdateExtInfoTx(ctx, tx, article.ID, article.ExtInfo); err != nil {
			return err
		}
		return l.svcCtx.ArticleSyncOutbox.CreateTx(ctx, tx, outbox)
	}); err != nil {
		logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, fmt.Errorf("persist article sync outbox failed: %w", err), logger.WithArticleID(msg.ArticleID), logger.WithUserID(msg.AuthorID))
		return err
	}

	logger.LogInfo(ctx, "article sync event queued", logger.WithArticleID(msg.ArticleID), logger.WithUserID(msg.AuthorID))
	return nil
}

func (l *ArticleConsumer) auditArticle(ctx context.Context, article *model.Article, content string, authorID string) error {
	if l.svcCtx.SecurityRpc == nil {
		return fmt.Errorf("content security client not initialized")
	}

	// 临时注释掉大模型文本广告检测，直接当做无广告处理（为了节省 Token 方便测试）
	/*
		result, err := l.svcCtx.SecurityRpc.SanitizeContent(ctx, &security.SanitizeContentRequest{
			Text: content,
			Options: &security.SanitizeOptions{
				EnableAdDetection:             true,
				EnableHtmlSanitization:        true,
				EnableUnicodeNormalization:    true,
				EnableWhitespaceNormalization: true,
			},
		})
		if err != nil {
			logger.LogBusinessErr(ctx, errmsg.ErrorServerCommon, fmt.Errorf("content security rpc error: %w", err), logger.WithArticleID(article.ID), logger.WithUserID(authorID))
			return err
		}
		if !result.Success {
			return fmt.Errorf("content security service error: %s", result.ErrorMessage)
		}
		if result.IsAd {
			article.Status = int32(pb.ArticleStatus_REJECTED)
			SetSyncState(article, "rejected", "failed", article.ExtInfo[ExtPendingSyncReason], "", time.Now().UnixMilli(), "content_rejected")
			if err := l.svcCtx.ArticleRepo.UpdateStatusAndExtInfo(ctx, article.ID, article.Status, article.ExtInfo); err != nil {
				logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, fmt.Errorf("failed to update article status to rejected: %w", err), logger.WithArticleID(article.ID), logger.WithUserID(authorID))
				return err
			}
			return nil
		}
	*/
	imageURLs := l.extractImageUrls(content)
	if article.CoverImageURL != "" {
		imageURLs = append(imageURLs, article.CoverImageURL)
	}
	for _, imgURL := range imageURLs {
		isAd, _, err := l.auditImage(ctx, imgURL)
		if err != nil {
			logger.LogBusinessErr(ctx, errmsg.ErrorServerCommon, fmt.Errorf("audit image %s failed: %w", imgURL, err), logger.WithArticleID(article.ID), logger.WithUserID(authorID))
			return err
		}
		if isAd {
			article.Status = int32(pb.ArticleStatus_REJECTED)
			SetSyncState(article, "rejected", "failed", article.ExtInfo[ExtPendingSyncReason], "", time.Now().UnixMilli(), "image_rejected")
			if err := l.svcCtx.ArticleRepo.UpdateStatusAndExtInfo(ctx, article.ID, article.Status, article.ExtInfo); err != nil {
				logger.LogBusinessErr(ctx, errmsg.ErrorDbUpdate, fmt.Errorf("failed to update article status to rejected: %w", err), logger.WithArticleID(article.ID), logger.WithUserID(authorID))
				return err
			}
			return nil
		}
	}

	return nil
}

func (l *ArticleConsumer) markArticleRejected(ctx context.Context, articleID, failReason string) error {
	article, err := l.svcCtx.ArticleRepo.FindOne(ctx, articleID)
	if err != nil || article == nil {
		return err
	}
	article.Status = int32(pb.ArticleStatus_REJECTED)
	EnsureExtInfo(article)
	SetSyncState(article, "rejected", "failed", article.ExtInfo[ExtPendingSyncReason], "", time.Now().UnixMilli(), failReason)
	return l.svcCtx.ArticleRepo.UpdateStatusAndExtInfo(ctx, articleID, article.Status, article.ExtInfo)
}

func (l *ArticleConsumer) extractImageUrls(content string) []string {
	re := regexp.MustCompile(`!\[.*?\]\((.*?)\)`)
	matches := re.FindAllStringSubmatch(content, -1)
	urls := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) > 1 {
			urls = append(urls, match[1])
		}
	}
	return urls
}

func (l *ArticleConsumer) auditImage(ctx context.Context, imgURL string) (bool, float32, error) {
	// 临时跳过图片广告检测大模型调用，直接返回无广告（为了节省 Token 方便测试）
	return false, 0, nil
}

func (l *ArticleConsumer) newSyncEventID() (string, error) {
	id, err := snowflake.GetID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", id), nil
}
