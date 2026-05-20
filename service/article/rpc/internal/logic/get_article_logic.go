package logic

import (
	"context"
	"io"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/zeromicro/go-zero/core/logx"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"sea-try-go/service/article/rpc/internal/svc"
	"sea-try-go/service/article/rpc/metrics"
	__ "sea-try-go/service/article/rpc/pb"
)

type GetArticleLogic struct {
	ctx    context.Context
	svcCtx *svc.ServiceContext
	logx.Logger
}

func NewGetArticleLogic(ctx context.Context, svcCtx *svc.ServiceContext) *GetArticleLogic {
	return &GetArticleLogic{
		ctx:    ctx,
		svcCtx: svcCtx,
		Logger: logx.WithContext(ctx),
	}
}

func (l *GetArticleLogic) GetArticle(in *__.GetArticleRequest) (*__.GetArticleResponse, error) {
	tracer := otel.Tracer("article-rpc")
	ctx, span := tracer.Start(l.ctx, "GetArticle", trace.WithAttributes(
		attribute.String("article_id", in.ArticleId),
	))
	defer span.End()

	// 1. 尝试从缓存获取元数据，或者加载（方案A：Redis 仅存元数据和路径）
	var articlePb *__.Article
	var err error
	if l.svcCtx.ArticleCache != nil {
		logx.Infof("ArticleCache is NOT nil, trying to GetOrLoadDetail for id: %s", in.ArticleId)
		articlePb, err = l.svcCtx.ArticleCache.GetOrLoadDetail(ctx, in.ArticleId, 5*time.Minute, func() (*__.Article, error) {
			logx.Infof("Cache missed for article id: %s, loading from database...", in.ArticleId)
			// 缓存未命中，从数据库获取元数据
			article, err := l.svcCtx.ArticleRepo.FindOne(ctx, in.ArticleId)
			if err != nil {
				logx.Errorf("Load from database failed for article id: %s, err: %v", in.ArticleId, err)
				if err == gorm.ErrRecordNotFound {
					return nil, nil
				}
				return nil, err
			}

			logx.Infof("Loaded article from database successfully for id: %s", in.ArticleId)
			// 注意：这里不再从 MinIO 读取全文，只存路径到 MarkdownContent 字段中
			return &__.Article{
				Id:              article.ID,
				Title:           article.Title,
				Brief:           article.Brief,
				MarkdownContent: article.Content, // 存入的是 MinIO 相对路径
				CoverImageUrl:   article.CoverImageURL,
				ManualTypeTag:   article.ManualTypeTag,
				SecondaryTags:   article.SecondaryTags,
				AuthorId:        article.AuthorID,
				CreateTime:      article.CreatedAt.UnixMilli(),
				UpdateTime:      article.UpdatedAt.UnixMilli(),
				Status:          __.ArticleStatus(article.Status),
				ViewCount:       article.ViewCount,
				LikeCount:       article.LikeCount,
				CommentCount:    article.CommentCount,
				ShareCount:      article.ShareCount,
				ExtInfo:         cloneStringMap(map[string]string(article.ExtInfo)),
			}, nil
		})
		if err != nil {
			logx.Errorf("get/load article cache failed: %v", err)
		} else {
			logx.Infof("GetOrLoadDetail succeeded for id: %s", in.ArticleId)
		}
	} else {
		logx.Errorf("ArticleCache is NIL! Fallback to database directly. Redis initialization might have failed.")
	}

	if articlePb == nil {
		logx.Infof("articlePb is nil after cache logic, querying database directly for id: %s", in.ArticleId)
		// 兜底逻辑：直接查库
		article, err := l.svcCtx.ArticleRepo.FindOne(ctx, in.ArticleId)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil, nil
			}
			return nil, err
		}
		articlePb = &__.Article{
			Id:              article.ID,
			Title:           article.Title,
			Brief:           article.Brief,
			MarkdownContent: article.Content,
			CoverImageUrl:   article.CoverImageURL,
			ManualTypeTag:   article.ManualTypeTag,
			SecondaryTags:   article.SecondaryTags,
			AuthorId:        article.AuthorID,
			CreateTime:      article.CreatedAt.UnixMilli(),
			UpdateTime:      article.UpdatedAt.UnixMilli(),
			Status:          __.ArticleStatus(article.Status),
			ViewCount:       article.ViewCount,
			LikeCount:       article.LikeCount,
			CommentCount:    article.CommentCount,
			ShareCount:      article.ShareCount,
			ExtInfo:         cloneStringMap(map[string]string(article.ExtInfo)),
		}
	}

	// 2. 无论缓存命中与否，文章正文（全文）始终实时从 MinIO 获取（方案A的核心）
	if articlePb.MarkdownContent != "" {
		path := articlePb.MarkdownContent
		timer := prometheus.NewTimer(metrics.MinioRequestDuration.WithLabelValues("get"))
		object, err := l.svcCtx.MinioClient.GetObject(ctx, l.svcCtx.Config.MinIO.BucketName, path, minio.GetObjectOptions{})
		if err != nil {
			timer.ObserveDuration()
			logx.Errorf("minio get object failed: %v, path: %s", err, path)
			// 如果读取失败，可能路径是过期的或者 MinIO 故障，此时 MarkdownContent 仍是路径
		} else {
			defer object.Close()
			contentBytes, err := io.ReadAll(object)
			timer.ObserveDuration()
			if err != nil {
				logx.Errorf("read minio object failed: %v", err)
			} else {
				// 只有成功读取后，才将路径替换为真正的正文内容
				articlePb.MarkdownContent = string(contentBytes)
			}
		}
	}

	// 3. 处理阅读量（合并 Redis 中的实时增量）
	if in.IncrView && l.svcCtx.ViewCounter != nil {
		delta, err := l.svcCtx.ViewCounter.Incr(ctx, in.ArticleId)
		if err == nil {
			articlePb.ViewCount += int32(delta)
		}
	} else if l.svcCtx.ViewCounter != nil {
		delta, err := l.svcCtx.ViewCounter.GetDelta(ctx, in.ArticleId)
		if err == nil {
			articlePb.ViewCount += int32(delta)
		}
	}

	return &__.GetArticleResponse{Article: articlePb}, nil
}

func cloneStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return nil
	}

	cloned := make(map[string]string, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}
