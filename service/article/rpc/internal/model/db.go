package model

import (
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
	"sea-try-go/service/article/rpc/internal/config"
)

type ArticleRepo struct {
	Db *gorm.DB
}

func NewArticleRepo(c config.Config) *ArticleRepo {
	db, err := InitDB(c)
	if err != nil {
		logx.Errorf("init db error:%v", err)
		panic(err)
	}

	logx.Infof("init db success")
	return &ArticleRepo{Db: db}
}

func InitDB(c config.Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable TimeZone=Asia/Shanghai",
		c.Postgres.Host,
		c.Postgres.Port,
		c.Postgres.User,
		c.Postgres.Password,
		c.Postgres.Dbname,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		SkipDefaultTransaction:                   true,
		DisableForeignKeyConstraintWhenMigrating: true,
		NamingStrategy: schema.NamingStrategy{
			SingularTable: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql.DB: %w", err)
	}

	if err := db.AutoMigrate(
		&Article{},
		&ArticleSyncOutboxEvent{},
	); err != nil {
		return nil, fmt.Errorf("failed to auto migrate models: %w", err)
	}

	// AutoMigrate cannot create GIN or partial indexes; manage them explicitly.
	// DROP the old full B-tree indexes (idempotent: no-op if already gone).
	// A plain B-tree on deleted_at is useless for the IS NULL queries GORM generates,
	// and full indexes on author_id/manual_type_tag are superseded by the partial ones below.
	for _, drop := range []string{
		`DROP INDEX IF EXISTS "idx_article_deleted_at"`,
		`DROP INDEX IF EXISTS "idx_article_author_id"`,
		`DROP INDEX IF EXISTS "idx_article_manual_type_tag"`,
	} {
		if err := db.Exec(drop).Error; err != nil {
			return nil, fmt.Errorf("failed to drop old index (%s): %w", drop, err)
		}
	}

	for _, create := range []string{
		`CREATE INDEX IF NOT EXISTS "idx_article_author_id_active" ON "article" ("author_id") WHERE deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS "idx_article_manual_type_tag_active" ON "article" ("manual_type_tag") WHERE deleted_at IS NULL`,
		`CREATE INDEX IF NOT EXISTS "idx_article_secondary_tags_gin" ON "article" USING GIN ("secondary_tags")`,
	} {
		if err := db.Exec(create).Error; err != nil {
			return nil, fmt.Errorf("failed to create index (%s): %w", create, err)
		}
	}

	// Keep the local pool conservative so the outbox poller does not exhaust
	// the shared Postgres instance.
	sqlDB.SetMaxOpenConns(c.Postgres.MaxOpenConns)
	sqlDB.SetMaxIdleConns(c.Postgres.MaxIdleConns)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)

	return db, nil
}
