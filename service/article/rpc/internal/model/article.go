package model

import (
	"context"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Article GORM Model
type Article struct {
	ID            string      `gorm:"primaryKey;type:varchar(32)"`
	Title         string      `gorm:"type:varchar(255);not null"`
	Brief         string      `gorm:"type:varchar(512)"`
	Content       string      `gorm:"type:text"` // 对应 markdown_content
	CoverImageURL string      `gorm:"type:varchar(255)"`
	ManualTypeTag string      `gorm:"type:varchar(64)"`
	SecondaryTags StringArray `gorm:"type:jsonb"` // 使用 jsonb 存储标签数组
	AuthorID      string      `gorm:"type:varchar(32)"`
	Status        int32       `gorm:"type:smallint;default:0"`
	ViewCount     int32       `gorm:"default:0"`
	LikeCount     int32       `gorm:"default:0"`
	CommentCount  int32       `gorm:"default:0"`
	ShareCount    int32       `gorm:"default:0"`
	ExtInfo       JSONMap     `gorm:"type:jsonb"` // 使用 jsonb 存储扩展信息
	CreatedAt     time.Time   `gorm:"autoCreateTime"`
	UpdatedAt     time.Time   `gorm:"autoUpdateTime"`
	DeletedAt     gorm.DeletedAt
}

// --- Custom Types for Postgres JSONB ---

// StringArray handles []string <-> jsonb
type StringArray []string

func (a StringArray) Value() (driver.Value, error) {
	return json.Marshal(a)
}

func (a *StringArray) Scan(value interface{}) error {
	if value == nil {
		*a = nil
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, a)
}

// JSONMap handles map[string]string <-> jsonb
type JSONMap map[string]string

func (m JSONMap) Value() (driver.Value, error) {
	return json.Marshal(m)
}

func (m *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*m = nil
		return nil
	}
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, m)
}

// --- ArticleRepo Methods ---

var articleSourceAndStateColumns = []string{
	"title", "brief", "content", "cover_image_url",
	"manual_type_tag", "secondary_tags",
	"status", "ext_info", "deleted_at", "updated_at",
}

func (m *ArticleRepo) Insert(ctx context.Context, article *Article) error {
	return m.Db.WithContext(ctx).Create(article).Error
}

func (m *ArticleRepo) InsertTx(ctx context.Context, tx *gorm.DB, article *Article) error {
	return tx.WithContext(ctx).Create(article).Error
}

func (m *ArticleRepo) FindOne(ctx context.Context, id string) (*Article, error) {
	var article Article
	err := m.Db.WithContext(ctx).Where("id = ?", id).Take(&article).Error
	if err != nil {
		return nil, err
	}
	return &article, nil
}

func (m *ArticleRepo) FindOneUnscoped(ctx context.Context, id string) (*Article, error) {
	var article Article
	err := m.Db.WithContext(ctx).Unscoped().Where("id = ?", id).Take(&article).Error
	if err != nil {
		return nil, err
	}
	return &article, nil
}

func (m *ArticleRepo) Update(ctx context.Context, article *Article) error {
	return m.Db.WithContext(ctx).Model(article).Select(articleSourceAndStateColumns).Updates(article).Error
}

func (m *ArticleRepo) UpdateTx(ctx context.Context, tx *gorm.DB, article *Article) error {
	return tx.WithContext(ctx).Model(article).Select(articleSourceAndStateColumns).Updates(article).Error
}

func (m *ArticleRepo) UpdateExtInfo(ctx context.Context, id string, extInfo JSONMap) error {
	return m.Db.WithContext(ctx).Model(&Article{}).Where("id = ?", id).
		Select("ext_info", "updated_at").
		Updates(map[string]any{"ext_info": extInfo}).Error
}

func (m *ArticleRepo) UpdateExtInfoTx(ctx context.Context, tx *gorm.DB, id string, extInfo JSONMap) error {
	return tx.WithContext(ctx).Model(&Article{}).Where("id = ?", id).
		Select("ext_info", "updated_at").
		Updates(map[string]any{"ext_info": extInfo}).Error
}

func (m *ArticleRepo) UpdateStatusAndExtInfo(ctx context.Context, id string, status int32, extInfo JSONMap) error {
	return m.Db.WithContext(ctx).Model(&Article{}).Where("id = ?", id).
		Select("status", "ext_info", "updated_at").
		Updates(map[string]any{"status": status, "ext_info": extInfo}).Error
}

func (m *ArticleRepo) UpdateStatusAndExtInfoTx(ctx context.Context, tx *gorm.DB, id string, status int32, extInfo JSONMap) error {
	return tx.WithContext(ctx).Model(&Article{}).Where("id = ?", id).
		Select("status", "ext_info", "updated_at").
		Updates(map[string]any{"status": status, "ext_info": extInfo}).Error
}

func (m *ArticleRepo) Delete(ctx context.Context, id string) error {
	return m.Db.WithContext(ctx).Delete(&Article{}, "id = ?", id).Error
}

func (m *ArticleRepo) DeleteTx(ctx context.Context, tx *gorm.DB, id string) error {
	return tx.WithContext(ctx).Delete(&Article{}, "id = ?", id).Error
}

func (m *ArticleRepo) RunInTx(ctx context.Context, fn func(tx *gorm.DB) error) (err error) {
	tx := m.Db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return tx.Error
	}

	// fnDone guards against runtime.Goexit() (triggered by t.Fatal in tests):
	// Goexit still runs defers, but recover() returns nil and err stays zero,
	// so without this flag the transaction would be incorrectly committed.
	// Panics propagate naturally when recover() is not called here.
	fnDone := false
	defer func() {
		if !fnDone {
			tx.Rollback()
			return
		}
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit().Error
		}
	}()

	err = fn(tx)
	fnDone = true
	return err
}

func (m *ArticleRepo) AddViewCount(ctx context.Context, id string, delta int64) error {
	if delta <= 0 {
		return nil
	}
	return m.Db.WithContext(ctx).Model(&Article{}).Where("id = ?", id).
		UpdateColumn("view_count", gorm.Expr("view_count + ?", delta)).Error
}

type ListArticlesOption struct {
	Page          int
	PageSize      int
	SortBy        string
	Desc          bool
	ManualTypeTag string
	SecondaryTag  string
	AuthorId      string
}

func (m *ArticleRepo) List(ctx context.Context, opt ListArticlesOption) ([]*Article, int64, error) {
	type row struct {
		Article
		TotalCount int64 `gorm:"column:total_count"`
	}

	db := m.Db.WithContext(ctx).Model(&Article{})

	if opt.ManualTypeTag != "" {
		db = db.Where("manual_type_tag = ?", opt.ManualTypeTag)
	}
	if opt.SecondaryTag != "" {
		tag, err := json.Marshal(opt.SecondaryTag)
		if err != nil {
			return nil, 0, err
		}
		db = db.Where("secondary_tags @> ?", fmt.Sprintf("[%s]", tag))
	}
	if opt.AuthorId != "" {
		db = db.Where("author_id = ?", opt.AuthorId)
	}

	order := "created_at desc"
	if opt.SortBy != "" {
		switch opt.SortBy {
		case "create_time":
			order = "created_at"
		case "view_count":
			order = "view_count"
		case "like_count":
			order = "like_count"
		}
		if opt.Desc {
			order += " desc"
		} else {
			order += " asc"
		}
	}

	offset := (opt.Page - 1) * opt.PageSize
	if offset < 0 {
		offset = 0
	}

	// COUNT(*) OVER() returns the total matching rows in the same query,
	// eliminating the separate Count call and the data-vs-total inconsistency
	// that existed when two independent queries were issued.
	var rows []row
	if err := db.Select("*, COUNT(*) OVER() AS total_count").
		Order(order).Offset(offset).Limit(opt.PageSize).
		Scan(&rows).Error; err != nil {
		return nil, 0, err
	}

	if len(rows) == 0 {
		return nil, 0, nil
	}

	articles := make([]*Article, len(rows))
	for i := range rows {
		a := rows[i].Article
		articles[i] = &a
	}
	return articles, rows[0].TotalCount, nil
}

// ListByCursorOption drives cursor-based (keyset) pagination.
// Cursor encodes the last item's (CreatedAt, ID); empty means first page.
type ListByCursorOption struct {
	Cursor        string
	PageSize      int
	ManualTypeTag string
	SecondaryTag  string
	AuthorId      string
}

// ListByCursor returns at most PageSize articles and a NextCursor for the
// following page. NextCursor is empty when there are no more results.
// Unlike OFFSET pagination this is O(log n) regardless of page depth.
func (m *ArticleRepo) ListByCursor(ctx context.Context, opt ListByCursorOption) ([]*Article, string, error) {
	db := m.Db.WithContext(ctx).Model(&Article{})

	if opt.ManualTypeTag != "" {
		db = db.Where("manual_type_tag = ?", opt.ManualTypeTag)
	}
	if opt.SecondaryTag != "" {
		tag, err := json.Marshal(opt.SecondaryTag)
		if err != nil {
			return nil, "", err
		}
		db = db.Where("secondary_tags @> ?", fmt.Sprintf("[%s]", tag))
	}
	if opt.AuthorId != "" {
		db = db.Where("author_id = ?", opt.AuthorId)
	}

	if opt.Cursor != "" {
		cur, err := decodeCursor(opt.Cursor)
		if err != nil {
			return nil, "", fmt.Errorf("invalid cursor: %w", err)
		}
		// Composite condition handles ties on created_at correctly.
		db = db.Where("created_at < ? OR (created_at = ? AND id < ?)", cur.CreatedAt, cur.CreatedAt, cur.ID)
	}

	// Fetch one extra row to detect whether a next page exists.
	var articles []*Article
	if err := db.Order("created_at DESC, id DESC").Limit(opt.PageSize + 1).Find(&articles).Error; err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(articles) > opt.PageSize {
		articles = articles[:opt.PageSize]
		last := articles[len(articles)-1]
		nextCursor = encodeCursor(cursorPayload{CreatedAt: last.CreatedAt, ID: last.ID})
	}

	return articles, nextCursor, nil
}

type cursorPayload struct {
	CreatedAt time.Time `json:"c"`
	ID        string    `json:"i"`
}

func encodeCursor(c cursorPayload) string {
	b, _ := json.Marshal(c)
	return base64.StdEncoding.EncodeToString(b)
}

func decodeCursor(s string) (cursorPayload, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return cursorPayload{}, err
	}
	var c cursorPayload
	return c, json.Unmarshal(b, &c)
}
