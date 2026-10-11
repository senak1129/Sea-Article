package article

import (
	"context"
	"encoding/json"
	"testing"

	"sea-try-go/service/article/common/errmsg"
)

// extractCurrentUserID 需要兼容 JWT 中间件可能塞入的多种类型（json.Number / string / 数值）。
func TestExtractCurrentUserID(t *testing.T) {
	const key = "userId"

	tests := []struct {
		name     string
		withVal  bool
		val      interface{}
		wantID   string
		wantCode int
	}{
		{"缺少 userId", false, nil, "", errmsg.ErrorUnauthorized},
		{"显式 nil", true, nil, "", errmsg.ErrorUnauthorized},
		{"json.Number", true, json.Number("1234567890123"), "1234567890123", errmsg.Success},
		{"json.Number 空串", true, json.Number(""), "", errmsg.ErrorUnauthorized},
		{"字符串含空白", true, "  42  ", "42", errmsg.Success},
		{"纯空白字符串", true, "   ", "", errmsg.ErrorUnauthorized},
		{"int64", true, int64(7), "7", errmsg.Success},
		{"int", true, 8, "8", errmsg.Success},
		{"float64", true, float64(9), "9", errmsg.Success},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.withVal {
				ctx = context.WithValue(ctx, key, tt.val)
			}

			gotID, gotCode := extractCurrentUserID(ctx)
			if gotID != tt.wantID || gotCode != tt.wantCode {
				t.Errorf("extractCurrentUserID() = (%q, %d), want (%q, %d)",
					gotID, gotCode, tt.wantID, tt.wantCode)
			}
		})
	}
}

func TestFallbackAuthorName(t *testing.T) {
	tests := []struct {
		authorID string
		want     string
	}{
		{"", "未知作者"},
		{"   ", "未知作者"},
		{"123", "用户 123"},
		{"9876543210", "用户 9876543210"},
	}

	for _, tt := range tests {
		if got := fallbackAuthorName(tt.authorID); got != tt.want {
			t.Errorf("fallbackAuthorName(%q) = %q, want %q", tt.authorID, got, tt.want)
		}
	}
}

func TestCloneStringMap(t *testing.T) {
	if got := cloneStringMap(nil); got != nil {
		t.Errorf("cloneStringMap(nil) = %v, want nil", got)
	}
	if got := cloneStringMap(map[string]string{}); got != nil {
		t.Errorf("cloneStringMap(空 map) = %v, want nil", got)
	}

	src := map[string]string{"a": "1", "b": "2"}
	clone := cloneStringMap(src)
	if len(clone) != len(src) {
		t.Fatalf("clone 长度 = %d, want %d", len(clone), len(src))
	}
	for k, v := range src {
		if clone[k] != v {
			t.Errorf("clone[%q] = %q, want %q", k, clone[k], v)
		}
	}

	// 副本必须独立于源 map，避免调用方修改互相影响。
	clone["a"] = "changed"
	if src["a"] != "1" {
		t.Error("修改 clone 不应影响源 map")
	}
}

// extractArticleIDFromStatusMessage 从 gRPC status message 的最后一个冒号后取文章 ID。
func TestExtractArticleIDFromStatusMessage(t *testing.T) {
	tests := []struct {
		name    string
		message string
		want    string
	}{
		{"空消息", "", ""},
		{"无冒号", "no colon here", ""},
		{"标准格式", "article publish timeout: 123456", "123456"},
		{"多个冒号取最后一个", "a:b: 789", "789"},
		{"冒号结尾", "trailing colon:", ""},
		{"冒号开头", ":999", "999"},
		{"尾部带空白", "failed: 456   ", "456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractArticleIDFromStatusMessage(tt.message); got != tt.want {
				t.Errorf("extractArticleIDFromStatusMessage(%q) = %q, want %q",
					tt.message, got, tt.want)
			}
		})
	}
}
