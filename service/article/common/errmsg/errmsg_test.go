package errmsg

import "testing"

func TestGetErrMsg(t *testing.T) {
	tests := []struct {
		name string
		code int
		want string
	}{
		{"成功", Success, "OK"},
		{"通用失败", Error, "FAIL"},
		{"文章已存在", ErrorArticleExist, "文章已存在"},
		{"未登录", ErrorUnauthorized, "请先登录"},
		{"MinIO 上传失败", ErrorMinioUpload, "文件上传失败"},
		{"服务繁忙", CodeServerBusy, "服务繁忙"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetErrMsg(tt.code); got != tt.want {
				t.Errorf("GetErrMsg(%d) = %q, want %q", tt.code, got, tt.want)
			}
		})
	}
}

// 未知 code 应回退到 Error 的文案，而不是返回空串。
func TestGetErrMsgUnknownCodeFallsBackToError(t *testing.T) {
	fallback := codeMsg[Error]

	for _, code := range []int{-1, 0, 999999} {
		if got := GetErrMsg(code); got != fallback {
			t.Errorf("GetErrMsg(%d) = %q, want %q", code, got, fallback)
		}
	}
}

// 所有已定义的业务 code 都必须在 codeMsg 中有非空文案，避免线上返回空消息。
func TestAllCodesHaveNonEmptyMessage(t *testing.T) {
	codes := []int{
		Success, Error, CodeServerBusy, ErrorArticleExist, ErrorArticleNone,
		ErrorArticlePublishing, ErrorArticlePublishFailed, ErrorUnauthorized,
		ErrorArticleForbidden, ErrorServerCommon, ErrorDbUpdate, ErrorDbSelect,
		ErrorMinioUpload, ErrorMinioDelete, ErrorMinioDownload,
	}

	for _, code := range codes {
		msg, ok := codeMsg[code]
		if !ok || msg == "" {
			t.Errorf("code %d 缺少错误文案", code)
		}
	}
}
