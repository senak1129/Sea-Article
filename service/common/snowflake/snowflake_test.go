package snowflake

import (
	"fmt"
	"sync"
	"testing"
)

// SNOWFLAKE_NODE_ID 显式指定时，generateNodeId 应直接采用该值。
func TestGenerateNodeIdFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  string
		want uint16
	}{
		{"指定非零", "5", 5},
		{"指定零", "0", 0},
		{"指定上界", "1023", 1023},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("SNOWFLAKE_NODE_ID", tt.env)

			got, err := generateNodeId()
			if err != nil {
				t.Fatalf("generateNodeId() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("generateNodeId() = %d, want %d", got, tt.want)
			}
		})
	}
}

// 非法或负数环境变量应回退到 hash 方案而非报错。
func TestGenerateNodeIdFallsBackWhenEnvInvalid(t *testing.T) {
	for _, invalid := range []string{"abc", "-1", "1.5"} {
		t.Run(invalid, func(t *testing.T) {
			t.Setenv("SNOWFLAKE_NODE_ID", invalid)

			if _, err := generateNodeId(); err != nil {
				t.Errorf("非法环境变量 %q 应回退，却返回错误: %v", invalid, err)
			}
		})
	}
}

// 未指定环境变量时，同一进程内 nodeId 应保持稳定（服务名 + PID 的 hash）。
func TestGenerateNodeIdDeterministic(t *testing.T) {
	t.Setenv("SNOWFLAKE_NODE_ID", "")
	t.Setenv("SERVICE_NAME", "unit-test-service")

	first, err := generateNodeId()
	if err != nil {
		t.Fatalf("generateNodeId() error = %v", err)
	}
	second, err := generateNodeId()
	if err != nil {
		t.Fatalf("generateNodeId() error = %v", err)
	}
	if first != second {
		t.Errorf("同一进程内 generateNodeId 应稳定，得到 %d 与 %d", first, second)
	}
}

func TestGetIDPositiveAndUnique(t *testing.T) {
	const n = 2000

	seen := make(map[int64]struct{}, n)
	for i := 0; i < n; i++ {
		id, err := GetID()
		if err != nil {
			t.Fatalf("GetID() error = %v", err)
		}
		if id <= 0 {
			t.Fatalf("GetID() = %d, want 正数", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("GetID() 产生重复 ID: %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestGetIDConcurrentUnique(t *testing.T) {
	const workers = 8
	const perWorker = 250

	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[int64]struct{}, workers*perWorker)
	errs := make(chan error, workers)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id, err := GetID()
				if err != nil {
					errs <- err
					return
				}

				mu.Lock()
				if _, dup := seen[id]; dup {
					errs <- fmt.Errorf("并发下产生重复 ID: %d", id)
				} else {
					seen[id] = struct{}{}
				}
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
	if len(seen) != workers*perWorker {
		t.Errorf("生成 %d 个唯一 ID，want %d", len(seen), workers*perWorker)
	}
}
