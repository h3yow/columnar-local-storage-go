package columnar

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestBackgroundAutoCompaction 不手动触发合并，持续写入后段数应被后台合并压低，
// 且任意时刻读到的数据都不重复、不丢失。
func TestBackgroundAutoCompaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	// 阈值很小，让后台合并频繁触发。
	s, err := Create(dir, baseSchema(), Options{
		FlushThreshold:      5,
		CompactionThreshold: 3,
		MergeMaxRows:        50,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	const total = 200
	for i := 0; i < total; i++ {
		id := int64(i + 1)
		row := Record{
			"id":     id,
			"name":   fmt.Sprintf("u-%d", id),
			"score":  float64(id) * 0.25,
			"active": id%3 != 0,
		}
		if err := s.Append(context.Background(), []Record{row}); err != nil {
			t.Fatalf("Append %d: %v", id, err)
		}
		if i%10 == 0 {
			// 写入过程中不断验证当前可见状态自洽：id 唯一、数量单调不减。
			ids := readIDs(t, s)
			seen := make(map[int64]struct{}, len(ids))
			for _, x := range ids {
				if _, dup := seen[x]; dup {
					t.Fatalf("duplicate visible id %d at iteration %d", x, i)
				}
				seen[x] = struct{}{}
			}
			if int64(len(ids)) != id {
				t.Fatalf("visible count at iteration %d: want %d got %d", i, id, len(ids))
			}
		}
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// 等待后台合并把段数压到 MergeMaxRows 决定的上界（200/50=4）以下并稳定。
	deadline := time.Now().Add(3 * time.Second)
	var st Stats
	for time.Now().Before(deadline) {
		st = s.Stats()
		if st.LiveSegments <= 4 && st.DeletedSegments == 0 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Logf("final visible state: live=%d deleted=%d rows=%d openPeak=%d",
		st.LiveSegments, st.DeletedSegments, st.Rows, s.openMax.Load())
	if st.Rows != total {
		t.Fatalf("rows: want %d got %d", total, st.Rows)
	}
	if st.LiveSegments > 4 {
		t.Fatalf("background compaction did not reduce segments: live=%d", st.LiveSegments)
	}

	ids := readIDs(t, s)
	if len(ids) != total {
		t.Fatalf("final rows: want %d got %d", total, len(ids))
	}
	seen := make(map[int64]struct{}, len(ids))
	for _, x := range ids {
		if _, dup := seen[x]; dup {
			t.Fatalf("duplicate final id %d", x)
		}
		seen[x] = struct{}{}
	}
	// 顺序也必须保持写入顺序。
	for i, x := range ids {
		if x != int64(i+1) {
			t.Fatalf("order broken at %d: %d", i, x)
		}
	}
	t.Logf("background auto-compaction OK: %d unique ordered rows across %d segments", len(ids), st.LiveSegments)
}
