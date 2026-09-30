package columnar

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"
)

// TestScanSnapshotAcrossCompaction 验证：
//   - 合并开始前已打开的扫描器，在合并完成后仍能完整读完合并前的视图（旧段文件受屏障保护）；
//   - 合并结束后新打开的扫描器看到合并后的数据，行数一致、无重复无丢失。
func TestScanSnapshotAcrossCompaction(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	s, err := Create(dir, baseSchema(), Options{
		FlushThreshold:        3,
		CompactionThreshold:   4,
		MergeMaxRows:          1000,
		DisableAutoCompaction: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	const total = 15
	if err := s.Append(context.Background(), mkRows(total, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// 打开一个扫描器，但只读第一行就持有不关闭（模拟在途长扫描）。
	oldScan, err := s.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan old: %v", err)
	}
	var first Record
	if err := oldScan.Next(&first); err != nil {
		t.Fatalf("old scan first: %v", err)
	}
	t.Logf("old scan opened, first id=%v; now compacting while it is still open", first["id"])

	compacted := make(chan error, 1)
	go func() {
		_, cerr := s.Compact(context.Background())
		compacted <- cerr
	}()

	// 给合并时间运行到物理删除阶段：它必须阻塞在 scanMu.Lock()，等待在途扫描结束。
	// 此时旧段文件必须仍然可读。
	select {
	case cerr := <-compacted:
		t.Fatalf("compaction must wait for in-flight scan, but returned early: %v", cerr)
	case <-time.After(80 * time.Millisecond):
		t.Logf("compaction is blocked waiting for in-flight scan as expected")
	}

	// 旧扫描继续读完，数据完整、与合并前一致。
	oldCount := 1
	for {
		var r Record
		err := oldScan.Next(&r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("old scan next during compaction: %v", err)
		}
		oldCount++
	}
	if err := oldScan.Close(); err != nil {
		t.Fatalf("old scan close: %v", err)
	}
	if oldCount != total {
		t.Fatalf("in-flight scan lost rows: read %d want %d", oldCount, total)
	}
	t.Logf("in-flight scan completed with %d rows intact; closing it unblocks compaction", oldCount)

	if cerr := <-compacted; cerr != nil {
		t.Fatalf("compaction after scan closed: %v", cerr)
	}
	st := s.Stats()
	t.Logf("compaction finished: live=%d rows=%d", st.LiveSegments, st.Rows)
	if st.LiveSegments != 1 || st.Rows != total {
		t.Fatalf("post-compaction state wrong: %+v", st)
	}

	// 新扫描器看到合并后的单一视图。
	newIDs := readIDs(t, s)
	if len(newIDs) != total {
		t.Fatalf("new scan rows: want %d got %d", total, len(newIDs))
	}
	for i, v := range newIDs {
		if v != int64(i+1) {
			t.Fatalf("new scan order wrong at %d: %d", i, v)
		}
	}
	t.Logf("new scan after compaction sees %d rows in merged view, no dup/loss", len(newIDs))
}
