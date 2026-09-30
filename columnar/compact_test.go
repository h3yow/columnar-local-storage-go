package columnar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// countSegFiles 统计目录中段文件（.col）与临时文件（.tmp）数量。
func countSegFiles(t *testing.T, dir string) (col, tmp int) {
	t.Helper()
	entries, err := readDirNames(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	for _, n := range entries {
		switch {
		case len(n) > 4 && n[len(n)-4:] == ".col":
			col++
		case len(n) > 4 && n[len(n)-4:] == ".tmp":
			tmp++
		}
	}
	return col, tmp
}

// readIDs 扫描并收集 id 列，用于检查重复/丢失/顺序。
func readIDs(t *testing.T, s *Store) []int64 {
	t.Helper()
	sc, err := s.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer sc.Close()
	var ids []int64
	for {
		var r Record
		err := sc.Next(&r)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		ids = append(ids, r["id"].(int64))
	}
	return ids
}

// TestCompactionReducesSegmentsAndPreservesData 验证合并后数据不重不丢、顺序不变、段数下降。
func TestCompactionReducesSegmentsAndPreservesData(t *testing.T) {
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

	total := 30
	if err := s.Append(context.Background(), mkRows(total, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("flush Close: %v", err)
	}
	s, err = Open(dir, Options{FlushThreshold: 3, CompactionThreshold: 4, DisableAutoCompaction: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	before := s.Stats()
	t.Logf("before compaction: live=%d rows=%d", before.LiveSegments, before.Rows)
	if before.LiveSegments < 4 {
		t.Fatalf("expected >=4 segments before compaction, got %d", before.LiveSegments)
	}

	did, err := s.Compact(context.Background())
	if err != nil || !did {
		t.Fatalf("Compact: did=%v err=%v", did, err)
	}
	after := s.Stats()
	t.Logf("after compaction: live=%d deleted=%d rows=%d", after.LiveSegments, after.DeletedSegments, after.Rows)
	if after.LiveSegments != 1 {
		t.Fatalf("expected 1 live segment after compaction, got %d", after.LiveSegments)
	}
	if after.Rows != uint64(total) {
		t.Fatalf("row count after compaction: want %d got %d", total, after.Rows)
	}

	ids := readIDs(t, s)
	if len(ids) != total {
		t.Fatalf("ids count: want %d got %d", total, len(ids))
	}
	for i, v := range ids {
		if v != int64(i+1) {
			t.Fatalf("id at %d: want %d got %d (order/duplication/loss)", i, i+1, v)
		}
	}
	col, tmp := countSegFiles(t, dir)
	t.Logf("files on disk: col=%d tmp=%d (old segments physically removed)", col, tmp)
	if col != 1 || tmp != 0 {
		t.Fatalf("file layout after compaction: col=%d tmp=%d, want 1/0", col, tmp)
	}

	// 重开后数据依旧完整（MANIFEST 中的 deleted 条目应已清理）。
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir, Options{DisableAutoCompaction: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if ids2 := readIDs(t, s2); len(ids2) != total {
		t.Fatalf("after reopen ids: want %d got %d", total, len(ids2))
	}
	t.Logf("reopen after compaction OK, rows=%d", total)
}

// TestCompactionConcurrentWrites 验证合并进行中持续写入：无重复、无丢失、
// 所有记录在合并结束后都恰好可见一次，且读取打开的文件数存在上界。
func TestCompactionConcurrentWrites(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	s, err := Create(dir, baseSchema(), Options{
		FlushThreshold:        2,
		CompactionThreshold:   4,
		MergeMaxRows:          25,
		DisableAutoCompaction: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	const writers = 4
	const perWriter = 80

	var wg sync.WaitGroup
	var writeErr atomic.Value
	stop := make(chan struct{})

	// 并发写入：每个 writer 写一段互不重叠的 id 区间，便于精确检测重复/丢失。
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := int64(w*perWriter) + 1
			for i := 0; i < perWriter; i++ {
				select {
				case <-stop:
					return
				default:
				}
				id := base + int64(i)
				row := Record{
					"id":     id,
					"name":   fmt.Sprintf("u-%d", id),
					"score":  float64(id),
					"active": true,
				}
				if err := s.Append(context.Background(), []Record{row}); err != nil {
					writeErr.Store(err)
					return
				}
				if i%5 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(w)
	}

	// 并发反复手动合并。
	compactorDone := make(chan struct{})
	go func() {
		defer close(compactorDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = s.Compact(context.Background())
			time.Sleep(2 * time.Millisecond)
		}
	}()

	// 并发扫描：任何时刻扫描器读到的数据必须自洽（无重复 id）。
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			ids := readIDs(t, s)
			seen := make(map[int64]int, len(ids))
			for _, id := range ids {
				seen[id]++
				if seen[id] > 1 {
					writeErr.Store(fmt.Errorf("duplicate id %d visible during scan (count=%d)", id, seen[id]))
					return
				}
			}
		}
	}()

	wg.Wait()
	close(stop)
	<-readerDone
	<-compactorDone // 确保测试自建的合并 goroutine 已退出，避免与收尾/Close 竞争

	if e := writeErr.Load(); e != nil {
		t.Fatalf("concurrent phase error: %v", e)
	}

	// 让所有 pending 落盘，再做一次最终合并。
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	// 循环直到本调用真正执行了一次合并（TryLock 撞车时返回 didWork=false 属正常）。
	var did bool
	for i := 0; i < 50; i++ {
		d, err := s.Compact(context.Background())
		if err != nil {
			t.Fatalf("final Compact: %v", err)
		}
		if d {
			did = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("final compaction didWork=%v stats=%+v", did, s.Stats())
	if !did {
		// 没有可合并内容时也允许：段数本就已在上界内。
		if st := s.Stats(); st.LiveSegments > (writers*perWriter)/s.opt.MergeMaxRows+1 {
			t.Fatalf("final compaction never executed and too many segments: %+v", s.Stats())
		}
	}

	ids := readIDs(t, s)
	if len(ids) != writers*perWriter {
		t.Fatalf("total rows: want %d got %d", writers*perWriter, len(ids))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for i, v := range ids {
		if v != int64(i+1) {
			t.Fatalf("missing or duplicate near position %d: got id %d", i, v)
		}
	}
	peak := s.openMax.Load()
	t.Logf("all %d records present exactly once; concurrent scan duplicate-free; open-segment peak=%d",
		len(ids), peak)
	if peak > 2 {
		t.Fatalf("open segments peak=%d exceeds bound 2 (scans must use 1, compaction <=2)", peak)
	}
	// 在 TempDir 清理前显式关闭，避免与 t.Cleanup 的 RemoveAll 竞争目录。
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestCompactionFailureRollsBack 覆盖两条失败注入路径：
// (1) 输出段尚未 rename 前失败；(2) 输出段已 rename、MANIFEST 尚未发布时失败。
// 两种情况下可见数据必须与合并前完全一致，且不留下临时/孤儿文件。
func TestCompactionFailureRollsBack(t *testing.T) {
	cases := []struct {
		name      string
		setFailFn func(s *Store)
		clearFn   func(s *Store)
	}{
		{
			name:      "fail before manifest swap (outputs still tmp)",
			setFailFn: func(s *Store) { s.fp.failBeforeManifestSwap.Store(true) },
			clearFn:   func(s *Store) { s.fp.failBeforeManifestSwap.Store(false) },
		},
		{
			name:      "fail after rename before manifest publish",
			setFailFn: func(s *Store) { s.fp.failAfterRenameBeforeManifest.Store(true) },
			clearFn:   func(s *Store) { s.fp.failAfterRenameBeforeManifest.Store(false) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
			const total = 18
			if err := s.Append(context.Background(), mkRows(total, 1)); err != nil {
				t.Fatalf("Append: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("Close/flush: %v", err)
			}
			s, err = Open(dir, Options{FlushThreshold: 3, CompactionThreshold: 4, DisableAutoCompaction: true})
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			beforeIDs := readIDs(t, s)
			beforeStats := s.Stats()
			t.Logf("before failing compaction: live=%d rows=%d ids=[%d..%d]",
				beforeStats.LiveSegments, beforeStats.Rows, beforeIDs[0], beforeIDs[len(beforeIDs)-1])

			tc.setFailFn(s)
			_, err = s.Compact(context.Background())
			var ce *CompactionError
			if !errors.As(err, &ce) {
				t.Fatalf("expected *CompactionError from injected failure, got %T: %v", err, err)
			}
			t.Logf("injected failure surfaced with reason=%q: %v", ce.Reason, ce)
			tc.clearFn(s)

			afterStats := s.Stats()
			afterIDs := readIDs(t, s)
			t.Logf("after rollback: live=%d deleted=%d rows=%d",
				afterStats.LiveSegments, afterStats.DeletedSegments, afterStats.Rows)

			// 可见状态必须完全回到合并前。
			if afterStats.LiveSegments != beforeStats.LiveSegments {
				t.Fatalf("live segments changed after rollback: before=%d after=%d",
					beforeStats.LiveSegments, afterStats.LiveSegments)
			}
			if len(afterIDs) != len(beforeIDs) {
				t.Fatalf("rows changed after rollback: before=%d after=%d", len(beforeIDs), len(afterIDs))
			}
			for i := range beforeIDs {
				if beforeIDs[i] != afterIDs[i] {
					t.Fatalf("row %d changed after rollback: %d -> %d",
						i, beforeIDs[i], afterIDs[i])
				}
			}
			col, tmp := countSegFiles(t, dir)
			if tmp != 0 {
				t.Fatalf("orphan temp files left after rollback: tmp=%d", tmp)
			}
			if col != beforeStats.LiveSegments {
				t.Fatalf("segment files changed after rollback: col=%d want %d",
					col, beforeStats.LiveSegments)
			}
			t.Logf("rollback verified: %d rows intact, %d original segments, col files=%d tmp files=%d",
				len(afterIDs), afterStats.LiveSegments, col, tmp)

			// 清除故障后，下一次合并必须成功，数据仍然完整。
			did, err := s.Compact(context.Background())
			if err != nil || !did {
				t.Fatalf("compaction after recovery: did=%v err=%v", did, err)
			}
			if ids := readIDs(t, s); len(ids) != total {
				t.Fatalf("rows after successful re-compaction: want %d got %d", total, len(ids))
			}
			if st := s.Stats(); st.LiveSegments != 1 {
				t.Fatalf("segments after re-compaction: %d", st.LiveSegments)
			}
			t.Logf("recovery compaction succeeded: %d rows in 1 segment", total)
			s.Close()
		})
	}
}
