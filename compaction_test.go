package columnstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func writeRows(t *testing.T, s *Store, start, n int) {
	t.Helper()
	b := make(Batch, n)
	for i := 0; i < n; i++ {
		id := start + i
		b[i] = Record{
			"id":    int64(id),
			"name":  fmt.Sprintf("name-%d", id),
			"score": float64(id) + 0.5,
		}
	}
	if err := s.Write(b); err != nil {
		t.Fatalf("write rows start=%d n=%d: %v", start, n, err)
	}
}

func verifyAllIDs(t *testing.T, s *Store, wantIDs []int64, note string) {
	t.Helper()
	rs, err := s.ReadColumns([]string{"id"})
	if err != nil {
		t.Fatalf("%s: ReadColumns: %v", note, err)
	}
	if rs.Rows != len(wantIDs) {
		t.Fatalf("%s: rows got %d want %d", note, rs.Rows, len(wantIDs))
	}
	seen := map[int64]int{}
	for _, v := range rs.Columns[0].Values {
		seen[v.(int64)]++
	}
	for _, id := range wantIDs {
		switch seen[id] {
		case 0:
			t.Fatalf("%s: LOST record id=%d (visible rows=%d)", note, id, rs.Rows)
		case 1:
		default:
			t.Fatalf("%s: DUPLICATE record id=%d x%d", note, id, seen[id])
		}
	}
	t.Logf("OK %s: %d ids verified, no duplicate/loss; visible_segments=%d rows=%d",
		note, len(wantIDs), s.Stats().SegmentCount, rs.Rows)
}

// 基本合并：多段合并为 1，数据不重不丢，合并后读回一致。
func TestCompactionBasic(t *testing.T) {
	s, dir := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	writeRows(t, s, 1, 3)
	writeRows(t, s, 4, 2)
	writeRows(t, s, 6, 4)
	if st := logVisible(t, s, "before compaction"); st.SegmentCount != 3 || st.TotalRows != 9 {
		t.Fatalf("pre-compaction state: %+v", st)
	}
	if err := s.ForceCompaction(context.Background()); err != nil {
		t.Fatalf("ForceCompaction: %v", err)
	}
	if st := logVisible(t, s, "after compaction"); st.SegmentCount != 1 || st.TotalRows != 9 {
		t.Fatalf("post-compaction state: %+v", st)
	}
	want := make([]int64, 9)
	for i := range want {
		want[i] = int64(i + 1)
	}
	verifyAllIDs(t, s, want, "after force compaction")
	assertRoundTrip(t, s, []string{"id", "name", "score"},
		map[string][]any{
			"id":    []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8), int64(9)},
			"name":  {"name-1", "name-2", "name-3", "name-4", "name-5", "name-6", "name-7", "name-8", "name-9"},
			"score": {1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5},
		}, nil)
	if n := countSegmentFiles(t, dir); n != 1 {
		t.Fatalf("expected 1 segment file after compaction, got %d", n)
	}

	// 合并后再写入 + 重开，状态仍正确。
	writeRows(t, s, 10, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, baseSchema(t), &Options{MaxSegments: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	st := logVisible(t, s2, "reopen after compaction")
	if st.TotalRows != 10 {
		t.Fatalf("rows after reopen: %d", st.TotalRows)
	}
}

// 自动后台合并：不断写小批次，可见段数被压缩到有界，不会随小文件无限增长。
func TestCompactionAutoKeepsSegmentsBounded(t *testing.T) {
	s, dir := testStore(t, baseSchema(t), &Options{MaxSegments: 3})
	for i := 0; i < 20; i++ {
		writeRows(t, s, i*2+1, 2)
	}
	// 给后台合并时间完成（可能触发多轮）。
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.Stats().SegmentCount <= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := logVisible(t, s, "after auto compaction")
	if st.TotalRows != 40 {
		t.Fatalf("total rows: %d want 40", st.TotalRows)
	}
	if st.SegmentCount > 2 {
		t.Fatalf("segment count unbounded: %d (want <= 2)", st.SegmentCount)
	}
	want := make([]int64, 40)
	for i := range want {
		want[i] = int64(i + 1)
	}
	verifyAllIDs(t, s, want, "after auto compaction")
	// 等待"已被合并移除但仍有在途读者"的延迟删除文件清理完。
	deadline2 := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline2) {
		if s.deferredGarbageCount() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 关闭会等待在途后台合并收敛（临时段要么发布要么清理），关闭后数据目录
	// 只应包含清单引用的正式段，文件数不随小批数量线性增长。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if n := countSegmentFiles(t, dir); n > 2 {
		t.Fatalf("segment files on disk unbounded: %d", n)
	}
}

// 合并期间并发写入：所有记录恰好出现一次；并发触发的合并要么成功要么放弃重试。
func TestCompactionConcurrentWriters(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 4})

	// 预置一些段，让首轮合并目标集合更大，拉长合并窗口。
	writeRows(t, s, 1, 5)
	writeRows(t, s, 6, 5)
	writeRows(t, s, 11, 5)

	var wg sync.WaitGroup
	const writers = 6
	const perWriter = 20
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := 16 + w*perWriter
			for i := 0; i < perWriter; i++ {
				id := start + i
				if err := s.Write(Batch{{
					"id": int64(id), "name": fmt.Sprintf("c-%d", id), "score": float64(id),
				}}); err != nil {
					t.Errorf("writer %d id %d: %v", w, id, err)
					return
				}
				if i%7 == 0 {
					time.Sleep(time.Millisecond)
				}
			}
		}(w)
	}

	// 同时周期性触发强制合并（多数会与写入撞车而返回 ErrCompactionBusy 或稍后重试）。
	compDone := make(chan struct{})
	go func() {
		defer close(compDone)
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			err := s.ForceCompaction(context.Background())
			if err != nil && !errors.Is(err, ErrCompactionBusy) {
				t.Errorf("forced compaction: %v", err)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()
	<-compDone

	// 最终强制收敛一次。
	if err := s.ForceCompaction(context.Background()); err != nil {
		t.Fatalf("final compaction: %v", err)
	}
	totalRows := 15 + writers*perWriter
	want := make([]int64, totalRows)
	for i := range want {
		want[i] = int64(i + 1)
	}
	st := logVisible(t, s, "after concurrent writers + compactions")
	if st.TotalRows != totalRows {
		t.Fatalf("total rows got %d want %d", st.TotalRows, totalRows)
	}
	if st.SegmentCount != 1 {
		t.Fatalf("expected fully compacted to 1 segment, got %d", st.SegmentCount)
	}
	verifyAllIDs(t, s, want, "concurrent")
}

// 合并期间并发读取：读取永远成功且看到的行数单调不减（快照要么全旧要么全新）。
func TestCompactionConcurrentReaders(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 3})
	writeRows(t, s, 1, 4)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // writer
		defer wg.Done()
		for i := 0; i < 60; i++ {
			writeRows(t, s, 5+i*2, 2)
			time.Sleep(time.Millisecond)
		}
	}()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func(r int) {
			defer readers.Done()
			lastRows := 4
			for {
				select {
				case <-stop:
					return
				default:
				}
				rs, err := s.ReadColumns([]string{"id", "name"})
				if err != nil {
					t.Errorf("reader %d: %v", r, err)
					return
				}
				if rs.Rows < lastRows {
					t.Errorf("reader %d saw rows go backwards: %d -> %d", r, lastRows, rs.Rows)
					return
				}
				// 每次读取内两列必须等长且按行对齐。
				if len(rs.Columns[1].Values) != rs.Rows {
					t.Errorf("reader %d column length mismatch", r)
					return
				}
				lastRows = rs.Rows
			}
		}(r)
	}
	wg.Wait()
	close(stop)
	readers.Wait()
	if err := s.ForceCompaction(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := make([]int64, 4+60*2)
	for i := range want {
		want[i] = int64(i + 1)
	}
	verifyAllIDs(t, s, want, "concurrent readers")
}
