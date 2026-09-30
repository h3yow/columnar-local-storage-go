package columnstore

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// 合并失败回滚：注入"清单替换前失败"，可见段、行数与读回数据必须完全回到合并前；
// 临时段文件不得残留；再次合并（去掉注入）必须成功且数据一致。
func TestCompactionFailureRollback(t *testing.T) {
	s, dir := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	writeRows(t, s, 1, 3)
	writeRows(t, s, 4, 3)
	writeRows(t, s, 7, 3)
	before := logVisible(t, s, "before failed compaction")
	if before.SegmentCount != 3 || before.TotalRows != 9 {
		t.Fatalf("setup state wrong: %+v", before)
	}

	injected := errors.New("injected compaction failure")
	s.SetCompactionFailureHook(func(stage string) error {
		t.Logf("HOOK: failing compaction at stage=%s; pre-state segments=%d rows=%d",
			stage, before.SegmentCount, before.TotalRows)
		return injected
	})
	err := s.ForceCompaction(context.Background())
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected error, got %v", err)
	}
	t.Logf("OK compaction returned injected error: %v", err)
	s.SetCompactionFailureHook(nil)

	after := logVisible(t, s, "after failed compaction")
	if after.SegmentCount != before.SegmentCount || after.TotalRows != before.TotalRows || after.SchemaVersion != before.SchemaVersion {
		t.Fatalf("rollback incomplete: before=%+v after=%+v", before, after)
	}
	// 数据读回必须仍是合并前的 9 行，无重复/丢失/串列。
	want := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9}
	verifyAllIDs(t, s, want, "after rollback")
	rs, err := s.ReadColumns([]string{"id", "name", "score"})
	if err != nil {
		t.Fatal(err)
	}
	dumpRows(t, "rollback", rs)
	// 临时/提升后未发布的段文件都必须清理干净。
	files := countSegmentFiles(t, dir)
	if files != 3 {
		t.Fatalf("expected 3 segment files after rollback, got %d", files)
	}

	// 回滚后写入仍正常，下一次合并必须成功。
	writeRows(t, s, 10, 2)
	if err := s.ForceCompaction(context.Background()); err != nil {
		t.Fatalf("compaction after rollback failed: %v", err)
	}
	st := logVisible(t, s, "after recovery compaction")
	if st.SegmentCount != 1 || st.TotalRows != 11 {
		t.Fatalf("recovery compaction state: %+v", st)
	}
	want2 := make([]int64, 11)
	for i := range want2 {
		want2[i] = int64(i + 1)
	}
	verifyAllIDs(t, s, want2, "after recovery")
}

// 只有一个段时合并是空操作；已有合并进行时 ForceCompaction 返回 ErrCompactionBusy。
func TestCompactionBusyAndTrivial(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	writeRows(t, s, 1, 2)
	if err := s.ForceCompaction(context.Background()); err != nil {
		t.Fatalf("trivial compaction should be no-op, got %v", err)
	}
	if st := s.Stats(); st.SegmentCount != 1 {
		t.Fatalf("single segment should remain 1, got %d", st.SegmentCount)
	}

	// 模拟占用合并位。
	s.mu.Lock()
	s.comping = true
	s.mu.Unlock()
	err := s.ForceCompaction(context.Background())
	assertErrIs(t, err, ErrCompactionBusy, "second concurrent compaction")
	s.mu.Lock()
	s.comping = false
	s.mu.Unlock()
	t.Logf("OK compaction busy is distinguishable: %v", err)
}

// 演进与回滚交错：合并失败后加列，旧行仍按缺省语义读回。
func TestCompactionRollbackThenEvolve(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	writeRows(t, s, 1, 4)
	writeRows(t, s, 5, 2)
	s.SetCompactionFailureHook(func(string) error { return fmt.Errorf("boom") })
	if err := s.ForceCompaction(context.Background()); err == nil {
		t.Fatal("expected injected failure")
	}
	s.SetCompactionFailureHook(nil)

	if err := s.AddColumn("tag", TypeString, true, nil); err != nil {
		t.Fatal(err)
	}
	writeRows(t, s, 7, 1)
	if err := s.Write(Batch{{"id": int64(8), "name": "new", "score": float64(8), "tag": "fresh"}}); err != nil {
		t.Fatal(err)
	}
	rs, err := s.ReadColumns([]string{"id", "tag"})
	if err != nil {
		t.Fatal(err)
	}
	dumpRows(t, "rollback-evolve", rs)
	if rs.Rows != 8 {
		t.Fatalf("rows=%d want 8", rs.Rows)
	}
	for i := 0; i < 7; i++ {
		if !rs.Columns[1].Nulls[i] {
			t.Fatalf("old row %d tag should be null after add-column post rollback", i)
		}
	}
	if rs.Columns[1].Nulls[7] || rs.Columns[1].Values[7] != "fresh" {
		t.Fatalf("new row tag should be %q, got null=%v", "fresh", rs.Columns[1].Nulls[7])
	}
	t.Logf("OK rollback then evolve: 7 old rows NULL, new row tag=fresh")
}
