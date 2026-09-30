package columnstore

import (
	"errors"
	"testing"
)

// 加列：旧批次行按缺省值/null 补齐；新批次可写新列；跨段读回一致。
func TestEvolutionAddColumn(t *testing.T) {
	s, dir := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	if err := s.Write(Batch{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
	}); err != nil {
		t.Fatal(err)
	}
	// 加可空列：旧行缺省 null。
	if err := s.AddColumn("note", TypeString, true, nil); err != nil {
		t.Fatalf("AddColumn note: %v", err)
	}
	// 加非空列必须带缺省值。
	assertErrIs(t, s.AddColumn("ver", TypeInt64, false, nil), ErrIncompatibleSchema, "add non-nullable without default")
	if err := s.AddColumn("ver", TypeInt64, false, int64(1)); err != nil {
		t.Fatalf("AddColumn ver: %v", err)
	}
	// 缺省值类型不匹配也要拒绝。
	assertErrIs(t, s.AddColumn("bad", TypeInt64, true, 7), ErrTypeMismatch, "default wrong type (untyped int)")

	if err := s.Write(Batch{
		{"id": int64(3), "name": "c", "note": "new", "ver": int64(9)},
	}); err != nil {
		t.Fatal(err)
	}
	logVisible(t, s, "after add column + write")

	rs, err := s.ReadColumns([]string{"id", "note", "ver"})
	if err != nil {
		t.Fatal(err)
	}
	dumpRows(t, "add-column", rs)
	assertRoundTrip(t, s, []string{"id", "note", "ver"},
		map[string][]any{
			"id":   {int64(1), int64(2), int64(3)},
			"note": {"", "", "new"},
			"ver":  {int64(1), int64(1), int64(9)},
		},
		map[string][]bool{
			"note": {true, true, false},
		},
	)

	// 重新打开：schema 演进结果必须持久化。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, s.Schema(), &Options{MaxSegments: 100})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	st := logVisible(t, s2, "after reopen")
	if st.SchemaVersion != 3 {
		t.Fatalf("schema version after reopen: got %d want 3", st.SchemaVersion)
	}
	rs2, err := s2.ReadColumns([]string{"id", "note", "ver"})
	if err != nil {
		t.Fatal(err)
	}
	if rs2.Rows != 3 || rs2.Columns[2].Nulls[0] != false {
		t.Fatalf("reopen data mismatch: rows=%d ver[0]=%v", rs2.Rows, rs2.Columns[2].Values[0])
	}
	if rs2.Columns[2].Values[0] != int64(1) {
		t.Fatalf("reopen default value lost: ver[0]=%v", rs2.Columns[2].Values[0])
	}
	// 替换测试夹具的句柄，供 Cleanup 关闭 s2（s 已手动关闭）。
	s2.opts.Logger = s.opts.Logger
	t.Cleanup(func() { _ = s2.Close() })
}

// 改名：ID 不变，旧名读取失败、新名读到旧数据，绝不串列。
func TestEvolutionRenameNoShift(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	if err := s.Write(Batch{
		{"id": int64(1), "name": "alice", "score": float64(10)},
		{"id": int64(2), "name": "bob", "score": float64(20)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameColumn("name", "user"); err != nil {
		t.Fatalf("RenameColumn: %v", err)
	}
	// 旧名现在是未知列。
	_, err := s.ReadColumns([]string{"name"})
	assertErrIs(t, err, ErrNotFound, "read old name after rename")
	// 用旧名写入也必须被拒绝。
	err = s.Write(Batch{{"id": int64(3), "name": "x", "score": float64(1)}})
	assertErrIs(t, err, ErrUnknownColumn, "write with old name")
	// 改成已存在的名字 -> 冲突拒绝。
	assertErrIs(t, s.RenameColumn("user", "score"), ErrIncompatibleSchema, "rename to existing name")
	// 新名读到的是旧数据，且与 id/score 严格对齐，没有错位。
	assertRoundTrip(t, s, []string{"id", "user", "score"},
		map[string][]any{
			"id":    {int64(1), int64(2)},
			"user":  {"alice", "bob"},
			"score": {float64(10), float64(20)},
		}, nil)
	sc := s.Schema()
	var userID int32 = -1
	for _, c := range sc.Columns {
		if c.Name == "user" {
			userID = c.ID
		}
	}
	if userID != 2 {
		t.Fatalf("renamed column id changed: got %d want 2", userID)
	}
	t.Logf("OK rename keeps stable id=2; no column shift")
}

// 删列：标记删除后读不到、写不进；其它列对齐不变；ID 不复用。
func TestEvolutionDropColumn(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	if err := s.Write(Batch{
		{"id": int64(1), "name": "a", "score": float64(1)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.DropColumn("score"); err != nil {
		t.Fatalf("DropColumn: %v", err)
	}
	_, err := s.ReadColumns([]string{"score"})
	assertErrIs(t, err, ErrNotFound, "read dropped column")
	err = s.Write(Batch{{"id": int64(2), "name": "b", "score": float64(2)}})
	assertErrIs(t, err, ErrUnknownColumn, "write dropped column")
	// 删除后再加同名列：必须分配新 ID（不复用旧 ID），旧数据不会被误读成新列。
	if err := s.AddColumn("score", TypeString, true, nil); err != nil {
		t.Fatalf("re-add column: %v", err)
	}
	if err := s.Write(Batch{{"id": int64(3), "name": "c", "score": "fresh"}}); err != nil {
		t.Fatal(err)
	}
	rs, err := s.ReadColumns([]string{"id", "score"})
	if err != nil {
		t.Fatal(err)
	}
	dumpRows(t, "drop-readd", rs)
	// 第 0 行写于新 score 列存在之前 -> null（不会读到旧 float 列数据）。
	if !rs.Columns[1].Nulls[0] || rs.Columns[1].Values[1] != "fresh" {
		t.Fatalf("column id reuse caused cross-column read: %+v", rs.Columns[1])
	}
	if rs.Columns[1].Type != TypeString {
		t.Fatalf("re-added column type: got %s", rs.Columns[1].Type)
	}
	t.Logf("OK drop + re-add: old float rows read as NULL for new string column; no data bleed")
}

func TestEvolutionErrors(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), nil)
	// 重复加列。
	assertErrIs(t, s.AddColumn("id", TypeInt64, true, nil), ErrIncompatibleSchema, "duplicate add")
	// 改不存在的列名。
	assertErrIs(t, s.RenameColumn("ghost", "x"), ErrNotFound, "rename missing")
	// 删不存在的列。
	assertErrIs(t, s.DropColumn("ghost"), ErrNotFound, "drop missing")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.AddColumn("x", TypeBool, true, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("after close expected ErrClosed, got %v", err)
	}
}
