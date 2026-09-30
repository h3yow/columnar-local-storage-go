package columnar

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

// TestSchemaEvolution_AddColumnDefaultNull 验证新增可空列对旧数据有稳定缺省语义（null），
// 且新数据写入、重开数据集后读回仍然一致。
func TestSchemaEvolution_AddColumnDefaultNull(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	s, err := Create(dir, baseSchema(), Options{FlushThreshold: 5, CompactionThreshold: 1000, DisableAutoCompaction: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	oldRows := mkRows(6, 1) // 跨两个段
	if err := s.Append(context.Background(), oldRows); err != nil {
		t.Fatalf("Append old: %v", err)
	}
	t.Logf("before evolve: stats=%+v currentVersion=v%d", s.Stats(), s.CurrentSchema().Version)

	// 演进：新增可空列 tag（string）；放宽一列（score 原本就可空）；同时给 name 改名。
	evolved := Schema{Columns: []ColumnDef{
		{ID: "id", Name: "id", Type: TypeInt64, Nullable: false},
		{ID: "name", Name: "display_name", Type: TypeString, Nullable: true}, // 仅改名
		{ID: "score", Name: "score", Type: TypeFloat64, Nullable: true},
		{ID: "active", Name: "active", Type: TypeBool, Nullable: false},
		{ID: "tag", Name: "tag", Type: TypeString, Nullable: true}, // 新增列
	}}
	if err := s.EvolveSchema(evolved); err != nil {
		t.Fatalf("EvolveSchema compatible: %v", err)
	}
	t.Logf("after evolve: currentVersion=v%d columns=%v",
		s.CurrentSchema().Version, colNames(s.CurrentSchemaPtr()))

	// 新批次必须显式提供新列（缺失应报错），提供 null 合法。
	bad := Record{"id": int64(100), "name": "new", "score": 1.0, "active": true}
	err = s.Append(context.Background(), []Record{bad})
	var se *SchemaError
	if !errors.As(err, &se) || se.Reason != ReasonColumnMissing || se.Column != "tag" {
		t.Fatalf("missing new column should be rejected, got %v", err)
	}
	t.Logf("write without new column rejected as expected: reason=%s column=%s", se.Reason, se.Column)

	newRows := []Record{
		{"id": int64(7), "name": "user-7", "score": 10.5, "active": true, "tag": "vip"},
		{"id": int64(8), "name": "user-8", "score": nil, "active": false, "tag": nil},
	}
	if err := s.Append(context.Background(), newRows); err != nil {
		t.Fatalf("Append new: %v", err)
	}

	got := readAll(t, s)
	if len(got) != 8 {
		t.Fatalf("rows: want 8 got %d", len(got))
	}
	for i := 0; i < 6; i++ {
		if got[i]["tag"] != nil {
			t.Fatalf("old row %d: new column tag must default to null, got %#v", i, got[i]["tag"])
		}
		if got[i]["name"] == nil && oldRows[i]["name"] != nil {
			t.Fatalf("old row %d: rename must not shift columns, name lost", i)
		}
	}
	if got[6]["tag"] != "vip" {
		t.Fatalf("new row tag: want vip got %#v", got[6]["tag"])
	}
	if got[7]["tag"] != nil {
		t.Fatalf("new row explicit null tag not preserved: %#v", got[7]["tag"])
	}
	t.Logf("evolution visible state: %d rows; old rows tag=<nil>; new row tag=%v",
		len(got), got[6]["tag"])

	// 关闭重开：schema 历史与缺省语义必须持久化。
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2, err := Open(dir, Options{DisableAutoCompaction: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s2.Close()
	got2 := readAll(t, s2)
	if len(got2) != 8 {
		t.Fatalf("after reopen rows: want 8 got %d", len(got2))
	}
	if got2[0]["tag"] != nil || got2[6]["tag"] != "vip" {
		t.Fatalf("after reopen default semantics broken: %#v %#v", got2[0]["tag"], got2[6]["tag"])
	}
	t.Logf("reopen OK: version=v%d rows=%d, old tag null / new tag=%v persisted",
		s2.CurrentSchema().Version, len(got2), got2[6]["tag"])
}

// TestSchemaEvolution_IncompatibleChangesRejected 验证删列、改类型、收紧 nullable、
// 新增非空列等不兼容变化被明确拒绝，且数据集状态保持不变。
func TestSchemaEvolution_IncompatibleChangesRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	s, err := Create(dir, baseSchema(), Options{DisableAutoCompaction: true})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()
	if err := s.Append(context.Background(), mkRows(2, 1)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	versionBefore := s.CurrentSchema().Version

	cases := []struct {
		name   string
		cols   []ColumnDef
		reason string
		col    ColumnID
	}{
		{
			name: "drop column",
			cols: []ColumnDef{
				{ID: "id", Name: "id", Type: TypeInt64, Nullable: false},
				{ID: "name", Name: "name", Type: TypeString, Nullable: true},
				{ID: "score", Name: "score", Type: TypeFloat64, Nullable: true},
				// active 被删除
			},
			reason: ReasonColumnDropped,
			col:    "active",
		},
		{
			name: "rename id (simulated by replacing id)",
			cols: []ColumnDef{
				{ID: "id", Name: "id", Type: TypeInt64, Nullable: false},
				{ID: "full_name", Name: "full_name", Type: TypeString, Nullable: true}, // name 的 ID 消失
				{ID: "score", Name: "score", Type: TypeFloat64, Nullable: true},
				{ID: "active", Name: "active", Type: TypeBool, Nullable: false},
			},
			reason: ReasonColumnDropped,
			col:    "name",
		},
		{
			name: "change type",
			cols: []ColumnDef{
				{ID: "id", Name: "id", Type: TypeInt64, Nullable: false},
				{ID: "name", Name: "name", Type: TypeString, Nullable: true},
				{ID: "score", Name: "score", Type: TypeString, Nullable: true}, // float64 -> string
				{ID: "active", Name: "active", Type: TypeBool, Nullable: false},
			},
			reason: ReasonTypeChanged,
			col:    "score",
		},
		{
			name: "tighten nullable",
			cols: []ColumnDef{
				{ID: "id", Name: "id", Type: TypeInt64, Nullable: false},
				{ID: "name", Name: "name", Type: TypeString, Nullable: false}, // nullable -> not null
				{ID: "score", Name: "score", Type: TypeFloat64, Nullable: true},
				{ID: "active", Name: "active", Type: TypeBool, Nullable: false},
			},
			reason: ReasonNullableTighten,
			col:    "name",
		},
		{
			name:   "new column not nullable",
			cols:   append(baseSchema().Columns, ColumnDef{ID: "tenant", Name: "tenant", Type: TypeString, Nullable: false}),
			reason: ReasonNullableTighten,
			col:    "tenant",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.EvolveSchema(Schema{Columns: tc.cols})
			var ce *CompatibilityError
			if !errors.As(err, &ce) {
				t.Fatalf("want *CompatibilityError, got %T: %v", err, err)
			}
			if ce.Reason != tc.reason || ce.Column != tc.col {
				t.Fatalf("compatibility error: want (%s,%s) got (%s,%s): %v",
					tc.reason, tc.col, ce.Reason, ce.Column, ce)
			}
			// 状态必须保持不变。
			if s.CurrentSchema().Version != versionBefore {
				t.Fatalf("schema version changed after rejected evolution: %d", s.CurrentSchema().Version)
			}
			got := readAll(t, s)
			if len(got) != 2 {
				t.Fatalf("visible rows changed after rejected evolution: %d", len(got))
			}
			t.Logf("rejected change=%q reason=%q column=%q | version stays v%d | visible rows=%d",
				tc.name, ce.Reason, ce.Column, versionBefore, len(got))
		})
	}
}

func colNames(s *Schema) []string {
	out := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		out[i] = c.ID + "->" + c.Name
	}
	return out
}
