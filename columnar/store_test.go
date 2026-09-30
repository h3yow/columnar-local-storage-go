package columnar

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func baseSchema() Schema {
	return Schema{
		Columns: []ColumnDef{
			{ID: "id", Name: "id", Type: TypeInt64, Nullable: false},
			{ID: "name", Name: "name", Type: TypeString, Nullable: true},
			{ID: "score", Name: "score", Type: TypeFloat64, Nullable: true},
			{ID: "active", Name: "active", Type: TypeBool, Nullable: false},
		},
	}
}

// readAll 通过扫描器读出全部行，返回投影到当前 schema 后的记录。
func readAll(t *testing.T, s *Store) []Record {
	t.Helper()
	sc, err := s.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer sc.Close()
	var got []Record
	for {
		var r Record
		err := sc.Next(&r)
		if errors.Is(err, errEOF()) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		got = append(got, r)
	}
	return got
}

// errEOF 只是为了在测试里显式引用标准 io.EOF。
func errEOF() error { return ioEOF }

func mkRows(n int, offset int64) []Record {
	rows := make([]Record, n)
	for i := 0; i < n; i++ {
		id := offset + int64(i)
		r := Record{
			"id":     id,
			"name":   fmt.Sprintf("user-%d", id),
			"score":  float64(id) * 1.5,
			"active": id%2 == 0,
		}
		rows[i] = r
	}
	return rows
}

func TestWriteAndScanRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(filepath.Join(dir, "db"), baseSchema(), Options{
		FlushThreshold:        7,
		CompactionThreshold:   1000, // 关闭自动合并干扰
		DisableAutoCompaction: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	rows := mkRows(20, 1)
	if err := s.Append(context.Background(), rows); err != nil {
		t.Fatalf("Append: %v", err)
	}
	t.Logf("input rows=%d, stats=%+v", len(rows), s.Stats())

	got := readAll(t, s)
	if len(got) != 20 {
		t.Fatalf("row count: want 20 got %d", len(got))
	}
	for i := 0; i < 20; i++ {
		want := rows[i].Materialize(s.CurrentSchemaPtr())
		if !recordsEqual(want, got[i]) {
			t.Fatalf("row %d mismatch:\n want=%#v\n got =%#v", i, want, got[i])
		}
	}
	t.Logf("round-trip OK: %d rows across segments, sample id=%v name=%v null?=%v",
		len(got), got[0]["id"], got[0]["name"], got[0]["score"] == nil)
}

func TestNullAndMissingAndTypeErrorsAreDistinguishable(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(filepath.Join(dir, "db"), baseSchema(), Options{
		FlushThreshold:        100,
		DisableAutoCompaction: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer s.Close()

	cases := []struct {
		name   string
		row    Record
		reason string
		col    ColumnID
	}{
		{
			name:   "missing column",
			row:    Record{"id": int64(1), "active": true}, // 缺 name/score 也算
			reason: ReasonColumnMissing,
			col:    "name",
		},
		{
			name:   "null into not-null",
			row:    Record{"id": nil, "name": nil, "score": nil, "active": true},
			reason: ReasonNullNotNull,
			col:    "id",
		},
		{
			name:   "type mismatch",
			row:    Record{"id": int64(1), "name": "n", "score": "not-a-float", "active": true},
			reason: ReasonTypeMismatch,
			col:    "score",
		},
		{
			name:   "unknown column is not silently dropped",
			row:    Record{"id": int64(1), "name": "n", "score": 1.0, "active": true, "typo": 1},
			reason: ReasonTypeMismatch,
			col:    "typo",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Append(context.Background(), []Record{tc.row})
			var se *SchemaError
			if !errors.As(err, &se) {
				t.Fatalf("want *SchemaError, got %T: %v", err, err)
			}
			if se.Reason != tc.reason {
				t.Fatalf("reason: want %s got %s", tc.reason, se.Reason)
			}
			if se.Column != tc.col {
				t.Fatalf("column: want %s got %s", tc.col, se.Column)
			}
			t.Logf("input=%v -> error reason=%q column=%q: %v", tc.row, se.Reason, se.Column, se)
		})
	}

	// 合法的 null 值必须被精确保留。
	rows := []Record{
		{"id": int64(1), "name": nil, "score": 3.5, "active": true},
		{"id": int64(2), "name": "kate", "score": nil, "active": false},
	}
	if err := s.Append(context.Background(), rows); err != nil {
		t.Fatalf("Append null rows: %v", err)
	}
	got := readAll(t, s)
	if len(got) != 2 {
		t.Fatalf("want 2 rows (bad rows must not be persisted), got %d", len(got))
	}
	if got[0]["name"] != nil {
		t.Fatalf("name null not preserved: %#v", got[0]["name"])
	}
	if got[1]["score"] != nil {
		t.Fatalf("score null not preserved: %#v", got[1]["score"])
	}
	if got[0]["score"].(float64) != 3.5 {
		t.Fatalf("score value mismatch: %v", got[0]["score"])
	}
	t.Logf("null semantics preserved: row0.name=nil row1.score=nil; visible rows=%d", len(got))
}
