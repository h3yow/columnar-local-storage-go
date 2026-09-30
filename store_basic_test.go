package columnstore

import (
	"errors"
	"testing"
)

func TestWriteAndReadRoundTrip(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100})

	b1 := Batch{
		{"id": int64(1), "name": "alice", "score": float64(1.5)},
		{"id": int64(2), "name": nil, "score": float64(2)},
		{"id": int64(3), "score": float64(3.5)}, // 缺 name -> 缺省 null
	}
	if err := s.Write(b1); err != nil {
		t.Fatalf("Write b1: %v", err)
	}
	b2 := Batch{
		{"id": int64(4), "name": "dave", "score": nil},
	}
	if err := s.Write(b2); err != nil {
		t.Fatalf("Write b2: %v", err)
	}
	st := logVisible(t, s, "after two writes")
	if st.SegmentCount != 2 || st.TotalRows != 4 {
		t.Fatalf("unexpected stats: %+v", st)
	}

	rs, err := s.ReadColumns([]string{"id", "name", "score"})
	if err != nil {
		t.Fatalf("ReadColumns: %v", err)
	}
	dumpRows(t, "roundtrip", rs)
	assertRoundTrip(t, s, []string{"id", "name", "score"},
		map[string][]any{
			"id":    {int64(1), int64(2), int64(3), int64(4)},
			"name":  {"alice", "", "", "dave"},
			"score": {float64(1.5), float64(2), float64(3.5), 0},
		},
		map[string][]bool{
			"name":  {false, true, true, false},
			"score": {false, false, false, true},
		},
	)
}

func TestAllTypesRoundTrip(t *testing.T) {
	sc, err := NewSchema([]Column{
		{Name: "i", Type: TypeInt64, Nullable: false},
		{Name: "f", Type: TypeFloat64, Nullable: false},
		{Name: "s", Type: TypeString, Nullable: false},
		{Name: "b", Type: TypeBool, Nullable: false},
		{Name: "raw", Type: TypeBytes, Nullable: false},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := testStore(t, sc, nil)
	bin := []byte{0x00, 0x01, 0xfe, 0xff}
	if err := s.Write(Batch{
		{"i": int64(-9), "f": float64(2.25), "s": "héllo", "b": true, "raw": bin},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	rs, err := s.ReadColumns([]string{"i", "f", "s", "b", "raw"})
	if err != nil {
		t.Fatal(err)
	}
	dumpRows(t, "alltypes", rs)
	if rs.Columns[4].Values[0].([]byte)[0] != 0 || len(rs.Columns[4].Values[0].([]byte)) != 4 {
		t.Fatalf("bytes mismatch: %#v", rs.Columns[4].Values[0])
	}
}

func TestWriteValidationErrors(t *testing.T) {
	sc, err := NewSchema([]Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "tag", Type: TypeString, Nullable: false, DefaultValue: "x"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s, _ := testStore(t, sc, &Options{MaxSegments: 100})

	cases := []struct {
		name string
		rec  Record
		want error
	}{
		{"missing required column", Record{"tag": "y"}, ErrColumnMissing},
		{"null in non-nullable", Record{"id": nil, "tag": "y"}, ErrNullInNonNullable},
		{"type mismatch int vs string", Record{"id": "1", "tag": "y"}, ErrTypeMismatch},
		{"implicit numeric conversion rejected", Record{"id": 1, "tag": "y"}, ErrTypeMismatch},
		{"unknown column", Record{"id": int64(1), "tag": "y", "ghost": 7}, ErrUnknownColumn},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := s.Write(Batch{tc.rec})
			assertErrIs(t, err, tc.want, tc.name)
		})
	}
	// 所有失败写入后可见行数必须仍为 0：失败不产生部分写入。
	if st := logVisible(t, s, "after rejected writes"); st.TotalRows != 0 || st.SegmentCount != 0 {
		t.Fatalf("rejected writes changed visible state: %+v", st)
	}
	// 默认值语义：省略 tag 时写入默认 "x"。
	if err := s.Write(Batch{{"id": int64(42)}}); err != nil {
		t.Fatalf("Write with default: %v", err)
	}
	assertRoundTrip(t, s, []string{"id", "tag"},
		map[string][]any{"id": {int64(42)}, "tag": {"x"}},
		map[string][]bool{"id": {false}, "tag": {false}})
}

func TestBatchAtomicReject(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	// 第 3 行非法，整批拒绝。
	bad := Batch{
		{"id": int64(1), "name": "a"},
		{"id": int64(2), "name": "b"},
		{"id": "not-an-int", "name": "c"},
	}
	err := s.Write(bad)
	assertErrIs(t, err, ErrTypeMismatch, "bad batch")
	var ce *ColumnError
	if !errors.As(err, &ce) || ce.Column != "id" {
		t.Fatalf("expected ColumnError on id, got %v", err)
	}
	if st := logVisible(t, s, "after atomic reject"); st.TotalRows != 0 {
		t.Fatalf("partial write visible: %+v", st)
	}
	countSegmentFiles(t, s.dir)
}

func TestReadUnknownColumn(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), nil)
	if err := s.Write(Batch{{"id": int64(1), "name": "a"}}); err != nil {
		t.Fatal(err)
	}
	_, err := s.ReadColumns([]string{"nope"})
	assertErrIs(t, err, ErrNotFound, "read missing column")
}

func TestEmptyWriteAndEmptyRead(t *testing.T) {
	s, _ := testStore(t, baseSchema(t), nil)
	if err := s.Write(nil); err != nil {
		t.Fatalf("empty write: %v", err)
	}
	rs, err := s.ReadColumns(nil)
	if err != nil {
		t.Fatal(err)
	}
	if rs.Rows != 0 {
		t.Fatalf("expected 0 rows, got %d", rs.Rows)
	}
}
