package columnstore

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testStore 创建临时目录下的存储，日志带测试名输出（关键输入与可见状态可追溯）。
func testStore(t *testing.T, schema *Schema, opts *Options) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	var buf bytes.Buffer
	lw := log.New(&buf, "", log.LstdFlags)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("[%s] store log:\n%s", t.Name(), buf.String())
		}
	})
	if opts == nil {
		opts = &Options{}
	}
	opts.Logger = lw
	s, err := Open(dir, schema, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func baseSchema(t *testing.T) *Schema {
	t.Helper()
	s, err := NewSchema([]Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "score", Type: TypeFloat64, Nullable: true},
	})
	if err != nil {
		t.Fatalf("NewSchema: %v", err)
	}
	return s
}

// logVisible 打印当前可见状态与判定依据。
func logVisible(t *testing.T, s *Store, note string) Stats {
	t.Helper()
	st := s.Stats()
	t.Logf("STATE %s: schema_v=%d visible_segments=%d total_rows=%d",
		note, st.SchemaVersion, st.SegmentCount, st.TotalRows)
	return st
}

func assertErrIs(t *testing.T, err error, target error, ctx string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected error wrapping %v, got nil", ctx, target)
	}
	if !errors.Is(err, target) {
		t.Fatalf("%s: expected error wrapping %v, got %v", ctx, target, err)
	}
	t.Logf("OK %s: got expected error: %v", ctx, err)
}

// assertRoundTrip 读回全部列并逐字段断言。
func assertRoundTrip(t *testing.T, s *Store, names []string, wantCols map[string][]any, wantNulls map[string][]bool) {
	t.Helper()
	rs, err := s.ReadColumns(names)
	if err != nil {
		t.Fatalf("ReadColumns %v: %v", names, err)
	}
	wantRows := 0
	for _, v := range wantCols {
		wantRows = len(v)
	}
	if rs.Rows != wantRows {
		t.Fatalf("rows mismatch: got %d want %d", rs.Rows, wantRows)
	}
	for _, cd := range rs.Columns {
		wantV, okV := wantCols[cd.Name]
		if !okV {
			continue
		}
		if len(cd.Values) != len(wantV) {
			t.Fatalf("column %q length: got %d want %d", cd.Name, len(cd.Values), len(wantV))
		}
		wantN := wantNulls[cd.Name]
		for i := 0; i < len(wantV); i++ {
			isNull := cd.Nulls[i]
			if wantN != nil {
				if isNull != wantN[i] {
					t.Fatalf("column %q row %d null flag: got %v want %v", cd.Name, i, isNull, wantN[i])
				}
			}
			if isNull {
				continue
			}
			if !valuesEqual(cd.Values[i], wantV[i]) {
				t.Fatalf("column %q row %d: got %#v (%T) want %#v (%T)",
					cd.Name, i, cd.Values[i], cd.Values[i], wantV[i], wantV[i])
			}
		}
		t.Logf("OK column %q: %d rows verified", cd.Name, len(cd.Values))
	}
}

func valuesEqual(a, b any) bool {
	switch av := a.(type) {
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	default:
		return a == b
	}
}

func countSegmentFiles(t *testing.T, dir string) int {
	t.Helper()
	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	n := 0
	var names []string
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".cs") || strings.HasSuffix(f.Name(), ".compacting") {
			n++
			names = append(names, f.Name())
		}
	}
	t.Logf("segment files on disk (%d): %v", n, names)
	return n
}

func manifestExists(t *testing.T, dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "MANIFEST.json"))
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("stat manifest: %v", err)
	}
	return err == nil
}

func dumpRows(t *testing.T, label string, rs *ResultSet) {
	t.Helper()
	for i := 0; i < rs.Rows; i++ {
		parts := make([]string, len(rs.Columns))
		for j, c := range rs.Columns {
			if c.Nulls[i] {
				parts[j] = fmt.Sprintf("%s=NULL", c.Name)
			} else {
				parts[j] = fmt.Sprintf("%s=%v", c.Name, c.Values[i])
			}
		}
		t.Logf("%s row %d: %s", label, i, strings.Join(parts, " "))
	}
}
