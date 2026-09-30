package columnar_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	columnar "github.com/h3yow/columnar-local-storage-go/columnar"
)

// Example_storeUsage 演示建库、批次写入、扫描读回与显式合并。
func Example_storeUsage() {
	dir := filepath.Join(os.TempDir(), "columnar-example")
	_ = os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	schema := columnar.Schema{
		Columns: []columnar.ColumnDef{
			{ID: "id", Name: "id", Type: columnar.TypeInt64, Nullable: false},
			{ID: "note", Name: "note", Type: columnar.TypeString, Nullable: true},
		},
	}
	s, err := columnar.Create(filepath.Join(dir, "db"), schema,
		columnar.Options{DisableAutoCompaction: true})
	if err != nil {
		panic(err)
	}

	batch := []columnar.Record{
		{"id": int64(1), "note": "a"},
		{"id": int64(2), "note": nil},
	}
	if err := s.Append(context.Background(), batch); err != nil {
		panic(err)
	}

	sc, err := s.Scan(context.Background())
	if err != nil {
		panic(err)
	}
	for {
		var r columnar.Record
		if err := sc.Next(&r); err != nil {
			break
		}
		fmt.Printf("id=%v note=%v\n", r["id"], r["note"])
	}
	_ = sc.Close()

	if _, err := s.Compact(context.Background()); err != nil {
		panic(err)
	}
	if err := s.Close(); err != nil {
		panic(err)
	}
	// Output:
	// id=1 note=a
	// id=2 note=<nil>
}
