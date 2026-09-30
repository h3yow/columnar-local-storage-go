package columnstore

import (
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"
)

// 篡改段内 schema：把同一列 ID 的类型从 int64 改成 string，
// 读取必须返回 ErrIncompatibleSchema，而不是按错误结构读出。
func TestReadRejectsCrossFileTypeChange(t *testing.T) {
	s, dir := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	if err := s.Write(Batch{{"id": int64(1), "name": "a", "score": float64(1)}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新解包段文件，只改 header schema 中 id 列的类型，其余字节保持。
	path := filepath.Join(dir, "seg-0000000001.cs")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	magicLen := len(segmentMagic)
	n := readU32(data[magicLen : magicLen+4])
	schemaBytes := data[magicLen+4 : magicLen+4+int(n)]
	var sc Schema
	if err := gob.NewDecoder(bytesReader(schemaBytes)).Decode(&sc); err != nil {
		t.Fatal(err)
	}
	for i := range sc.Columns {
		if sc.Columns[i].Name == "id" {
			sc.Columns[i].Type = TypeString // 同 ID 改类型：不兼容
		}
	}
	fixedSchema := mustGob(t, &sc)
	out := append(append([]byte{}, data[:magicLen]...), u32Bytes(uint32(len(fixedSchema)))...)
	out = append(out, fixedSchema...)
	// 用原始剩余字节（含列块与 trailer）。
	out = append(out, data[magicLen+4+int(n):]...)
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(dir, nil, &Options{MaxSegments: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	_, err = s2.ReadColumns([]string{"id"})
	assertErrIs(t, err, ErrIncompatibleSchema, "read cross-file type-changed column")
	t.Logf("OK type drift across files is rejected, not misread: %v", err)
}

// 清单存在但段文件缺失：读取返回明确错误（而不是静默少行）。
func TestMissingSegmentFileIsError(t *testing.T) {
	s, dir := testStore(t, baseSchema(t), &Options{MaxSegments: 100})
	writeRows(t, s, 1, 2)
	writeRows(t, s, 3, 2)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "seg-0000000002.cs")); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir, nil, &Options{MaxSegments: 100})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if _, err := s2.ReadColumns([]string{"id"}); err == nil {
		t.Fatal("expected error when a manifest segment file is missing")
	} else {
		t.Logf("OK missing segment file surfaces as explicit error: %v", err)
	}
}
