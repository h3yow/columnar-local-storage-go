package columnar

import (
	"fmt"
	"io"
	"os"
	"sort"
)

var ioEOF = io.EOF

// readDirNames 列出目录下的文件名。
func readDirNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// CurrentSchemaPtr 仅供测试使用：返回当前 schema 指针。
func (s *Store) CurrentSchemaPtr() *Schema {
	return s.manifest.Load().CurrentSchema()
}

// recordsEqual 按列 ID 比较两条记录（值类型严格一致）。
func recordsEqual(a, b Record) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if va == nil || vb == nil {
			if va != vb {
				return false
			}
			continue
		}
		if fmt.Sprintf("%v", va) != fmt.Sprintf("%v", vb) {
			return false
		}
	}
	return true
}

// sortedIDs 返回记录的列 ID，便于日志稳定输出。
func sortedIDs(r Record) []string {
	ids := make([]string, 0, len(r))
	for k := range r {
		ids = append(ids, k)
	}
	sort.Strings(ids)
	return ids
}
