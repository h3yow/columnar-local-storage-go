package columnstore

import (
	"errors"
	"fmt"
)

// ColumnData 是读回的一列数据，行序在所有列之间一致，
// 按"段在清单中的顺序、段内行序"排列（即写入先后顺序）。
type ColumnData struct {
	Name   string
	Type   ColumnType
	Rows   int
	Nulls  []bool // 与行对齐；Nulls[i] 为 true 时 Values[i] 为类型零值
	Values []any
}

// ResultSet 是一次按列读取的结果。
type ResultSet struct {
	Rows    int
	Columns []ColumnData
}

// ReadColumns 按列名读回数据。列名不存在（含已删除/改名前）会返回
// 包装 ErrNotFound 的错误；跨段发现同 ID 类型不一致返回 ErrIncompatibleSchema。
//
// 实现上逐段顺序打开、读完即关，因此无论磁盘上有多少个小文件，
// 单次扫描同时打开的文件句柄数恒为 1（有上界，不随文件数线性增长）。
func (s *Store) ReadColumns(names []string) (*ResultSet, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	snapshot := s.mani.cloneForUpdate()
	s.mu.RUnlock()

	// 在扫描期间钉住快照中的段文件，防止并发合并在扫描中途删除它们。
	paths := make([]string, len(snapshot.Segments))
	for i, seg := range snapshot.Segments {
		paths[i] = seg.Path
	}
	s.pinSegments(paths)
	defer s.unpinSegments(paths)

	if len(names) == 0 {
		return &ResultSet{Rows: totalRows(snapshot)}, nil
	}

	cols := make([]Column, len(names))
	for i, name := range names {
		c, ok := snapshot.Schema.ColumnByName(name)
		if !ok {
			return nil, wrapColumnErr("read", name, ErrNotFound)
		}
		cols[i] = c
	}

	out := make([]ColumnData, len(names))
	for i, c := range cols {
		out[i] = ColumnData{Name: c.Name, Type: c.Type}
	}
	totalRowsRead := 0

	for _, meta := range snapshot.Segments {
		// 每段只打开一次，读完立即关闭：同时打开的文件数恒为 1。
		r, err := openSegmentReader(meta.Path)
		if err != nil {
			return nil, fmt.Errorf("open segment %s: %w", meta.Path, err)
		}
		segErr := func() error {
			defer r.Close()
			oldSchema := r.Schema()
			for i, c := range cols {
				// 先按稳定列 ID 校验段 header schema：同 ID 类型变化属于不兼容演进，
				// 必须明确拒绝（即使列块内嵌类型恰好能解码，也不能按错结构读出）。
				if oldCol, existed := oldSchema.ColumnByID(c.ID); existed && !oldCol.Deleted {
					if oldCol.Type != c.Type {
						return wrapColumnErr("read", c.Name,
							fmt.Errorf("%w: column id %d type mismatch in segment %s (stored %s, current %s)",
								ErrIncompatibleSchema, c.ID, meta.Path, oldCol.Type, c.Type))
					}
				}
				blk, err := r.ReadBlock(c.ID)
				if err != nil {
					if isNotFoundErr(err) {
						// 该段写于"加列之前"：整段按稳定缺省语义补齐。
						fillDefault(&out[i], c, r.Rows())
						continue
					}
					return err
				}
				if blk.Type != c.Type {
					return wrapColumnErr("read", c.Name,
						fmt.Errorf("%w: column id %d type mismatch in segment %s (stored %s, schema %s)",
							ErrIncompatibleSchema, c.ID, meta.Path, blk.Type, c.Type))
				}
				if blk.Rows != r.Rows() || len(blk.Nulls) != r.Rows() {
					return fmt.Errorf("corrupt segment %s: block rows mismatch for column %s",
						meta.Path, c.Name)
				}
				appendBlock(&out[i], c, blk)
			}
			return nil
		}()
		if segErr != nil {
			return nil, segErr
		}
		totalRowsRead += meta.Rows
	}

	// 列在全部段都缺失（整表写入早于加列）时，上面的逐段已补值。
	for i := range out {
		out[i].Rows = totalRowsRead
	}
	return &ResultSet{Rows: totalRowsRead, Columns: out}, nil
}

// appendBlock 把一个段内的列块按行追加到结果列。
func appendBlock(dst *ColumnData, c Column, blk *Block) {
	for row := 0; row < blk.Rows; row++ {
		dst.Nulls = append(dst.Nulls, blk.Nulls[row])
		if blk.Nulls[row] {
			dst.Values = append(dst.Values, zeroValue(c.Type))
			continue
		}
		switch c.Type {
		case TypeInt64:
			dst.Values = append(dst.Values, blk.Ints[row])
		case TypeFloat64:
			dst.Values = append(dst.Values, blk.Floats[row])
		case TypeString:
			dst.Values = append(dst.Values, blk.Strings[row])
		case TypeBool:
			dst.Values = append(dst.Values, blk.Bools[row])
		case TypeBytes:
			dst.Values = append(dst.Values, blk.Bytes[row])
		}
	}
}

// fillDefault 为"加列前"的旧段补齐 n 行：显式缺省值优先，否则 null。
func fillDefault(dst *ColumnData, c Column, n int) {
	for i := 0; i < n; i++ {
		if c.DefaultValue != nil {
			dst.Nulls = append(dst.Nulls, false)
			dst.Values = append(dst.Values, c.DefaultValue)
		} else {
			dst.Nulls = append(dst.Nulls, true)
			dst.Values = append(dst.Values, zeroValue(c.Type))
		}
	}
}

func zeroValue(t ColumnType) any {
	switch t {
	case TypeInt64:
		return int64(0)
	case TypeFloat64:
		return float64(0)
	case TypeString:
		return ""
	case TypeBool:
		return false
	case TypeBytes:
		return []byte(nil)
	}
	return nil
}

func isNotFoundErr(err error) bool {
	return errors.Is(err, ErrNotFound)
}
