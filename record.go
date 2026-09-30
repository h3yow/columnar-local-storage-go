package columnstore

import "fmt"

// Record 是一条结构化记录，键为当前 schema 的列名。
type Record map[string]any

// Batch 是一次写入的一批记录。
type Batch []Record

// validateRecord 对照当前 schema 校验一条记录：
//   - 多出的列（含已删除列、改名前的旧名）-> ErrUnknownColumn，绝不静默忽略；
//   - 缺失列：有缺省值或缺省为 null（可空列）时合法，否则 ErrColumnMissing；
//   - 显式 null 写入非空列 -> ErrNullInNonNullable；
//   - 值类型不匹配 -> ErrTypeMismatch。
func validateRecord(s *Schema, rec Record) error {
	for _, c := range s.ActiveColumns() {
		v, present := rec[c.Name]
		if !present {
			if c.DefaultValue != nil {
				continue
			}
			if c.Nullable {
				continue // 缺省即 null，语义稳定
			}
			return wrapColumnErr("write", c.Name, ErrColumnMissing)
		}
		if v == nil {
			if !c.Nullable {
				return wrapColumnErr("write", c.Name, ErrNullInNonNullable)
			}
			continue
		}
		if err := checkValueType(c.Type, v); err != nil {
			return wrapColumnErr("write", c.Name, err)
		}
	}
	for name := range rec {
		if _, ok := s.ColumnByName(name); !ok {
			return wrapColumnErr("write", name, ErrUnknownColumn)
		}
	}
	return nil
}

// cellValue 返回记录在某列上的取值与是否为 null：
// 显式值优先，其次缺省值，最后可空列的 null。
func cellValue(c Column, rec Record) (v any, isNull bool, err error) {
	raw, present := rec[c.Name]
	if !present {
		if c.DefaultValue != nil {
			return c.DefaultValue, c.DefaultValue == nil, nil
		}
		if c.Nullable {
			return nil, true, nil
		}
		return nil, false, fmt.Errorf("%w: %s", ErrColumnMissing, c.Name)
	}
	if raw == nil {
		return nil, true, nil
	}
	return raw, false, nil
}
