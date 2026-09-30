package columnar

// 本文件定义列类型与 schema，以及 schema 演进的兼容性规则。

import "fmt"

// ColumnID 是列的稳定身份标识。schema 演进时改名不会改变 ID，因此不会发生数据串列。
type ColumnID = string

// ColumnType 是列的逻辑数据类型。
type ColumnType int

const (
	TypeInvalid ColumnType = iota
	TypeInt64
	TypeFloat64
	TypeString
	TypeBool
)

func (t ColumnType) String() string {
	switch t {
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	default:
		return "invalid"
	}
}

// ColumnDef 描述一列。ID 是列的稳定标识；Name 仅用于展示，改名不影响存量数据对齐。
type ColumnDef struct {
	ID   ColumnID   `json:"id"`
	Name string     `json:"name"`
	Type ColumnType `json:"type"`
	// Nullable=false 时写入 null 会得到 ReasonNullNotNull 错误。
	// 通过演进新增的列固定 Nullable=true，缺省值为 null。
	Nullable bool `json:"nullable"`
}

// Schema 是某一批数据写入时的列定义集合。版本单调递增。
type Schema struct {
	Version int         `json:"version"`
	Columns []ColumnDef `json:"columns"`
}

// Column 按 ID 查找列定义，找不到返回 nil。
func (s *Schema) Column(id ColumnID) *ColumnDef {
	for i := range s.Columns {
		if s.Columns[i].ID == id {
			return &s.Columns[i]
		}
	}
	return nil
}

// CheckCompatible 判断 s 是否是相对于 old 的合法演进。
//
// 允许的变化（兼容）：
//   - 新增列：新增列必须可空（Nullable=true），老数据以 null 作为稳定缺省值；
//   - 列改名：仅 Name 变化，ID 不变，存量数据仍按 ID 对齐；
//   - 放宽 nullable：false -> true。
//
// 拒绝的变化（不兼容，返回 *CompatibilityError）：
//   - 删除列（某 ID 在新版本消失）；
//   - 列 ID 变化（等价于删列+加列）；
//   - 列类型变化；
//   - 收紧 nullable（true -> false）。
func (s *Schema) CheckCompatible(old *Schema) error {
	for _, oc := range old.Columns {
		nc := s.Column(oc.ID)
		if nc == nil {
			return &CompatibilityError{
				Reason: ReasonColumnDropped,
				Column: oc.ID,
				Detail: fmt.Sprintf("column %q (%s) present in v%d is missing in v%d; removing or renaming a column id is not allowed",
					oc.Name, oc.ID, old.Version, s.Version),
			}
		}
		if nc.Type != oc.Type {
			return &CompatibilityError{
				Reason: ReasonTypeChanged,
				Column: oc.ID,
				Detail: fmt.Sprintf("column %q type changed from %s to %s", oc.ID, oc.Type, nc.Type),
			}
		}
		if oc.Nullable && !nc.Nullable {
			return &CompatibilityError{
				Reason: ReasonNullableTighten,
				Column: oc.ID,
				Detail: fmt.Sprintf("column %q changed from nullable to not-nullable", oc.ID),
			}
		}
	}
	// 新出现的列必须可空，否则老数据无法提供非空值。
	oldByID := make(map[ColumnID]struct{}, len(old.Columns))
	for _, oc := range old.Columns {
		oldByID[oc.ID] = struct{}{}
	}
	for _, nc := range s.Columns {
		if _, ok := oldByID[nc.ID]; ok {
			continue
		}
		if !nc.Nullable {
			return &CompatibilityError{
				Reason: ReasonNullableTighten,
				Column: nc.ID,
				Detail: fmt.Sprintf("newly added column %q must be nullable; existing rows have no value for it", nc.ID),
			}
		}
	}
	return nil
}

// ValidateRecord 按 schema 校验一条记录：
//   - schema 中的每一列都必须在记录中出现（缺失 -> ReasonColumnMissing）；
//   - 值为 nil 时列必须 Nullable（否则 -> ReasonNullNotNull）；
//   - 值的 Go 类型必须与列类型严格一致（否则 -> ReasonTypeMismatch）。
//
// 记录中多余的列（不属于当前 schema）同样按 ReasonColumnMissing 的逆情形处理：
// 这里直接拒绝，返回 ReasonTypeMismatch，避免拼写错误的列被静默丢弃。
func (s *Schema) ValidateRecord(r *Record) error {
	for _, c := range s.Columns {
		v, ok := (*r)[c.ID]
		if !ok {
			return &SchemaError{
				Reason: ReasonColumnMissing,
				Column: c.ID,
				Detail: fmt.Sprintf("record does not provide column %q", c.ID),
			}
		}
		if v == nil {
			if !c.Nullable {
				return &SchemaError{
					Reason: ReasonNullNotNull,
					Column: c.ID,
					Detail: fmt.Sprintf("null written into non-nullable column %q", c.ID),
				}
			}
			continue
		}
		if !goTypeMatches(v, c.Type) {
			return &SchemaError{
				Reason: ReasonTypeMismatch,
				Column: c.ID,
				Detail: fmt.Sprintf("column %q expects %s but got %T", c.ID, c.Type, v),
			}
		}
	}
	for id := range *r {
		if s.Column(id) == nil {
			return &SchemaError{
				Reason: ReasonTypeMismatch,
				Column: id,
				Detail: fmt.Sprintf("record provides unknown column %q not present in schema v%d", id, s.Version),
			}
		}
	}
	return nil
}

func goTypeMatches(v any, t ColumnType) bool {
	switch t {
	case TypeInt64:
		_, ok := v.(int64)
		return ok
	case TypeFloat64:
		_, ok := v.(float64)
		return ok
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeBool:
		_, ok := v.(bool)
		return ok
	default:
		return false
	}
}
