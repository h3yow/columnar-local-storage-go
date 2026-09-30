package columnstore

import (
	"fmt"
	"sort"
)

// ColumnType 是支持的列数据类型。
type ColumnType int

const (
	TypeUnknown ColumnType = 0
	TypeInt64   ColumnType = iota
	TypeFloat64
	TypeString
	TypeBool
	TypeBytes
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
	case TypeBytes:
		return "bytes"
	default:
		return fmt.Sprintf("unknown(%d)", int(t))
	}
}

// Column 描述一列。ID 是跨 schema 版本稳定的身份；
// 改名只改 Name，删列只置 Deleted，ID 永不复用，
// 因此列顺序与列数据永远按 ID 对齐，不会因改名/删列而串列。
type Column struct {
	ID           int32      `json:"id"`
	Name         string     `json:"name"`
	Type         ColumnType `json:"type"`
	Nullable     bool       `json:"nullable"`
	Deleted      bool       `json:"deleted"`
	DefaultValue any        `json:"-"`
}

// 缺省值经过 manifest(JSON) 落盘时必须保留精确 Go 类型，否则 int64 会被
// 解码成 float64，破坏"写回读回完全一致"。用带类型标记的结构序列化。
type columnJSON struct {
	ID             int32      `json:"id"`
	Name           string     `json:"name"`
	Type           ColumnType `json:"type"`
	Nullable       bool       `json:"nullable"`
	Deleted        bool       `json:"deleted"`
	DefaultPresent bool       `json:"has_default"`
	Default        any        `json:"default,omitempty"`
}

func (c Column) MarshalJSON() ([]byte, error) {
	cj := columnJSON{
		ID: c.ID, Name: c.Name, Type: c.Type,
		Nullable: c.Nullable, Deleted: c.Deleted,
		DefaultPresent: c.DefaultValue != nil,
		Default:        typedJSONValue(c.Type, c.DefaultValue),
	}
	return jsonMarshal(cj)
}

func (c *Column) UnmarshalJSON(data []byte) error {
	var cj columnJSON
	if err := jsonUnmarshal(data, &cj); err != nil {
		return err
	}
	c.ID, c.Name, c.Type = cj.ID, cj.Name, cj.Type
	c.Nullable, c.Deleted = cj.Nullable, cj.Deleted
	if cj.DefaultPresent {
		v, err := decodeTypedJSONValue(cj.Type, cj.Default)
		if err != nil {
			return fmt.Errorf("decode default for column %q: %w", cj.Name, err)
		}
		c.DefaultValue = v
	}
	return nil
}

// Schema 是某个版本的列集合。Columns 保留全部历史列（含已删除），按 ID 排列。
type Schema struct {
	Version int      `json:"version"`
	Columns []Column `json:"columns"`
}

// NewSchema 用给定列定义创建第 1 版 schema，ID 按声明顺序从 1 开始分配。
// 不允许重名列。
func NewSchema(cols []Column) (*Schema, error) {
	s := &Schema{Version: 1}
	names := map[string]struct{}{}
	for i, c := range cols {
		if c.Name == "" {
			return nil, fmt.Errorf("%w: column #%d has empty name", ErrIncompatibleSchema, i)
		}
		if _, dup := names[c.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate column name %q", ErrIncompatibleSchema, c.Name)
		}
		names[c.Name] = struct{}{}
		if c.Type == TypeUnknown {
			return nil, fmt.Errorf("%w: column %q has unknown type", ErrIncompatibleSchema, c.Name)
		}
		if err := validateDefault(c.Type, c.Nullable, c.DefaultValue, false); err != nil {
			return nil, wrapColumnErr("init", c.Name, err)
		}
		c.ID = int32(i + 1)
		c.Deleted = false
		s.Columns = append(s.Columns, c)
	}
	return s, nil
}

// ColumnByName 返回当前未删除的列。
func (s *Schema) ColumnByName(name string) (Column, bool) {
	for _, c := range s.Columns {
		if !c.Deleted && c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// ColumnByID 返回任意状态的列（含已删除），供段文件按 ID 对齐时使用。
func (s *Schema) ColumnByID(id int32) (Column, bool) {
	for _, c := range s.Columns {
		if c.ID == id {
			return c, true
		}
	}
	return Column{}, false
}

// ActiveColumns 按 ID 升序返回未删除的列。
func (s *Schema) ActiveColumns() []Column {
	var out []Column
	for _, c := range s.Columns {
		if !c.Deleted {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (s *Schema) clone() *Schema {
	cp := &Schema{Version: s.Version, Columns: make([]Column, len(s.Columns))}
	copy(cp.Columns, s.Columns)
	return cp
}

// validateDefault 校验缺省值。requireDefaultForNonNullable=true（用于"后续加列"）
// 时，非空列必须提供同类型非空缺省值，因为历史行需要回填；
// 初始建表允许非空列无缺省，缺失在写入时按 ErrColumnMissing 拒绝。
func validateDefault(t ColumnType, nullable bool, def any, requireDefaultForNonNullable bool) error {
	if def == nil {
		if !nullable && requireDefaultForNonNullable {
			return fmt.Errorf("%w: non-nullable column requires a non-null default for existing rows", ErrIncompatibleSchema)
		}
		return nil
	}
	return checkValueType(t, def)
}

// checkValueType 严格检查 Go 值与列类型是否一致，不做隐式数值转换。
func checkValueType(t ColumnType, v any) error {
	var bad bool
	switch t {
	case TypeInt64:
		_, bad = v.(int64)
	case TypeFloat64:
		_, bad = v.(float64)
	case TypeString:
		_, bad = v.(string)
	case TypeBool:
		_, bad = v.(bool)
	case TypeBytes:
		_, bad = v.([]byte)
	default:
		return fmt.Errorf("%w: unknown column type %s", ErrTypeMismatch, t)
	}
	if !bad {
		return fmt.Errorf("%w: expected %s, got %T", ErrTypeMismatch, t, v)
	}
	return nil
}

// addColumn 在当前 schema 上增加一列，生成新版本。
// 新列对历史行的取值：显式 DefaultValue，否则可空列为 null。
func addColumn(s *Schema, name string, t ColumnType, nullable bool, def any) (*Schema, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: column has empty name", ErrIncompatibleSchema)
	}
	if t == TypeUnknown {
		return nil, fmt.Errorf("%w: column %q has unknown type", ErrIncompatibleSchema, name)
	}
	if _, exists := s.ColumnByName(name); exists {
		return nil, fmt.Errorf("%w: column %q already exists", ErrIncompatibleSchema, name)
	}
	if err := validateDefault(t, nullable, def, true); err != nil {
		return nil, wrapColumnErr("add column", name, err)
	}
	ns := s.clone()
	maxID := int32(0)
	for _, c := range ns.Columns {
		if c.ID > maxID {
			maxID = c.ID
		}
	}
	ns.Columns = append(ns.Columns, Column{
		ID:           maxID + 1,
		Name:         name,
		Type:         t,
		Nullable:     nullable,
		DefaultValue: def,
	})
	ns.Version = s.Version + 1
	return ns, nil
}

// renameColumn 只改名称映射，ID 与段内数据保持不变。
func renameColumn(s *Schema, oldName, newName string) (*Schema, error) {
	if newName == "" {
		return nil, fmt.Errorf("%w: new column name is empty", ErrIncompatibleSchema)
	}
	c, ok := s.ColumnByName(oldName)
	if !ok {
		return nil, wrapColumnErr("rename", oldName, ErrNotFound)
	}
	if _, clash := s.ColumnByName(newName); clash {
		return nil, fmt.Errorf("%w: column name %q already in use", ErrIncompatibleSchema, newName)
	}
	ns := s.clone()
	for i := range ns.Columns {
		if ns.Columns[i].ID == c.ID {
			ns.Columns[i].Name = newName
		}
	}
	ns.Version = s.Version + 1
	return ns, nil
}

// dropColumn 将列标记删除；ID 与历史段文件保留，新记录不得再写该列。
func dropColumn(s *Schema, name string) (*Schema, error) {
	c, ok := s.ColumnByName(name)
	if !ok {
		return nil, wrapColumnErr("drop", name, ErrNotFound)
	}
	ns := s.clone()
	for i := range ns.Columns {
		if ns.Columns[i].ID == c.ID {
			ns.Columns[i].Deleted = true
		}
	}
	ns.Version = s.Version + 1
	return ns, nil
}
