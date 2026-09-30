package columnar

// Record 是一条以列 ID 为键的结构化记录。
// 使用列 ID（而非展示名）作为键，确保改名后存量数据不会错位。
// nil 表示 SQL null；合法的 Go 值类型：int64、float64、string、bool。
type Record map[ColumnID]any

// RowsFromRecords 是记录集合的便捷别名。
type Rows = []Record

// NewRecord 用键值对构造一条记录，参数必须为 (id, value) 交替出现，id 必须为 string。
func NewRecord(kvs ...any) Record {
	r := make(Record, len(kvs)/2)
	for i := 0; i+1 < len(kvs); i += 2 {
		id, _ := kvs[i].(string)
		r[id] = kvs[i+1]
	}
	return r
}

// Value 按列 ID 取值。未设置时返回 (nil,false)；显式 null 值返回 (nil,true)。
func (r Record) Value(id ColumnID) (any, bool) {
	v, ok := r[id]
	return v, ok
}

// Materialize 以目标 schema 为基准投影记录：
// 对记录中缺失、但目标 schema 存在的列补 null 缺省值（新增列的稳定缺省语义）；
// 不属于目标 schema 的列被剔除。调用前应已通过兼容性与类型校验。
func (r Record) Materialize(dst *Schema) Record {
	out := make(Record, len(dst.Columns))
	for _, c := range dst.Columns {
		if v, ok := r[c.ID]; ok {
			out[c.ID] = v
		} else {
			out[c.ID] = nil // 新增列的稳定缺省值
		}
	}
	return out
}
