package columnar

// 本文件集中定义组件对外暴露的错误类型，保证不同失败路径可被区分、可断言。

import "fmt"

// ErrStoreClosed 在已关闭的数据集上执行读写时返回。
var ErrStoreClosed = fmt.Errorf("columnar: store is closed")

// SchemaError 表示记录与当前 schema 不匹配（列缺失、null 写入非空列、类型不一致等）。
// Reason 使用预定义的 Reason* 常量，便于调用方区分具体原因。
type SchemaError struct {
	Reason string
	Column ColumnID
	Detail string
}

func (e *SchemaError) Error() string {
	s := "columnar: schema error: " + e.Reason
	if e.Column != "" {
		s += " on column " + e.Column
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// CompatibilityError 表示新 schema 与已有 schema 不兼容（删列、改名、收窄类型等）。
type CompatibilityError struct {
	Reason string
	Column ColumnID
	Detail string
}

func (e *CompatibilityError) Error() string {
	s := "columnar: incompatible schema change: " + e.Reason
	if e.Column != "" {
		s += " on column " + e.Column
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// CompactionError 表示合并压缩过程中的失败。
type CompactionError struct {
	Reason string
	Detail string
}

func (e *CompactionError) Error() string {
	s := "columnar: compaction failed: " + e.Reason
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// 错误原因常量。
const (
	ReasonColumnMissing   = "column_missing"
	ReasonNullNotNull     = "null_into_not_null_column"
	ReasonTypeMismatch    = "type_mismatch"
	ReasonColumnDropped   = "column_dropped"
	ReasonColumnRenamed   = "column_renamed"
	ReasonTypeChanged     = "column_type_changed"
	ReasonNullableTighten = "nullable_tightened"
	ReasonWriteFailed     = "write_failed"
	ReasonReadFailed      = "read_failed"
	ReasonConflict        = "manifest_conflict"
)
