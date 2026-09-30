package columnstore

import (
	"errors"
	"fmt"
)

// 哨兵错误：调用方可用 errors.Is 区分失败原因。
var (
	// ErrNotFound 列或数据集不存在。
	ErrNotFound = errors.New("columnstore: not found")
	// ErrColumnMissing 记录缺少必填列（无缺省值且未显式给值）。
	ErrColumnMissing = errors.New("columnstore: column missing in record")
	// ErrNullInNonNullable 非空列被写入 null。
	ErrNullInNonNullable = errors.New("columnstore: null value in non-nullable column")
	// ErrTypeMismatch 记录值的 Go 类型与列声明类型不一致。
	ErrTypeMismatch = errors.New("columnstore: value type does not match schema")
	// ErrUnknownColumn 记录中出现 schema 里不存在（或已删除/改名前）的列。
	ErrUnknownColumn = errors.New("columnstore: unknown column in record")
	// ErrIncompatibleSchema schema 变更不兼容（例如同 ID 改类型）。
	ErrIncompatibleSchema = errors.New("columnstore: incompatible schema change")
	// ErrClosed 存储已关闭。
	ErrClosed = errors.New("columnstore: store closed")
	// ErrCompactionBusy 已有合并在进行。
	ErrCompactionBusy = errors.New("columnstore: compaction already running")
)

// ColumnError 携带出错的列名、操作与具体原因，便于调用方区分与断言。
type ColumnError struct {
	Column string
	Op     string
	Err    error
}

func (e *ColumnError) Error() string {
	if e.Column == "" {
		return fmt.Sprintf("columnstore: %s: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("columnstore: %s column %q: %v", e.Op, e.Column, e.Err)
}

func (e *ColumnError) Unwrap() error { return e.Err }

func wrapColumnErr(op, col string, err error) error {
	return &ColumnError{Column: col, Op: op, Err: err}
}
