package columnar

// 本文件实现列式段文件的编码与解码。
//
// 文件布局（小端）：
//
//	magic(4)="CLS1" | formatVersion u16 | schemaVersion u32 | rowCount u32 | colCount u32
//	列定义 × colCount（段文件自描述，不依赖外部即可解码）：
//	  id:string | name:string | type:u8 | nullable:u8
//	  string := u32 长度 + UTF-8 字节
//	随后按列存放数据块：
//	  presenceBitmap((rowCount+7)/8 字节，bit i=1 表示该行该列非 null)
//	  nonNullCount u32
//	  values（仅按行顺序存放非 null 值）：
//	    int64/float64: 8 字节定长；bool: 1 字节；string: u32 长度 + UTF-8
//
// 写入时先写同目录临时文件并 fsync，再原子 rename，读者永远不会看到写一半的文件。
// SegmentReader 一次只持有一个段文件描述符；解码后数据在内存中按列存放。

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const (
	segMagic   = "CLS1"
	segFormatV = uint16(1)
)

// SegmentMeta 是 MANIFEST 中登记的一个段的元数据（不含数据本身）。
type SegmentMeta struct {
	ID            uint64 `json:"id"`
	FileName      string `json:"fileName"`
	SchemaVersion int    `json:"schemaVersion"`
	RowCount      int    `json:"rowCount"`
	// MinSeq/MaxSeq 是该段覆盖的记录序号区间（按写入顺序），用于确定扫描顺序。
	MinSeq uint64 `json:"minSeq"`
	MaxSeq uint64 `json:"maxSeq"`
	// Deleted=true 表示段已被合并取代，文件可能尚未删除，扫描时必须跳过。
	Deleted bool `json:"deleted"`
}

func segFileName(id uint64) string { return fmt.Sprintf("%s%016d%s", segPrefix, id, segSuffix) }

func segTmpName(id uint64) string { return segFileName(id) + tmpSuffix }

// compactTmpName 是合并输出段在正式发布前使用的临时文件名（不会与正式段混淆，
// 崩溃后由 Open 的 *.tmp 清理逻辑回收）。
func compactTmpName(token uint64) string {
	return fmt.Sprintf("comp-%016d%s", token, tmpSuffix)
}

// ---- 写入 --------------------------------------------------------------------

// SegmentWriter 将一批记录按列编码为段文件。记录先在内存中收集；
// Seal/Commit 时一次编码到临时文件，再由调用方原子 rename 成正式名。
type SegmentWriter struct {
	dir           string
	id            uint64 // 普通写入的正式 ID；合并输出在 Publish 时分配
	tmpName       string // 临时文件名（相对 dir）
	schemaVersion int
	schema        *Schema
	rows          []Record
	sealed        bool
	aborted       bool
}

// CreateSegment 准备一个普通新段（临时名 seg-<id>.col.tmp）。
func CreateSegment(dir string, id uint64, schema *Schema) (*SegmentWriter, error) {
	return newSegmentWriter(dir, id, segTmpName(id), schema)
}

// CreateCompactionOutput 准备一个合并输出段（临时名 comp-<token>.tmp）。
func CreateCompactionOutput(dir string, token uint64, schema *Schema) (*SegmentWriter, error) {
	return newSegmentWriter(dir, 0, compactTmpName(token), schema)
}

func newSegmentWriter(dir string, id uint64, tmpName string, schema *Schema) (*SegmentWriter, error) {
	if schema == nil {
		return nil, errors.New("columnar: cannot create segment without schema")
	}
	return &SegmentWriter{
		dir:           dir,
		id:            id,
		tmpName:       tmpName,
		schemaVersion: schema.Version,
		schema:        schema,
	}, nil
}

// Append 写入一行（假定已通过 schema 校验）。
func (sw *SegmentWriter) Append(r Record) error {
	if sw.sealed || sw.aborted {
		return errors.New("columnar: segment writer is finished")
	}
	// 防御性再校验一次，避免调用方绕过 Store 直接使用时写入脏数据。
	if err := sw.schema.ValidateRecord(&r); err != nil {
		return err
	}
	sw.rows = append(sw.rows, r)
	return nil
}

// Rows 返回已收集行数。
func (sw *SegmentWriter) Rows() int { return len(sw.rows) }

// TmpName 返回临时文件名。
func (sw *SegmentWriter) TmpName() string { return sw.tmpName }

// Seal 编码并 fsync 到临时文件（不做 rename）。
func (sw *SegmentWriter) Seal() error {
	if sw.aborted {
		return errors.New("columnar: segment already aborted")
	}
	if sw.sealed {
		return errors.New("columnar: segment already sealed")
	}
	tmpPath := filepath.Join(sw.dir, sw.tmpName)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return &CompactionError{Reason: ReasonWriteFailed, Detail: err.Error()}
	}
	wErr := sw.encode(f)
	if wErr == nil {
		wErr = f.Sync()
	}
	if cerr := f.Close(); wErr == nil {
		wErr = cerr
	}
	if wErr != nil {
		_ = os.Remove(tmpPath)
		return wErr
	}
	sw.sealed = true
	return nil
}

// Commit 用于普通写入：Seal 后原子 rename 为正式段名并 fsync 目录，返回段元数据。
func (sw *SegmentWriter) Commit(minSeq uint64) (SegmentMeta, error) {
	if err := sw.Seal(); err != nil {
		return SegmentMeta{}, err
	}
	finalName := segFileName(sw.id)
	if err := sw.renameFinal(finalName); err != nil {
		return SegmentMeta{}, err
	}
	return sw.meta(finalName, minSeq), nil
}

// renameFinal 将临时文件原子改名为正式段名并 fsync 目录。
func (sw *SegmentWriter) renameFinal(finalName string) error {
	if err := os.Rename(filepath.Join(sw.dir, sw.tmpName), filepath.Join(sw.dir, finalName)); err != nil {
		return &CompactionError{Reason: ReasonWriteFailed, Detail: err.Error()}
	}
	return fsyncDir(sw.dir)
}

func (sw *SegmentWriter) meta(finalName string, minSeq uint64) SegmentMeta {
	n := len(sw.rows)
	m := SegmentMeta{
		ID:            sw.id,
		FileName:      finalName,
		SchemaVersion: sw.schemaVersion,
		RowCount:      n,
		MinSeq:        minSeq,
	}
	if n > 0 {
		m.MaxSeq = minSeq + uint64(n) - 1
	} else {
		m.MaxSeq = minSeq - 1
	}
	return m
}

// Publish 用于合并输出：把已 Seal 的临时段 rename 为指定正式名。必须在 MANIFEST 发布前调用。
func (sw *SegmentWriter) Publish(id uint64, finalName string, minSeq uint64) (SegmentMeta, error) {
	if !sw.sealed {
		return SegmentMeta{}, errors.New("columnar: cannot publish unsealed segment")
	}
	if err := sw.renameFinal(finalName); err != nil {
		return SegmentMeta{}, err
	}
	m := sw.meta(finalName, minSeq)
	m.ID = id
	return m, nil
}

// Abort 放弃写入并清理临时文件。
func (sw *SegmentWriter) Abort() error {
	sw.aborted = true
	return os.Remove(filepath.Join(sw.dir, sw.tmpName))
}

func (sw *SegmentWriter) encode(f *os.File) error {
	w := &byteWriter{w: f}
	w.bytes([]byte(segMagic))
	w.u16(segFormatV)
	w.u32(uint32(sw.schemaVersion))
	w.u32(uint32(len(sw.rows)))
	w.u32(uint32(len(sw.schema.Columns)))
	for _, c := range sw.schema.Columns {
		w.str(c.ID)
		w.str(c.Name)
		w.u8(uint8(c.Type))
		if c.Nullable {
			w.u8(1)
		} else {
			w.u8(0)
		}
	}
	n := len(sw.rows)
	for ci := range sw.schema.Columns {
		c := &sw.schema.Columns[ci]
		bm := make([]byte, (n+7)/8)
		nonNull := 0
		for ri, r := range sw.rows {
			v := r[c.ID]
			if v != nil {
				bm[ri/8] |= 1 << uint(ri%8)
				nonNull++
			}
		}
		w.bytes(bm)
		w.u32(uint32(nonNull))
		for _, r := range sw.rows {
			v := r[c.ID]
			if v == nil {
				continue
			}
			if err := writeValue(w, c.Type, v); err != nil {
				return err
			}
		}
	}
	return w.err
}

func writeValue(w *byteWriter, t ColumnType, v any) error {
	switch t {
	case TypeInt64:
		x, ok := v.(int64)
		if !ok {
			return &SchemaError{Reason: ReasonTypeMismatch, Detail: fmt.Sprintf("expected int64 got %T", v)}
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(x))
		w.bytes(b[:])
	case TypeFloat64:
		x, ok := v.(float64)
		if !ok {
			return &SchemaError{Reason: ReasonTypeMismatch, Detail: fmt.Sprintf("expected float64 got %T", v)}
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], math.Float64bits(x))
		w.bytes(b[:])
	case TypeBool:
		x, ok := v.(bool)
		if !ok {
			return &SchemaError{Reason: ReasonTypeMismatch, Detail: fmt.Sprintf("expected bool got %T", v)}
		}
		if x {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case TypeString:
		x, ok := v.(string)
		if !ok {
			return &SchemaError{Reason: ReasonTypeMismatch, Detail: fmt.Sprintf("expected string got %T", v)}
		}
		w.str(x)
	default:
		return fmt.Errorf("columnar: unsupported column type %d", t)
	}
	return w.err
}

type byteWriter struct {
	w   io.Writer
	err error
}

func (b *byteWriter) bytes(p []byte) {
	if b.err != nil {
		return
	}
	_, b.err = b.w.Write(p)
}

func (b *byteWriter) u8(v uint8) { b.bytes([]byte{v}) }
func (b *byteWriter) u16(v uint16) {
	var buf [2]byte
	binary.LittleEndian.PutUint16(buf[:], v)
	b.bytes(buf[:])
}
func (b *byteWriter) u32(v uint32) {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	b.bytes(buf[:])
}
func (b *byteWriter) str(s string) {
	b.u32(uint32(len(s)))
	b.bytes([]byte(s))
}

// ---- 读取 --------------------------------------------------------------------

type columnData struct {
	def     ColumnDef
	present []bool
	ints    []int64
	floats  []float64
	strs    []string
	bools   []bool
}

// SegmentReader 顺序读取一个段文件；底层文件描述符在该段整个读取期间保持打开，
// Scanner 结束当前段、切换下一段前会 Close，因此一次扫描的段 FD 数恒为 1。
type SegmentReader struct {
	f        *os.File
	meta     SegmentMeta
	cols     []columnData
	rowCount int
	row      int
	closed   bool
}

// OpenSegment 打开一个段文件并读取头部与全部列数据。
func OpenSegment(dir, fileName string) (*SegmentReader, error) {
	path := filepath.Join(dir, fileName)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		_ = f.Close()
		return nil, &CompactionError{Reason: ReasonReadFailed, Detail: err.Error()}
	}
	r := &byteReader{buf: data}
	sr := &SegmentReader{f: f}
	if err := sr.decode(r); err != nil {
		_ = f.Close()
		return nil, err
	}
	if len(r.buf) != r.pos {
		_ = f.Close()
		return nil, fmt.Errorf("columnar: segment %s has %d trailing bytes", fileName, len(r.buf)-r.pos)
	}
	sr.meta = SegmentMeta{
		FileName:      fileName,
		SchemaVersion: sr.meta.SchemaVersion, // decode 中已写入真实版本
	}
	return sr, nil
}

func (sr *SegmentReader) decode(r *byteReader) error {
	magic := string(r.bytesN(4))
	if r.err != nil {
		return r.err
	}
	if magic != segMagic {
		return fmt.Errorf("columnar: bad segment magic %q", magic)
	}
	fv := r.u16()
	if fv != segFormatV {
		return fmt.Errorf("columnar: unsupported segment format version %d", fv)
	}
	schemaVersion := int(r.u32())
	rowCount := int(r.u32())
	colCount := int(r.u32())
	if r.err != nil {
		return r.err
	}
	sr.rowCount = rowCount
	cols := make([]columnData, colCount)
	schema := &Schema{Version: schemaVersion}
	for i := 0; i < colCount; i++ {
		id := r.str()
		name := r.str()
		ct := ColumnType(r.u8())
		nullable := r.u8() != 0
		if r.err != nil {
			return r.err
		}
		cols[i].def = ColumnDef{ID: id, Name: name, Type: ct, Nullable: nullable}
		schema.Columns = append(schema.Columns, cols[i].def)
	}
	for i := 0; i < colCount; i++ {
		cd := &cols[i]
		cd.present = make([]bool, rowCount)
		if rowCount > 0 {
			bm := r.bytesN((rowCount + 7) / 8)
			if r.err != nil {
				return r.err
			}
			for ri := 0; ri < rowCount; ri++ {
				cd.present[ri] = bm[ri/8]&(1<<uint(ri%8)) != 0
			}
		}
		nonNull := int(r.u32())
		if r.err != nil {
			return r.err
		}
		switch cd.def.Type {
		case TypeInt64:
			cd.ints = make([]int64, rowCount)
			for k := 0; k < nonNull; k++ {
				cd.ints[k] = int64(r.u64())
			}
		case TypeFloat64:
			cd.floats = make([]float64, rowCount)
			for k := 0; k < nonNull; k++ {
				cd.floats[k] = math.Float64frombits(r.u64())
			}
		case TypeBool:
			cd.bools = make([]bool, rowCount)
			for k := 0; k < nonNull; k++ {
				cd.bools[k] = r.u8() != 0
			}
		case TypeString:
			cd.strs = make([]string, rowCount)
			for k := 0; k < nonNull; k++ {
				cd.strs[k] = r.str()
			}
		default:
			return fmt.Errorf("columnar: unsupported column type %d in segment", cd.def.Type)
		}
		if r.err != nil {
			return r.err
		}
		// 把紧凑的非空值数组展开回按行索引（与 presence 对齐）。
		expandColumn(cd, rowCount)
	}
	sr.cols = cols
	sr.meta.SchemaVersion = schemaVersion
	return nil
}

// expandColumn 将“仅含非空值”的紧凑数组重排为按行索引的数组，空缺位保持零值。
func expandColumn(cd *columnData, rowCount int) {
	present := cd.present
	switch cd.def.Type {
	case TypeInt64:
		vals := cd.ints
		out := make([]int64, rowCount)
		k := 0
		for ri := 0; ri < rowCount; ri++ {
			if present[ri] {
				out[ri] = vals[k]
				k++
			}
		}
		cd.ints = out
	case TypeFloat64:
		vals := cd.floats
		out := make([]float64, rowCount)
		k := 0
		for ri := 0; ri < rowCount; ri++ {
			if present[ri] {
				out[ri] = vals[k]
				k++
			}
		}
		cd.floats = out
	case TypeBool:
		vals := cd.bools
		out := make([]bool, rowCount)
		k := 0
		for ri := 0; ri < rowCount; ri++ {
			if present[ri] {
				out[ri] = vals[k]
				k++
			}
		}
		cd.bools = out
	case TypeString:
		vals := cd.strs
		out := make([]string, rowCount)
		k := 0
		for ri := 0; ri < rowCount; ri++ {
			if present[ri] {
				out[ri] = vals[k]
				k++
			}
		}
		cd.strs = out
	}
}

// Meta 返回段元数据头部信息。
func (sr *SegmentReader) Meta() SegmentMeta { return sr.meta }

// SchemaVersion 返回段写入时的 schema 版本。
func (sr *SegmentReader) SchemaVersion() int { return sr.meta.SchemaVersion }

// Next 读取下一行到 dst（按段内列 ID 填充；未填充的列由上层按最新 schema 补 null）。
// 返回 io.EOF 表示结束。
func (sr *SegmentReader) Next(dst Record) error {
	if sr.closed {
		return errors.New("columnar: segment reader is closed")
	}
	if sr.row >= sr.rowCount {
		return io.EOF
	}
	ri := sr.row
	for i := range sr.cols {
		cd := &sr.cols[i]
		if !cd.present[ri] {
			dst[cd.def.ID] = nil
			continue
		}
		switch cd.def.Type {
		case TypeInt64:
			dst[cd.def.ID] = cd.ints[ri]
		case TypeFloat64:
			dst[cd.def.ID] = cd.floats[ri]
		case TypeBool:
			dst[cd.def.ID] = cd.bools[ri]
		case TypeString:
			dst[cd.def.ID] = cd.strs[ri]
		}
	}
	sr.row++
	return nil
}

// Close 关闭底层文件。
func (sr *SegmentReader) Close() error {
	if sr.closed {
		return nil
	}
	sr.closed = true
	if sr.f != nil {
		return sr.f.Close()
	}
	return nil
}

type byteReader struct {
	buf []byte
	pos int
	err error
}

func (r *byteReader) need(n int) []byte {
	if r.err != nil {
		return nil
	}
	if r.pos+n > len(r.buf) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	p := r.buf[r.pos : r.pos+n]
	r.pos += n
	return p
}

func (r *byteReader) bytesN(n int) []byte {
	p := r.need(n)
	if p == nil {
		return make([]byte, n)
	}
	return p
}

func (r *byteReader) u8() uint8 {
	p := r.need(1)
	if p == nil {
		return 0
	}
	return p[0]
}

func (r *byteReader) u16() uint16 {
	p := r.need(2)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint16(p)
}

func (r *byteReader) u32() uint32 {
	p := r.need(4)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint32(p)
}

func (r *byteReader) u64() uint64 {
	p := r.need(8)
	if p == nil {
		return 0
	}
	return binary.LittleEndian.Uint64(p)
}

func (r *byteReader) str() string {
	n := int(r.u32())
	if r.err != nil {
		return ""
	}
	p := r.need(n)
	if p == nil {
		return ""
	}
	return string(p)
}

// fsyncDir 确保目录项变更（rename）落盘。
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
