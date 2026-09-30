package columnstore

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
)

// 段文件布局（本地文件，顺序写、按偏移定位读）：
//
//	[8B 魔数 "CSSEG001"]
//	[4B schema 长度][schema gob（完整列定义，含稳定列 ID）]
//	每个列块（按列 ID 顺序）:
//	  [4B 块长度][gob(Block)]
//	[gob(segmentIndex) 索引体][末尾 4B 索引体长度]
//
// 段一经 Close 即不可变；读侧按列 ID 对齐，永不依赖列在文件中的序号，
// 因而改名/删列不会造成串列。
const segmentMagic = "CSSEG001"

// Block 是某一列在一个段内的列式数据。
// Nulls 与类型切片始终等长（长度=Rows），空值位置在类型切片中为零值。
type Block struct {
	ColumnID int32
	Type     ColumnType
	Rows     int
	Nulls    []bool
	Ints     []int64
	Floats   []float64
	Strings  []string
	Bools    []bool
	Bytes    [][]byte
}

type blockIndexEntry struct {
	ColumnID int32
	Offset   int64 // 块"4B 长度前缀"相对文件头的偏移
	Length   int   // 块 gob 载荷字节数（不含 4B 长度前缀）
}

type segmentIndex struct {
	Rows    int
	Entries []blockIndexEntry
}

// segmentWriter 累积一个或多个批次的列式数据，Close 时一次性编码落盘。
// 多次 WriteBatch（合并时会用到）共享同一组列块，早先批次不会被覆盖。
type segmentWriter struct {
	path   string
	schema *Schema
	f      *os.File
	blocks []*Block // 与 schema 活跃列一一对应
	rows   int
	closed bool
}

func newSegmentWriter(path string, schema *Schema) (*segmentWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := &segmentWriter{path: path, schema: schema, f: f}
	if err := w.writeHeader(); err != nil {
		f.Close()
		return nil, err
	}
	for _, c := range schema.ActiveColumns() {
		w.blocks = append(w.blocks, &Block{ColumnID: c.ID, Type: c.Type})
	}
	return w, nil
}

func (w *segmentWriter) writeHeader() error {
	if _, err := io.WriteString(w.f, segmentMagic); err != nil {
		return err
	}
	buf, err := gobEncode(w.schema)
	if err != nil {
		return fmt.Errorf("encode schema: %w", err)
	}
	if err := writeUint32(w.f, uint32(len(buf))); err != nil {
		return err
	}
	_, err = w.f.Write(buf)
	return err
}

// WriteBatch 把一批记录追加进内存列块；实际写文件在 Close 完成。
func (w *segmentWriter) WriteBatch(b Batch) error {
	if w.closed {
		return fmt.Errorf("segment writer closed")
	}
	if len(b) == 0 {
		return nil
	}
	cols := w.schema.ActiveColumns()
	if len(cols) != len(w.blocks) {
		return fmt.Errorf("internal: writer block/schema column mismatch")
	}
	for _, rec := range b {
		for i, c := range cols {
			v, isNull, err := cellValue(c, rec)
			if err != nil {
				return wrapColumnErr("write segment", c.Name, err)
			}
			blk := w.blocks[i]
			blk.Nulls = append(blk.Nulls, isNull)
			appendZero(blk) // 先占类型槽位，保证所有切片等长
			if isNull {
				continue
			}
			switch c.Type {
			case TypeInt64:
				blk.Ints[len(blk.Ints)-1] = v.(int64)
			case TypeFloat64:
				blk.Floats[len(blk.Floats)-1] = v.(float64)
			case TypeString:
				blk.Strings[len(blk.Strings)-1] = v.(string)
			case TypeBool:
				blk.Bools[len(blk.Bools)-1] = v.(bool)
			case TypeBytes:
				blk.Bytes[len(blk.Bytes)-1] = v.([]byte)
			default:
				return wrapColumnErr("write segment", c.Name,
					fmt.Errorf("%w: unknown type", ErrTypeMismatch))
			}
		}
	}
	w.rows += len(b)
	return nil
}

func appendZero(blk *Block) {
	switch blk.Type {
	case TypeInt64:
		blk.Ints = append(blk.Ints, 0)
	case TypeFloat64:
		blk.Floats = append(blk.Floats, 0)
	case TypeString:
		blk.Strings = append(blk.Strings, "")
	case TypeBool:
		blk.Bools = append(blk.Bools, false)
	case TypeBytes:
		blk.Bytes = append(blk.Bytes, nil)
	}
}

// Close 编码全部列块、写 trailer 索引并 fsync。
// 未成功 Close 的段不会被发布到清单，读取方绝不会看到半写状态。
func (w *segmentWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	idx := segmentIndex{Rows: w.rows}
	for _, blk := range w.blocks {
		blk.Rows = w.rows
		off, err := w.f.Seek(0, io.SeekCurrent)
		if err != nil {
			w.f.Close()
			return err
		}
		buf, err := gobEncode(blk)
		if err != nil {
			w.f.Close()
			return fmt.Errorf("encode block %d: %w", blk.ColumnID, err)
		}
		if err := writeUint32(w.f, uint32(len(buf))); err != nil {
			w.f.Close()
			return err
		}
		if _, err := w.f.Write(buf); err != nil {
			w.f.Close()
			return err
		}
		idx.Entries = append(idx.Entries, blockIndexEntry{
			ColumnID: blk.ColumnID, Offset: off, Length: len(buf),
		})
	}

	// trailer：索引体在前、末尾 4B 长度收尾。
	idxBuf, err := gobEncode(&idx)
	if err != nil {
		w.f.Close()
		return fmt.Errorf("encode index: %w", err)
	}
	if _, err := w.f.Write(idxBuf); err != nil {
		w.f.Close()
		return err
	}
	if err := writeUint32(w.f, uint32(len(idxBuf))); err != nil {
		w.f.Close()
		return err
	}
	if err := w.f.Sync(); err != nil {
		w.f.Close()
		return err
	}
	return w.f.Close()
}

// segmentReader 按列读取段文件，通过 trailer 偏移索引只定位目标列块。
type segmentReader struct {
	path   string
	f      *os.File
	schema *Schema
	index  *segmentIndex
	closed bool
}

func openSegmentReader(path string) (*segmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	if openHook != nil {
		openHook(path)
	}
	r := &segmentReader{path: path, f: f}
	if err := r.readHeader(); err != nil {
		f.Close()
		if closeHook != nil {
			closeHook(path)
		}
		return nil, err
	}
	if err := r.readIndex(); err != nil {
		f.Close()
		if closeHook != nil {
			closeHook(path)
		}
		return nil, err
	}
	return r, nil
}

// openHook/closeHook 仅供测试统计扫描期间同时打开的段文件句柄数。
var (
	openHook  func(path string)
	closeHook func(path string)
)

func (r *segmentReader) readHeader() error {
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(r.f, magic); err != nil {
		return fmt.Errorf("read %s magic: %w", r.path, err)
	}
	if string(magic) != segmentMagic {
		return fmt.Errorf("segment %s: bad magic", r.path)
	}
	var n [4]byte
	if _, err := io.ReadFull(r.f, n[:]); err != nil {
		return err
	}
	buf := make([]byte, binary.LittleEndian.Uint32(n[:]))
	if _, err := io.ReadFull(r.f, buf); err != nil {
		return fmt.Errorf("read schema: %w", err)
	}
	var s Schema
	if err := gobDecode(buf, &s); err != nil {
		return fmt.Errorf("decode schema: %w", err)
	}
	r.schema = &s
	return nil
}

func (r *segmentReader) readIndex() error {
	fi, err := r.f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if size < 4 {
		return fmt.Errorf("segment %s too short", r.path)
	}
	var lenBuf [4]byte
	if _, err := r.f.ReadAt(lenBuf[:], size-4); err != nil {
		return err
	}
	n := int64(binary.LittleEndian.Uint32(lenBuf[:]))
	if n <= 0 || n+4 > size {
		return fmt.Errorf("segment %s: corrupt trailer length %d (size %d)", r.path, n, size)
	}
	buf := make([]byte, n)
	if _, err := r.f.ReadAt(buf, size-4-n); err != nil {
		return fmt.Errorf("read index: %w", err)
	}
	var idx segmentIndex
	if err := gobDecode(buf, &idx); err != nil {
		return fmt.Errorf("decode index: %w", err)
	}
	r.index = &idx
	return nil
}

func (r *segmentReader) Schema() *Schema { return r.schema }

func (r *segmentReader) Rows() int { return r.index.Rows }

func (r *segmentReader) Path() string { return r.path }

// ReadBlock 只读取指定列块的文件区间，不解码其他列。
// 段写入时该列尚不存在（加列前的旧段）返回包装 ErrNotFound 的错误。
func (r *segmentReader) ReadBlock(columnID int32) (*Block, error) {
	for _, e := range r.index.Entries {
		if e.ColumnID != columnID {
			continue
		}
		buf := make([]byte, e.Length)
		if _, err := r.f.ReadAt(buf, e.Offset+4); err != nil {
			return nil, fmt.Errorf("read block %d: %w", columnID, err)
		}
		var blk Block
		if err := gobDecode(buf, &blk); err != nil {
			return nil, fmt.Errorf("decode block %d: %w", columnID, err)
		}
		if len(blk.Nulls) != blk.Rows || typedLen(&blk) != blk.Rows {
			return nil, fmt.Errorf("corrupt block %d in %s: rows=%d nulls=%d typed=%d",
				columnID, r.path, blk.Rows, len(blk.Nulls), typedLen(&blk))
		}
		return &blk, nil
	}
	return nil, fmt.Errorf("%w: block column %d in segment %s", ErrNotFound, columnID, r.path)
}

func (r *segmentReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.f.Close()
	if closeHook != nil {
		closeHook(r.path)
	}
	return err
}

func typedLen(b *Block) int {
	switch b.Type {
	case TypeInt64:
		return len(b.Ints)
	case TypeFloat64:
		return len(b.Floats)
	case TypeString:
		return len(b.Strings)
	case TypeBool:
		return len(b.Bools)
	case TypeBytes:
		return len(b.Bytes)
	}
	return -1
}

func gobEncode(v any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gobDecode(buf []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(buf)).Decode(v)
}

func writeUint32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}
