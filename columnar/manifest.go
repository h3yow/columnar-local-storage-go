package columnar

// 本文件实现 MANIFEST（数据集元数据）的持久化与原子替换。
// MANIFEST 与段文件分离：数据在段文件里，MANIFEST 只记录“哪些段可见、当前 schema 历史”。
// 所有更新走“临时文件 + fsync + rename + 目录 fsync”，保证崩溃后要么旧状态、要么新状态。

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
)

// Manifest 是数据集的根元数据。
type Manifest struct {
	// NextSegmentID 单调递增，用于生成唯一段文件名。
	NextSegmentID uint64 `json:"nextSegmentId"`
	// NextSeq 是下一条写入记录的序号，扫描顺序以序号为准。
	NextSeq uint64 `json:"nextSeq"`
	// SchemaVersion 单调递增的当前 schema 版本。
	SchemaVersion int `json:"schemaVersion"`
	// Schemas 是全部历史版本，初始版本号从 1 开始，用于旧段数据的对齐与校验。
	Schemas map[int]*Schema `json:"schemas"`
	// Segments 包含全部已知段（含已逻辑删除、待物理删除的段）。
	Segments []*SegmentMeta `json:"segments"`
}

// clone 深拷贝 MANIFEST，保证更新在提交前不影响扫描方持有的快照。
func (m *Manifest) clone() *Manifest {
	cp := &Manifest{
		NextSegmentID: m.NextSegmentID,
		NextSeq:       m.NextSeq,
		SchemaVersion: m.SchemaVersion,
		Schemas:       make(map[int]*Schema, len(m.Schemas)),
		Segments:      make([]*SegmentMeta, len(m.Segments)),
	}
	for v, s := range m.Schemas {
		sc := *s
		sc.Columns = append([]ColumnDef(nil), s.Columns...)
		cp.Schemas[v] = &sc
	}
	for i, sm := range m.Segments {
		c := *sm
		cp.Segments[i] = &c
	}
	return cp
}

// CurrentSchema 返回当前版本 schema。
func (m *Manifest) CurrentSchema() *Schema {
	if s, ok := m.Schemas[m.SchemaVersion]; ok {
		return s
	}
	return nil
}

// LiveSegments 返回未删除段，按 (MinSeq, ID) 升序排列。
func (m *Manifest) LiveSegments() []*SegmentMeta {
	out := make([]*SegmentMeta, 0, len(m.Segments))
	for _, sm := range m.Segments {
		if !sm.Deleted {
			out = append(out, sm)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MinSeq != out[j].MinSeq {
			return out[i].MinSeq < out[j].MinSeq
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// segmentByID 按 ID 查找段。
func (m *Manifest) segmentByID(id uint64) *SegmentMeta {
	for _, sm := range m.Segments {
		if sm.ID == id {
			return sm
		}
	}
	return nil
}

// manifestStore 负责 MANIFEST 文件的原子读写。
type manifestStore struct {
	path    string // MANIFEST 正式文件路径
	dir     string
	tmpName string
}

func newManifestStore(dir string) *manifestStore {
	return &manifestStore{
		path:    filepath.Join(dir, manifestName),
		dir:     dir,
		tmpName: filepath.Join(dir, manifestName+tmpSuffix),
	}
}

// load 读取 MANIFEST；文件不存在时返回 (nil,nil)，由调用方决定是否初始化。
func (ms *manifestStore) load() (*Manifest, error) {
	data, err := os.ReadFile(ms.path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("columnar: corrupt manifest: %w", err)
	}
	if m.Schemas == nil {
		return nil, errors.New("columnar: corrupt manifest: missing schemas")
	}
	return &m, nil
}

// store 原子写入 MANIFEST（写临时文件、fsync、rename、fsync 目录）。
func (ms *manifestStore) store(m *Manifest) error {
	data, err := marshalManifest(m)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(ms.tmpName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(ms.tmpName)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(ms.tmpName)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(ms.tmpName)
		return err
	}
	if err := os.Rename(ms.tmpName, ms.path); err != nil {
		_ = os.Remove(ms.tmpName)
		return err
	}
	return fsyncDir(ms.dir)
}

// marshalManifest 序列化 JSON（带缩进，便于人工排查）。
func marshalManifest(m *Manifest) ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}
