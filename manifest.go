package columnstore

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const manifestName = "MANIFEST.json"

// segmentMeta 是清单中对一个段文件的描述。
type segmentMeta struct {
	ID   int64  `json:"id"`
	Path string `json:"path"`
	Rows int    `json:"rows"`
}

// manifest 是数据集的元数据清单：当前 schema + 全部可见段。
// 可见性的唯一事实来源是本文件：未出现在清单中的段文件对读不可见。
type manifest struct {
	Generation    int64         `json:"generation"`
	NextSegmentID int64         `json:"next_segment_id"`
	Schema        *Schema       `json:"schema"`
	Segments      []segmentMeta `json:"segments"`
}

func manifestPath(dir string) string { return filepath.Join(dir, manifestName) }

func loadManifest(dir string) (*manifest, bool, error) {
	data, err := os.ReadFile(manifestPath(dir))
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, false, fmt.Errorf("decode manifest: %w", err)
	}
	if m.Schema == nil {
		return nil, false, fmt.Errorf("manifest missing schema")
	}
	if m.NextSegmentID == 0 {
		m.NextSegmentID = 1
	}
	return &m, true, nil
}

// saveManifest 以"临时文件 + fsync + rename"原子替换清单，
// 读侧永远只能看到替换前或替换后的完整状态。
func saveManifest(dir string, m *manifest) error {
	m.Generation++
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := manifestPath(dir) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, manifestPath(dir)); err != nil {
		return err
	}
	// 目录项变更也需要落盘，保证重启后可见性一致。
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}
