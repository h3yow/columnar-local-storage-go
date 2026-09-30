package columnar

// 本文件实现数据集的顶层句柄 Store：批次写入、schema 演进、扫描读回，
// 并通过互斥锁与读写屏障协调后台合并压缩与并发读写。
//
// 锁的层级（固定加锁顺序，避免死锁）：
//
//	mu      : 保护 MANIFEST 发布与 append/compact 的元数据更新；
//	scanMu  : 读写屏障。扫描全程持有 RLock；物理删除旧段文件需要 Lock（等待在途扫描结束）。
//
// 加锁顺序恒为 mu -> scanMu；扫描路径只取 scanMu.RLock，绝不取 mu；append 只取 mu。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Options 控制 Store 的行为。
type Options struct {
	// FlushThreshold 内存中累积多少行就滚动生成一个新段（默认 1000）。
	FlushThreshold int
	// CompactionThreshold 存活段数达到该值时允许后台/手动合并（默认 4）。
	CompactionThreshold int
	// MergeMaxRows 单个合并输出段最多容纳的行数（默认 100000）。
	MergeMaxRows int
	// DisableAutoCompaction 关闭后台定时合并（仍可手动 Compact）。
	DisableAutoCompaction bool
}

func (o *Options) withDefaults() {
	if o.FlushThreshold <= 0 {
		o.FlushThreshold = 1000
	}
	if o.CompactionThreshold <= 0 {
		o.CompactionThreshold = 4
	}
	if o.MergeMaxRows <= 0 {
		o.MergeMaxRows = 100000
	}
}

const compactionInterval = 50 * time.Millisecond

// Store 是一个本地列式数据集句柄。
type Store struct {
	dir    string
	opt    Options
	mu     sync.Mutex   // 见文件头注释
	scanMu sync.RWMutex // 扫描 RLock；物理删除段文件 Lock
	ms     *manifestStore
	lock   *fileLock

	manifest atomic.Pointer[Manifest]

	pending []Record // 已校验、尚未落段的行

	closed    atomic.Bool
	stopBg    chan struct{}
	bgDone    chan struct{}
	compactMu sync.Mutex // 同一时刻只允许一个合并

	// failpoints（仅测试注入用）
	fp struct {
		// failBeforeManifestSwap: 合并在新段写出前/切换 MANIFEST 之前失败
		failBeforeManifestSwap atomic.Bool
		// failAfterRenameBeforeManifest: 输出段已 rename 成正式名、
		// 但 MANIFEST 尚未发布时失败（更强的回滚场景）
		failAfterRenameBeforeManifest atomic.Bool
	}

	// 打开段文件数的观测（测试断言用）
	openNow atomic.Int64
	openMax atomic.Int64
}

const (
	manifestName = "MANIFEST"
	lockName     = "lock"
	segPrefix    = "seg-"
	segSuffix    = ".col"
	tmpSuffix    = ".tmp"
)

// Create 在 dir 创建一个新数据集（dir 不得已存在 MANIFEST）。
func Create(dir string, schema Schema, opt Options) (*Store, error) {
	return openOrInit(dir, &schema, opt)
}

// Open 打开已有数据集，恢复 MANIFEST，并清理上次崩溃遗留的临时/孤儿文件。
func Open(dir string, opt Options) (*Store, error) {
	return openOrInit(dir, nil, opt)
}

func openOrInit(dir string, init *Schema, opt Options) (_ *Store, retErr error) {
	opt.withDefaults()
	if err := ensureDir(dir); err != nil {
		return nil, err
	}
	lk, err := acquireFileLock(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = lk.unlock()
		}
	}()

	ms := newManifestStore(dir)
	m, err := ms.load()
	if err != nil {
		return nil, err
	}
	if init != nil {
		if m != nil {
			return nil, fmt.Errorf("columnar: dataset already exists in %s", dir)
		}
		s := init
		s.Version = 1
		if err := validateSchemaShape(s); err != nil {
			return nil, err
		}
		m = &Manifest{
			NextSegmentID: 1,
			Schemas:       map[int]*Schema{1: cloneSchema(s)},
			SchemaVersion: 1,
		}
		if err := ms.store(m); err != nil {
			return nil, err
		}
	} else {
		if m == nil {
			return nil, fmt.Errorf("columnar: no dataset found in %s", dir)
		}
		m, err = reconcileOnOpen(dir, ms, m)
		if err != nil {
			return nil, err
		}
	}

	s := &Store{
		dir:    dir,
		opt:    opt,
		ms:     ms,
		lock:   lk,
		stopBg: make(chan struct{}),
		bgDone: make(chan struct{}),
	}
	s.manifest.Store(m)
	if !opt.DisableAutoCompaction {
		s.startBackgroundCompactor()
	} else {
		close(s.bgDone)
	}
	return s, nil
}

// reconcileOnOpen 清理崩溃残留：*.tmp、孤儿段文件；删除已逻辑删除段的物理文件；
// 从 MANIFEST 中移除这些条目并持久化。
func reconcileOnOpen(dir string, ms *manifestStore, m *Manifest) (*Manifest, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	referenced := make(map[string]*SegmentMeta, len(m.Segments))
	for _, sm := range m.Segments {
		referenced[sm.FileName] = sm
	}
	changed := false
	known := make(map[string]bool, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(name, tmpSuffix) {
			_ = os.Remove(filepath.Join(dir, name))
			changed = true
			continue
		}
		if !strings.HasPrefix(name, segPrefix) || !strings.HasSuffix(name, segSuffix) {
			continue
		}
		known[name] = true
		sm := referenced[name]
		switch {
		case sm == nil:
			// 段文件已 rename 但 MANIFEST 未更新就崩溃：未被引用，删除。
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			changed = true
		case sm.Deleted:
			if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, err
			}
			changed = true
		}
	}
	// 存活段文件丢失属于损坏；Deleted 条目直接清理。
	kept := m.Segments[:0]
	for _, sm := range m.Segments {
		if sm.Deleted || !known[sm.FileName] {
			if !sm.Deleted {
				return nil, fmt.Errorf("columnar: corrupt dataset: live segment %s is missing", sm.FileName)
			}
			changed = true
			continue
		}
		kept = append(kept, sm)
	}
	if changed {
		m.Segments = kept
		if err := ms.store(m); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Dir 返回数据集目录。
func (s *Store) Dir() string { return s.dir }

// Close 停止后台合并、刷新内存缓冲并释放文件锁。
func (s *Store) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopBg)
	<-s.bgDone
	s.mu.Lock()
	flushErr := s.flushLocked()
	m := s.manifest.Load()
	persistErr := s.ms.store(m)
	s.mu.Unlock()
	if flushErr != nil {
		return flushErr
	}
	if persistErr != nil {
		return persistErr
	}
	return s.lock.unlock()
}

// CurrentSchema 返回当前 schema 的深拷贝快照。
func (s *Store) CurrentSchema() Schema {
	m := s.manifest.Load()
	return *cloneSchema(m.CurrentSchema())
}

// EvolveSchema 以新 schema 进行演进；不兼容时返回 *CompatibilityError 且不改变任何状态。
// 新版本号由 Store 分配；调用方只需给出新的列定义集合。
func (s *Store) EvolveSchema(schema Schema) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	if err := validateSchemaShape(&schema); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.manifest.Load()
	cur := m.CurrentSchema()
	next := cloneSchema(&schema)
	next.Version = cur.Version + 1
	if err := next.CheckCompatible(cur); err != nil {
		return err // *CompatibilityError，状态完全未变
	}
	// 内存中已确认追加、但尚未落盘的行是按旧 schema 校验的。
	// 本组件仅允许“新增可空列”的演进，因此把这些行按新 schema 物化
	// （新增列补 null 缺省值）即可保持一致；先验证再发布，失败则状态不变。
	newPending := make([]Record, len(s.pending))
	for i, r := range s.pending {
		mr := r.Materialize(next)
		if err := next.ValidateRecord(&mr); err != nil {
			return err
		}
		newPending[i] = mr
	}
	nm := m.clone()
	nm.SchemaVersion = next.Version
	nm.Schemas[next.Version] = next
	if err := s.ms.store(nm); err != nil {
		return err
	}
	s.manifest.Store(nm)
	s.pending = newPending
	return nil
}

// Append 追加一个批次的记录。逐条按当前 schema 校验：
// 列缺失 / null 写入非空列 / 类型不一致 / 未知列均返回带原因的 *SchemaError，整批不生效。
func (s *Store) Append(ctx context.Context, rows []Record) error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	if len(rows) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.manifest.Load()
	schema := m.CurrentSchema()
	// 先整批校验：任何一条不合法，整批拒绝（不部分写入）。
	for i := range rows {
		if err := schema.ValidateRecord(&rows[i]); err != nil {
			return err
		}
	}
	s.pending = append(s.pending, rows...)
	// 达到滚动阈值就切成段，保证单个段文件不会无限增长。
	for len(s.pending) >= s.opt.FlushThreshold {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.flushChunkLocked(s.opt.FlushThreshold); err != nil {
			return err
		}
	}
	return nil
}

// flushChunkLocked 把 pending 的前 n 行写成一个新段并原子发布 MANIFEST。
func (s *Store) flushChunkLocked(n int) error {
	m := s.manifest.Load()
	schema := m.CurrentSchema()
	id := m.NextSegmentID
	sw, err := CreateSegment(s.dir, id, schema)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if err := sw.Append(s.pending[i]); err != nil {
			_ = sw.Abort()
			return err
		}
	}
	baseSeq := m.NextSeq
	meta, err := sw.Commit(baseSeq)
	if err != nil {
		_ = sw.Abort()
		return err
	}
	nm := m.clone()
	nm.NextSegmentID = id + 1
	nm.NextSeq = baseSeq + uint64(n)
	nm.Segments = append(nm.Segments, &meta)
	if err := s.ms.store(nm); err != nil {
		// MANIFEST 未发布：段文件是孤儿，Open 时会清理；本次写入失败。
		_ = os.Remove(filepath.Join(s.dir, meta.FileName))
		return err
	}
	s.manifest.Store(nm)
	s.pending = s.pending[n:]
	return nil
}

// flushLocked 把剩余 pending 行刷盘（Close 路径）。
func (s *Store) flushLocked() error {
	for len(s.pending) > 0 {
		n := len(s.pending)
		if n > s.opt.FlushThreshold {
			n = s.opt.FlushThreshold
		}
		if err := s.flushChunkLocked(n); err != nil {
			return err
		}
	}
	return nil
}

// Scan 打开一个一致性快照扫描器。
// 快照在同一把 mu 下取得“MANIFEST + 内存待落盘行”，随后持有 scanMu.RLock：
//   - 合并期间开始的扫描要么看到旧段集合、要么看到新段集合，绝不会看到半切换状态；
//   - 已 Append 但尚未滚动成段的行也立即可见（扫描的段数据之后按序输出）；
//   - 任何时刻只打开 1 个段文件（流式逐段），与小文件数量无关。
//
// 投影以当前最新 schema 为准：老段缺失的新增列按 null 补出。
func (s *Store) Scan(ctx context.Context) (*Scanner, error) {
	if s.closed.Load() {
		return nil, ErrStoreClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 先持读屏障，再在屏障内取“MANIFEST + pending”快照：
	// 这样快照引用的段文件在整个扫描期间绝不会被合并的物理删除阶段移除。
	// 不会死锁：合并的 mu 临界区与 scanMu 临界区互不嵌套，且这里持 RLock 期间
	// 等待 mu 时，持 mu 者并不等待 scanMu。
	s.scanMu.RLock()
	s.mu.Lock()
	m := s.manifest.Load()
	pending := make([]Record, len(s.pending))
	copy(pending, s.pending)
	s.mu.Unlock()

	sc := &Scanner{
		store:   s,
		schema:  cloneSchema(m.CurrentSchema()),
		live:    m.LiveSegments(),
		pending: pending,
	}
	return sc, nil
}

// Compact 同步触发一次合并；当前没有达到合并条件的内容时返回 (false,nil)。
func (s *Store) Compact(ctx context.Context) (bool, error) {
	if s.closed.Load() {
		return false, ErrStoreClosed
	}
	return s.runCompaction(ctx)
}

// Flush 把内存中尚未落盘的行写成段文件（不做合并）。
func (s *Store) Flush() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

// Stats 是数据集当前可见状态的观测信息。
type Stats struct {
	LiveSegments    int
	DeletedSegments int
	Rows            uint64
	PendingRows     int
	OpenNow         int64
	OpenPeak        int64
}

// Stats 返回当前可见状态快照。
func (s *Store) Stats() Stats {
	m := s.manifest.Load()
	st := Stats{PendingRows: len(s.pending), OpenNow: s.openNow.Load(), OpenPeak: s.openMax.Load()}
	var rows uint64
	for _, sm := range m.Segments {
		if sm.Deleted {
			st.DeletedSegments++
		} else {
			st.LiveSegments++
			rows += uint64(sm.RowCount)
		}
	}
	st.Rows = rows + uint64(st.PendingRows)
	return st
}

// trackOpen / trackClose 维护打开段文件数的当前值与历史峰值。
func (s *Store) trackOpen() {
	n := s.openNow.Add(1)
	for {
		peak := s.openMax.Load()
		if n <= peak || s.openMax.CompareAndSwap(peak, n) {
			break
		}
	}
}

func (s *Store) trackClose() { s.openNow.Add(-1) }

// Scanner 是一次扫描的句柄。
type Scanner struct {
	store   *Store
	schema  *Schema
	live    []*SegmentMeta
	pending []Record // 快照时刻尚未落盘的行，排在段数据之后
	idx     int
	pidx    int
	cur     *SegmentReader
	done    bool
}

// Next 推进到下一行，将当前最新 schema 投影后的值写入 *r；io.EOF 表示扫描结束。
func (sc *Scanner) Next(r *Record) error {
	if sc.done {
		return io.EOF
	}
	for {
		if sc.idx < len(sc.live) {
			if sc.cur == nil {
				sr, err := sc.store.openSegmentForRead(sc.live[sc.idx])
				if err != nil {
					return err
				}
				sc.cur = sr
			}
			raw := make(Record, len(sc.schema.Columns))
			err := sc.cur.Next(raw)
			if errors.Is(err, io.EOF) {
				_ = sc.closeCurrent()
				sc.idx++
				continue
			}
			if err != nil {
				return err
			}
			// 以最新 schema 投影：旧段没有的新增列补 null；杜绝列错位。
			*r = raw.Materialize(sc.schema)
			return nil
		}
		// 段数据读完后，输出快照时刻仍在内存中的待落盘行。
		if sc.pidx < len(sc.pending) {
			*r = sc.pending[sc.pidx].Materialize(sc.schema)
			sc.pidx++
			return nil
		}
		sc.done = true
		return io.EOF
	}
}

func (sc *Scanner) closeCurrent() error {
	if sc.cur == nil {
		return nil
	}
	err := sc.cur.Close()
	sc.cur = nil
	sc.store.trackClose()
	return err
}

// Close 结束扫描：释放段文件句柄并释放读写屏障。
func (sc *Scanner) Close() error {
	err := sc.closeCurrent()
	sc.done = true
	sc.store.scanMu.RUnlock()
	return err
}

// openSegmentForRead 打开一个段文件并计入观测指标。调用方持有 scanMu.RLock。
func (s *Store) openSegmentForRead(sm *SegmentMeta) (*SegmentReader, error) {
	s.trackOpen()
	sr, err := OpenSegment(s.dir, sm.FileName)
	if err != nil {
		s.trackClose()
		return nil, err
	}
	return sr, nil
}

// ---- 辅助 --------------------------------------------------------------------

func ensureDir(dir string) error { return os.MkdirAll(filepath.Clean(dir), 0o755) }

func cloneSchema(s *Schema) *Schema {
	cp := *s
	cp.Columns = append([]ColumnDef(nil), s.Columns...)
	return &cp
}

// validateSchemaShape 校验 schema 自身形状：ID 非空且唯一、类型合法、名字非空。
func validateSchemaShape(s *Schema) error {
	seen := make(map[ColumnID]struct{}, len(s.Columns))
	for _, c := range s.Columns {
		if c.ID == "" {
			return errors.New("columnar: column id must not be empty")
		}
		if c.Name == "" {
			return fmt.Errorf("columnar: column %q name must not be empty", c.ID)
		}
		if c.Type <= TypeInvalid || c.Type > TypeBool {
			return fmt.Errorf("columnar: column %q has invalid type", c.ID)
		}
		if _, dup := seen[c.ID]; dup {
			return fmt.Errorf("columnar: duplicate column id %q", c.ID)
		}
		seen[c.ID] = struct{}{}
	}
	return nil
}

// sortSegmentsBySeq 按 (MinSeq,ID) 稳定排序。
func sortSegmentsBySeq(in []*SegmentMeta) {
	sort.SliceStable(in, func(i, j int) bool {
		if in[i].MinSeq != in[j].MinSeq {
			return in[i].MinSeq < in[j].MinSeq
		}
		return in[i].ID < in[j].ID
	})
}
