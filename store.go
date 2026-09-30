package columnstore

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Options 控制存储行为。
type Options struct {
	// MaxSegments 可见段数量达到该阈值时触发后台合并（>=2 生效）。
	MaxSegments int
	// Logger 打印关键状态日志（写入、演进、合并、可见性）；nil 时丢弃。
	Logger *log.Logger
}

func (o *Options) withDefaults() *Options {
	if o == nil {
		o = &Options{}
	}
	if o.MaxSegments < 2 {
		o.MaxSegments = 4
	}
	if o.Logger == nil {
		o.Logger = log.New(io.Discard, "", 0)
	}
	return o
}

// SetCompactionFailureHook 仅供测试：在合并"新段写完、清单替换前"注入失败，
// 以验证回滚路径。传 nil 清除。
func (s *Store) SetCompactionFailureHook(fn func(stage string) error) {
	s.mu.Lock()
	s.failBeforeSwap = fn
	s.mu.Unlock()
}

// Store 是本地列式存储句柄。
//
// 可见性模型：MANIFEST.json 是唯一的可见性事实来源；段文件只有完整关闭
// 后才会随一次原子的 manifest 替换发布。读写用 sync.RWMutex 保护：
// 读路径在 RLock 期间持有可见段快照，合并在 Lock 下做"写新段→原子换清单→
// 删旧段"，因此读永远不会看到半写段或重复/丢失记录。
type Store struct {
	dir  string
	opts *Options
	mu   sync.RWMutex
	// mergeLock 串行化"段 ID 预留 + 段发布/替换清单"的关键区。
	// Write 与 compaction 都持有它；ReadColumns 不获取它（仅 RLock 快照），
	// 因此读不被合并/写入互斥阻塞。
	mergeLock sync.Mutex
	bgWG      sync.WaitGroup // 后台合并 goroutine，Close 必须等它们收敛
	closing   bool           // Close 已开始等待：新后台合并不再启动
	closed    bool           // 完全关闭：写/合并均拒绝
	mani      *manifest
	comping   bool // 是否有合并在进行（受 mu 保护）

	// 段文件生命周期（segMu 保护）：段被合并从清单移除后不能立即删除，
	// 必须等所有在途扫描（ReadColumns 已 pin）结束，否则读者会在扫描中途
	// 遇到文件消失。pins 记录每个段文件当前被多少个读者持有。
	segMu           sync.Mutex
	pins            map[string]int
	pendingDeletion map[string]bool

	// 测试钩子：若非 nil，合并在新段写完、清单替换前被调用；
	// 返回错误则触发合并失败回滚。
	failBeforeSwap func(stage string) error
}

// Open 打开（或初始化）dir 下的数据集；initial 仅在新建数据集时需要。
func Open(dir string, initial *Schema, opts *Options) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	opts = opts.withDefaults()
	s := &Store{dir: dir, opts: opts, pins: map[string]int{}, pendingDeletion: map[string]bool{}}
	// 清理上次进程崩溃可能留下的临时合并段（它们从未被清单引用，对读不可见）。
	if entries, err := os.ReadDir(s.tmpDir()); err == nil {
		for _, e := range entries {
			_ = os.Remove(filepath.Join(s.tmpDir(), e.Name()))
		}
	}

	m, existed, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	if !existed {
		if initial == nil {
			return nil, fmt.Errorf("%w: new dataset requires an initial schema", ErrIncompatibleSchema)
		}
		m = &manifest{Generation: 0, NextSegmentID: 1, Schema: initial.clone()}
		if err := saveManifest(dir, m); err != nil {
			return nil, fmt.Errorf("save initial manifest: %w", err)
		}
		s.opts.Logger.Printf("store: initialized at %s schema v%d cols=%d",
			dir, m.Schema.Version, len(m.Schema.ActiveColumns()))
	} else {
		s.opts.Logger.Printf("store: opened %s gen=%d schema v%d visible_segments=%d rows=%d",
			dir, m.Generation, m.Schema.Version, len(m.Segments), totalRows(m))
	}
	s.mani = m
	return s, nil
}

func (s *Store) logf(format string, args ...any) {
	s.opts.Logger.Printf(format, args...)
}

// Close 关闭存储。
//
// 语义：先置 closing，阻止"已被调度但还在 mergeLock 排队"的后台合并开工
// （它们尚未产生任何临时段）；已在运行的合并继续完成发布。等待全部后台
// goroutine 收敛后再置 closed。因此 Close 返回后不会留下未引用的临时段。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.bgWG.Wait()
		return nil
	}
	s.closing = true // 排队中的后台合并拿到锁后直接退出
	s.mu.Unlock()

	s.bgWG.Wait() // 等待已在运行（通过了 closing 检查）的合并完成

	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.logf("store: closed; final visible_segments=%d rows=%d", len(s.mani.Segments), totalRows(s.mani))
	return nil
}

func (s *Store) checkOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closing || s.closed {
		return ErrClosed
	}
	return nil
}

func totalRows(m *manifest) int {
	n := 0
	for _, seg := range m.Segments {
		n += seg.Rows
	}
	return n
}

// Write 校验并按批写入一个新段。任何记录校验失败都会整批拒绝，不产生部分写入。
func (s *Store) Write(b Batch) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(b) == 0 {
		return nil
	}
	// 纯 CPU 校验在读锁下完成（读锁可并发）。
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return ErrClosed
	}
	schema := s.mani.Schema
	s.mu.RUnlock()
	for i, rec := range b {
		if err := validateRecord(schema, rec); err != nil {
			s.logf("write: REJECT batch_size=%d at row=%d err=%v", len(b), i, err)
			return fmt.Errorf("write row %d: %w", i, err)
		}
	}

	// 关键区：预留 ID、写文件、发布清单。与 compaction 的关键区互斥，
	// 保证段 ID 不冲突、清单替换不交错。
	s.mergeLock.Lock()
	defer s.mergeLock.Unlock()
	s.mu.Lock()
	// 可能在排队等待 mergeLock 期间 Close 已发生：必须重新检查，
	// 否则会在关闭后发布段并启动无人等待的后台合并。
	if s.closing || s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	id := s.mani.NextSegmentID
	s.mani.NextSegmentID = id + 1
	s.mu.Unlock()

	path := s.segmentPath(id)
	w, err := newSegmentWriter(path, schema)
	if err != nil {
		s.releaseID(id)
		return fmt.Errorf("create segment: %w", err)
	}
	if err := w.WriteBatch(b); err != nil {
		w.Close()
		os.Remove(path)
		s.releaseID(id)
		return fmt.Errorf("encode segment: %w", err)
	}
	if err := w.Close(); err != nil {
		os.Remove(path)
		s.releaseID(id)
		return fmt.Errorf("finish segment: %w", err)
	}

	s.mu.Lock()
	newM := s.mani.cloneForUpdate()
	if id >= newM.NextSegmentID {
		newM.NextSegmentID = id + 1
	}
	newM.Segments = append(newM.Segments, segmentMeta{ID: id, Path: path, Rows: len(b)})
	if err := s.publish(newM); err != nil {
		s.mu.Unlock()
		os.Remove(path)
		return err
	}
	s.logf("write: OK segment=%s rows=%d; visible_segments=%d total_rows=%d",
		filepath.Base(path), len(b), len(newM.Segments), totalRows(newM))
	// Add 必须在启动 goroutine 之前完成，否则 Close 的 Wait 可能在计数器
	// 尚未增加时就返回，导致后台合并变成无人等待的孤儿。
	shouldCompact := len(newM.Segments) >= s.opts.MaxSegments && !s.closing
	if shouldCompact {
		s.bgWG.Add(1)
	}
	s.mu.Unlock()

	if shouldCompact {
		go func() {
			defer s.bgWG.Done()
			s.autoCompact()
		}()
	}
	return nil
}

// releaseID 回退预分配的段 ID（仅当 ID 仍是当前最大值时）。
func (s *Store) releaseID(id int64) {
	s.mu.Lock()
	if s.mani.NextSegmentID == id+1 {
		s.mani.NextSegmentID = id
	}
	s.mu.Unlock()
}

// cloneForUpdate 复制清单，使就地修改不会影响持有旧快照的读者（此处锁内修改，
// 仍复制 Segments 以保证清晰的发布语义）。
func (m *manifest) cloneForUpdate() *manifest {
	nm := &manifest{
		Generation:    m.Generation,
		NextSegmentID: m.NextSegmentID,
		Schema:        m.Schema,
		Segments:      make([]segmentMeta, len(m.Segments)),
	}
	copy(nm.Segments, m.Segments)
	return nm
}

// publish 原子落盘新清单并替换内存视图。落盘失败时内存视图保持旧状态。
func (s *Store) publish(newM *manifest) error {
	if err := saveManifest(s.dir, newM); err != nil {
		return fmt.Errorf("publish manifest: %w", err)
	}
	s.mani = newM
	return nil
}

func (s *Store) segmentPath(id int64) string {
	return filepath.Join(s.dir, fmt.Sprintf("seg-%010d.cs", id))
}

// tmpDir 存放合并产生的临时段，与数据目录隔离，避免扫描误识别。
func (s *Store) tmpDir() string { return filepath.Join(s.dir, "tmp") }
