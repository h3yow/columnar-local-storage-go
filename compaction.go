package columnstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// 合并可见性协议：
//  1. 在写锁下抓取"目标段集合（按顺序）+ 目标 schema"，并占用唯一合并位；
//  2. 旧段文件不可变，在锁外把目标段逐个合并成一个新临时段（顺序打开、用完即关），
//     这期间新写入照常进行，新段追加在目标集合之后，互不影响；
//  3. 回到写锁：若 schema 未变化，则以
//          [合并后新段] + [合并期间新追加的段]
//     原子替换清单。记录总数保持不变，旧记录恰好搬入新段一次（不重不丢），
//     新段全部保留；若 schema 在期间变化，则放弃本轮结果回滚（类型对齐无法保证）。
//  4. 任何失败步骤：清单从未指向新段，删除临时段即可，可见数据完全回到合并前。

// autoCompact 由写入后在 goroutine 中触发；已有合并进行时安静退出。
func (s *Store) autoCompact() {
	if err := s.compact(nil, true); err != nil {
		s.logf("compaction: auto finished with err=%v (visible state preserved)", err)
	}
}

// ForceCompaction 同步执行一次合并，便于测试断言。
func (s *Store) ForceCompaction(ctx context.Context) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return s.compact(ctx, false)
}

type compactionPlan struct {
	targets    []segmentMeta // 参与合并的段（顺序即行序）
	schema     *Schema
	nextID     int64 // 预留给合并段的 ID
	schemaVers int
}

func (s *Store) compact(ctx context.Context, auto bool) (err error) {
	// 关键区与 Write 的"预留 ID + 发布"互斥；昂贵的段合并在锁外执行。
	s.mergeLock.Lock()
	defer s.mergeLock.Unlock()

	// 1. 抓取快照并占用合并位。
	s.mu.Lock()
	if s.closing || s.closed {
		// Close 已开始：排队中的合并直接退出，此时还没创建临时段。
		s.mu.Unlock()
		return ErrClosed
	}
	if s.comping {
		s.mu.Unlock()
		if auto {
			return nil
		}
		return ErrCompactionBusy
	}
	if len(s.mani.Segments) < 2 {
		s.mu.Unlock()
		return nil
	}
	s.comping = true
	plan := &compactionPlan{
		targets:    append([]segmentMeta(nil), s.mani.Segments...),
		schema:     s.mani.Schema.clone(),
		nextID:     s.mani.NextSegmentID,
		schemaVers: s.mani.Schema.Version,
	}
	// 预留给合并段的 ID，写入方在此锁外等待，不会拿到相同 ID。
	// 注意：不推进 NextSegmentID，避免最终合并因任何原因不发生时产生 ID 空洞语义混乱；
	// 由于 mergeLock 被持有，也没有任何人能并发分配该 ID。
	s.mu.Unlock()
	clearComping := func() {
		s.mu.Lock()
		s.comping = false
		s.mu.Unlock()
	}

	s.logf("compaction: START targets=%d rows=%d schema v%d newseg_id=%d",
		len(plan.targets), planRows(plan.targets), plan.schemaVers, plan.nextID)

	// 2. 锁外执行昂贵合并（旧段不可变；写方在 mergeLock 上等待，不会并发追加）。
	newSegPath, mergedRows, err := s.mergeSegments(plan)
	if err != nil {
		clearComping()
		s.logf("compaction: FAIL during merge err=%v; rollback -> visible state unchanged (segments=%d rows=%d)",
			err, len(plan.targets), planRows(plan.targets))
		return err
	}

	// 3. 回到写锁做一致性检查与原子替换。
	s.mu.Lock()
	if s.closed {
		// 合并已把临时段写完但存储正在关闭：删除临时段回滚，绝不残留。
		if rmErr := os.Remove(newSegPath); rmErr != nil && !os.IsNotExist(rmErr) {
			s.logf("compaction: WARN remove temp after close failed: %v path=%s", rmErr, newSegPath)
		}
		s.comping = false
		s.mu.Unlock()
		s.logf("compaction: abort after merge because store closed; temp segment removed")
		return ErrClosed
	}
	// mergeLock 保证期间不会有新段发布；schema 演进也走 mu 但可能不持 mergeLock，
	// 因此这里仍需校验 schema 版本。
	if s.mani.Schema.Version != plan.schemaVers {
		_ = os.Remove(newSegPath)
		s.comping = false
		s.mu.Unlock()
		s.logf("compaction: ABORT schema changed during merge (v%d -> v%d); rollback: temp segment removed, state unchanged",
			plan.schemaVers, s.mani.Schema.Version)
		return ErrIncompatibleSchema
	}

	// 校验目标段仍是当前完整可见集合（mergeLock 下不应变化，防御性检查）。
	cur := s.mani.Segments
	if len(cur) != len(plan.targets) {
		_ = os.Remove(newSegPath)
		s.comping = false
		s.mu.Unlock()
		return fmt.Errorf("compaction invariant violated: segment set changed while mergeLock held")
	}
	for i, t := range plan.targets {
		if cur[i].ID != t.ID {
			_ = os.Remove(newSegPath)
			s.comping = false
			s.mu.Unlock()
			return fmt.Errorf("compaction invariant violated: target segment reorder/replace at %d", i)
		}
	}

	// 测试失败注入点：新段已写完、清单尚未替换。
	if s.failBeforeSwap != nil {
		if ferr := s.failBeforeSwap("before_swap"); ferr != nil {
			_ = os.Remove(newSegPath)
			s.comping = false
			s.mu.Unlock()
			s.logf("compaction: INJECTED FAIL before swap err=%v; rollback -> segments=%d rows=%d",
				ferr, len(s.mani.Segments), totalRows(s.mani))
			return ferr
		}
	}

	// 原子地把临时段提升为正式段；清单只引用正式路径，读取方永远不会打开 .compacting。
	finalPath := s.segmentPath(plan.nextID)
	if err := os.Rename(newSegPath, finalPath); err != nil {
		_ = os.Remove(newSegPath)
		s.comping = false
		s.mu.Unlock()
		s.logf("compaction: FAIL rename temp segment err=%v; rollback -> state unchanged", err)
		return fmt.Errorf("publish compacted segment: %w", err)
	}

	replacement := []segmentMeta{{ID: plan.nextID, Path: finalPath, Rows: mergedRows}}
	newM := s.mani.cloneForUpdate()
	if newM.NextSegmentID <= plan.nextID {
		newM.NextSegmentID = plan.nextID + 1
	}
	newM.Segments = replacement
	if err := saveManifest(s.dir, newM); err != nil {
		// 落盘失败：旧清单与全部旧段完好，删除已提升但未发布的新段——纯回滚。
		_ = os.Remove(finalPath)
		s.comping = false
		s.mu.Unlock()
		s.logf("compaction: FAIL manifest publish err=%v; rollback -> state unchanged", err)
		return err
	}
	// 清单已原子切换：此后旧文件对新读者不可见。
	oldPaths := make([]string, len(plan.targets))
	for i, t := range plan.targets {
		oldPaths[i] = t.Path
	}
	s.mani = newM
	s.comping = false
	s.mu.Unlock()

	// 删除旧段：有在途读者则延迟到其 unpin，绝不打断进行中的扫描。
	s.removeSegments(oldPaths)
	s.logf("compaction: OK %d old segments (%d rows) -> %s; visible_segments=1 rows=%d",
		len(oldPaths), mergedRows, filepath.Base(finalPath), totalRows(newM))
	return nil
}

func planRows(segs []segmentMeta) int {
	n := 0
	for _, sg := range segs {
		n += sg.Rows
	}
	return n
}

// mergeSegments 顺序打开旧段（任意时刻最多 1 个旧段 + 1 个新段文件句柄），
// 按行重组并以合并时 schema 写入单个新段。旧段缺列（加列前数据）按缺省值/null 补齐。
func (s *Store) mergeSegments(plan *compactionPlan) (string, int, error) {
	// 临时段放在独立的 tmp/ 目录：任何对数据目录的扫描都不会把它误当作数据段；
	// 发布时用 rename 原子提升到正式路径（tmp 与数据目录同文件系统）。
	if err := os.MkdirAll(s.tmpDir(), 0o755); err != nil {
		return "", 0, fmt.Errorf("create temp dir: %w", err)
	}
	newPath := filepath.Join(s.tmpDir(), fmt.Sprintf("seg-%010d.cs.compacting", plan.nextID))
	sw, err := newSegmentWriter(newPath, plan.schema)
	if err != nil {
		return "", 0, fmt.Errorf("create compacted segment: %w", err)
	}

	total := 0
	for _, meta := range plan.targets {
		if err := s.mergeOne(sw, meta, plan.schema); err != nil {
			sw.Close()
			os.Remove(newPath)
			return "", 0, err
		}
		total += meta.Rows
	}
	if err := sw.Close(); err != nil {
		os.Remove(newPath)
		return "", 0, fmt.Errorf("finish compacted segment: %w", err)
	}
	return newPath, total, nil
}

// mergeOne 读取单个旧段并逐行并入。同一列 ID 在旧段与合并时 schema 间必须类型一致，
// 否则说明发生了不兼容变更，明确报错而不是按错结构读出。
func (s *Store) mergeOne(sw *segmentWriter, meta segmentMeta, curSchema *Schema) error {
	r, err := openSegmentReader(meta.Path)
	if err != nil {
		return fmt.Errorf("open segment for compaction: %w", err)
	}
	defer r.Close()

	oldSchema := r.Schema()
	blocks := map[int32]*Block{}
	active := curSchema.ActiveColumns()
	for _, c := range active {
		oldCol, existed := oldSchema.ColumnByID(c.ID)
		if existed && !oldCol.Deleted {
			if oldCol.Type != c.Type {
				return wrapColumnErr("compact", c.Name,
					fmt.Errorf("%w: column id %d type changed across files (%s -> %s)",
						ErrIncompatibleSchema, c.ID, oldCol.Type, c.Type))
			}
			blk, err := r.ReadBlock(c.ID)
			if err != nil {
				return fmt.Errorf("read block during compaction: %w", err)
			}
			blocks[c.ID] = blk
		}
	}

	rows := r.Rows()
	for _, c := range active {
		if blk, ok := blocks[c.ID]; ok {
			l := typedLen(blk)
			if len(blk.Nulls) != rows || l != rows {
				return fmt.Errorf("internal: block shape mismatch col=%s rows=%d nulls=%d typed=%d path=%s",
					c.Name, rows, len(blk.Nulls), l, meta.Path)
			}
		}
	}
	batch := make(Batch, rows)
	for row := 0; row < rows; row++ {
		rec := Record{}
		for _, c := range active {
			blk, ok := blocks[c.ID]
			if !ok {
				if c.DefaultValue != nil {
					rec[c.Name] = c.DefaultValue
				} else if c.Nullable {
					rec[c.Name] = nil
				} else {
					return wrapColumnErr("compact", c.Name,
						fmt.Errorf("%w: old rows have no value and no default", ErrIncompatibleSchema))
				}
				continue
			}
			if blk.Nulls[row] {
				rec[c.Name] = nil
				continue
			}
			switch c.Type {
			case TypeInt64:
				rec[c.Name] = blk.Ints[row]
			case TypeFloat64:
				rec[c.Name] = blk.Floats[row]
			case TypeString:
				rec[c.Name] = blk.Strings[row]
			case TypeBool:
				rec[c.Name] = blk.Bools[row]
			case TypeBytes:
				rec[c.Name] = blk.Bytes[row]
			}
		}
		batch[row] = rec
	}
	return sw.WriteBatch(batch)
}
