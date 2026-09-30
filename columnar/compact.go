package columnar

// 本文件实现后台合并压缩。
//
// 可见性与失败回滚语义：
//  1. 选取全部存活段（达到阈值时），流式合并为一个或多个新段；
//     合并读采用逐段打开（任何时刻只有 1 个输入段 FD + 1 个输出段 FD）。
//  2. 所有新段先 Seal 到 comp-*.tmp 并 fsync，然后一次 rename 成正式段文件，
//     再原子发布 MANIFEST（新段 live + 旧段 deleted）。
//     发布之前的任何失败（含测试注入的 failpoint）都不触碰 MANIFEST，
//     已写出的新段文件被删除，可见数据与合并前完全一致。
//  3. MANIFEST 发布后，在 scanMu 写屏障下物理删除旧段文件：
//     必须等待所有在途扫描结束，因此扫描不会遇到文件消失，也不会读到重复或丢失的行。
//  4. 若物理删除阶段失败（文件系统错误），数据仍然正确（MANIFEST 已标记 deleted），
//     下次 Open 时由 reconcileOnOpen 兜底清理。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// startBackgroundCompactor 启动后台定时合并 goroutine。
func (s *Store) startBackgroundCompactor() {
	go func() {
		defer close(s.bgDone)
		t := time.NewTicker(compactionInterval)
		defer t.Stop()
		for {
			select {
			case <-s.stopBg:
				return
			case <-t.C:
				if s.closed.Load() {
					return
				}
				_, _ = s.runCompaction(context.Background())
			}
		}
	}()
}

// pickCompactionSegments 选择待合并段：全部存活段；数量 < 阈值时返回 nil。
func pickCompactionSegments(m *Manifest, threshold int) []*SegmentMeta {
	live := m.LiveSegments()
	if len(live) < threshold {
		return nil
	}
	return live
}

// runCompaction 执行一次完整合并。compactMu 串行化所有合并；返回 (didWork,err)。
func (s *Store) runCompaction(ctx context.Context) (bool, error) {
	// 同一数据集同一时刻只允许一个合并（后台与手动互斥）。
	if !s.compactMu.TryLock() {
		return false, nil
	}
	defer s.compactMu.Unlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}

	// 阶段 0：确定输入集合。
	s.mu.Lock()
	m := s.manifest.Load()
	picked := pickCompactionSegments(m, s.opt.CompactionThreshold)
	s.mu.Unlock()
	if picked == nil {
		return false, nil
	}
	pickedIDs := make(map[uint64]struct{}, len(picked))
	for _, sm := range picked {
		pickedIDs[sm.ID] = struct{}{}
	}

	// 阶段 1：流式合并，输出段先 Seal 为 comp-*.tmp（MANIFEST 完全不变）。
	sealed, err := s.mergeSegments(ctx, picked, m)
	if err != nil {
		abortOutputs(sealed)
		return false, err
	}

	// 测试注入：在 MANIFEST 切换前强制失败 -> 必须回滚到合并前可见状态。
	if s.fp.failBeforeManifestSwap.Load() {
		abortOutputs(sealed)
		return false, &CompactionError{
			Reason: ReasonWriteFailed,
			Detail: "injected failure before manifest swap",
		}
	}

	// 阶段 2：在 mu 下基于最新 MANIFEST 分配正式段 ID，rename 输出段，并原子发布。
	// 合并期间可能有并发 append 产生新段；这些段不在 picked 集合中，保持 live。
	s.mu.Lock()
	cur := s.manifest.Load()
	for _, sm := range cur.Segments {
		if _, ok := pickedIDs[sm.ID]; ok && sm.Deleted {
			// picked 段已被别的合并处理（compactMu 已串行化，正常不会发生），放弃本轮。
			s.mu.Unlock()
			abortOutputs(sealed)
			return false, nil
		}
	}

	baseID := cur.NextSegmentID
	published := make([]SegmentMeta, 0, len(sealed))
	for i, out := range sealed {
		id := baseID + uint64(i)
		finalName := segFileName(id)
		meta, perr := out.w.Publish(id, finalName, out.minSeq)
		if perr != nil {
			// rename 失败：已改名的需要清理，未改名的保留 tmp，随后统一 abort。
			for _, pm := range published {
				_ = os.Remove(filepath.Join(s.dir, pm.FileName))
			}
			abortOutputs(sealed[i:])
			s.mu.Unlock()
			return false, perr
		}
		published = append(published, meta)
	}

	// 测试注入：输出段已 rename 成正式名、MANIFEST 发布前强制失败。
	// 必须删除这些尚未发布的新段，可见数据回到合并前（旧段保持 live）。
	if s.fp.failAfterRenameBeforeManifest.Load() {
		s.mu.Unlock()
		for _, pm := range published {
			_ = os.Remove(filepath.Join(s.dir, pm.FileName))
		}
		return false, &CompactionError{
			Reason: ReasonWriteFailed,
			Detail: "injected failure after rename, before manifest publish",
		}
	}

	nm := cur.clone()
	for i := range nm.Segments {
		if _, ok := pickedIDs[nm.Segments[i].ID]; ok {
			nm.Segments[i].Deleted = true
		}
	}
	nm.NextSegmentID = baseID + uint64(len(published))
	for i := range published {
		pm := published[i]
		nm.Segments = append(nm.Segments, &pm)
	}
	if err := s.ms.store(nm); err != nil {
		// MANIFEST 未发布：新段尚未可见，清理之，旧数据完整。
		s.mu.Unlock()
		for _, pm := range published {
			_ = os.Remove(filepath.Join(s.dir, pm.FileName))
		}
		abortOutputs(sealed[len(published):])
		return false, err
	}
	s.manifest.Store(nm)
	s.mu.Unlock()

	// 阶段 3：物理删除旧段。获取写屏障等待所有在途扫描结束：
	// 扫描打开的 live 集合来自切换前或切换后快照，切换前快照引用旧文件，
	// 因此必须等它们都 Close 后才能删除。
	s.scanMu.Lock()
	var delErr error
	for _, sm := range picked {
		if err := os.Remove(filepath.Join(s.dir, sm.FileName)); err != nil &&
			!errors.Is(err, os.ErrNotExist) {
			delErr = fmt.Errorf("columnar: remove compacted segment %s: %w", sm.FileName, err)
		}
	}
	s.scanMu.Unlock()
	if delErr != nil {
		// 数据仍然正确（旧段已标 deleted，扫描不可见）；下次 Open 兜底清理。
		return true, delErr
	}

	// 阶段 4：从 MANIFEST 移除已删除条目。这一步失败不影响正确性（Open 时再清）。
	s.mu.Lock()
	cm := s.manifest.Load()
	kept := make([]*SegmentMeta, 0, len(cm.Segments))
	for _, sm := range cm.Segments {
		if _, drop := pickedIDs[sm.ID]; !drop {
			kept = append(kept, sm)
		}
	}
	cm2 := cm.clone()
	cm2.Segments = kept
	if err := s.ms.store(cm2); err == nil {
		s.manifest.Store(cm2)
	}
	s.mu.Unlock()

	return true, nil
}

// sealedOutput 是已 Seal、尚未发布的合并输出。
type sealedOutput struct {
	w      *SegmentWriter
	minSeq uint64
}

// abortOutputs 清理已 Seal（或尚未 Seal）的输出临时文件。
func abortOutputs(ws []*sealedOutput) {
	for _, o := range ws {
		if o == nil || o.w == nil {
			continue
		}
		_ = o.w.Abort()
	}
}

// mergeSegments 将 picked 段流式合并成输出段并 Seal。
// 任何时刻打开的文件数：1 个输入段 + 1 个输出段（<= 2）。
// 行数按 MergeMaxRows 分块；输出段序号区间与输入段覆盖的全局区间完全一致。
func (s *Store) mergeSegments(ctx context.Context, picked []*SegmentMeta, m *Manifest) ([]*sealedOutput, error) {
	currentSchema := m.CurrentSchema()
	baseSeq := picked[0].MinSeq
	var emitted uint64 // 已写入输出的总行数

	var outputs []*sealedOutput
	var cur *SegmentWriter
	curRows := 0

	closeOutput := func() error {
		if cur == nil {
			return nil
		}
		if curRows == 0 {
			_ = cur.Abort()
			cur = nil
			return nil
		}
		if err := cur.Seal(); err != nil {
			_ = cur.Abort()
			cur = nil
			return err
		}
		outputs = append(outputs, &sealedOutput{w: cur, minSeq: baseSeq + emitted - uint64(curRows)})
		cur = nil
		curRows = 0
		return nil
	}

	openOutput := func() error {
		if cur != nil {
			return nil
		}
		w, err := CreateCompactionOutput(s.dir, uint64(len(outputs)), currentSchema)
		if err != nil {
			return err
		}
		cur = w
		return nil
	}

	for _, sm := range picked {
		if err := ctx.Err(); err != nil {
			_ = closeOutput()
			return outputs, err
		}
		// 逐段打开、读完即关：输入 FD 数恒为 1。
		sr, err := s.openSegmentForRead(sm)
		if err != nil {
			_ = closeOutput()
			return outputs, err
		}
		readErr := func() error {
			defer func() {
				_ = sr.Close()
				s.trackClose()
			}()
			for {
				raw := make(Record, len(currentSchema.Columns))
				if err := sr.Next(raw); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				// 按当前 schema 投影：旧段缺失的新增列补 null。
				row := raw.Materialize(currentSchema)
				if curRows >= s.opt.MergeMaxRows {
					if err := closeOutput(); err != nil {
						return err
					}
				}
				if cur == nil {
					if err := openOutput(); err != nil {
						return err
					}
				}
				if err := cur.Append(row); err != nil {
					return err
				}
				curRows++
				emitted++
			}
		}()
		if readErr != nil {
			if cur != nil {
				_ = cur.Abort()
				cur = nil
			}
			return outputs, readErr
		}
	}
	if err := closeOutput(); err != nil {
		return outputs, err
	}
	return outputs, nil
}
