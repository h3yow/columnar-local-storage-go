package columnstore

import "os"

// 可见性与文件生命周期：
//   - ReadColumns 在读锁快照后立即 pin 所有可见段，扫描结束（含提前出错）时 unpin；
//   - compaction 原子换清单后，旧段进入 pendingDeletion，文件必须等 pin 归零才删除；
//   - 因此任何在途读取都不会看到"文件不存在"，同时磁盘文件数仍有上界：
//     现存文件 = 当前可见段 + 至多一批等待在途读者的旧段。
func (s *Store) pinSegments(paths []string) {
	s.segMu.Lock()
	for _, p := range paths {
		s.pins[p]++
	}
	s.segMu.Unlock()
}

func (s *Store) unpinSegments(paths []string) {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	for _, p := range paths {
		s.pins[p]--
		if s.pins[p] <= 0 {
			delete(s.pins, p)
			// 若它已被列入待删除，则最后一个读者刚离开，立即清理。
			if s.pendingDeletion[p] {
				delete(s.pendingDeletion, p)
				if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
					s.logf("compaction: warn deferred removing %s: %v", p, err)
				} else {
					s.logf("compaction: removed deferred %s after in-flight readers finished", p)
				}
			}
		}
	}
}

// removeSegments 标记旧段待删除：无在途读者则立即删除，否则延迟到最后一个 unpin。
func (s *Store) removeSegments(paths []string) {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	for _, p := range paths {
		if s.pins[p] > 0 {
			s.pendingDeletion[p] = true
			s.logf("compaction: defer removing %s (%d in-flight readers)", p, s.pins[p])
			continue
		}
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			s.logf("compaction: warn removing %s: %v (no longer visible)", p, err)
		}
	}
}

// deferredGarbageCount 返回"已不可见但仍有在途读者而暂留磁盘"的段文件数（诊断用）。
func (s *Store) deferredGarbageCount() int {
	s.segMu.Lock()
	defer s.segMu.Unlock()
	return len(s.pendingDeletion)
}
