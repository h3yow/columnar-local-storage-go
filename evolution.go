package columnstore

// 演进只改 MANIFEST 中的 schema 映射，不重写任何历史段：
// 段内列块以稳定 ID 标识，读路径按 ID 对齐，因此
//   - 加列：旧段缺该列块，读回按 DefaultValue（无则 null）补；
//   - 改名：仅 Name 变化，ID 不变，绝不串列；
//   - 删列：标记 Deleted，旧块不再暴露，ID 不复用；
//   - 不兼容变更（同 ID 改类型）没有 API 入口，必须被拒绝。

// AddColumn 增加一列。nullable 为 false 时必须给出同类型非空 defaultValue。
func (s *Store) AddColumn(name string, t ColumnType, nullable bool, defaultValue any) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mergeLock.Lock()
	defer s.mergeLock.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	ns, err := addColumn(s.mani.Schema, name, t, nullable, defaultValue)
	if err != nil {
		s.logf("schema: ADD %s REJECT err=%v", name, err)
		return err
	}
	newM := s.mani.cloneForUpdate()
	newM.Schema = ns
	if err := s.publish(newM); err != nil {
		return err
	}
	s.logf("schema: ADD %s type=%s nullable=%v -> schema v%d (segments untouched=%d)",
		name, t, nullable, ns.Version, len(newM.Segments))
	return nil
}

// RenameColumn 修改列名，ID 与历史数据保持不变。
func (s *Store) RenameColumn(oldName, newName string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mergeLock.Lock()
	defer s.mergeLock.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	ns, err := renameColumn(s.mani.Schema, oldName, newName)
	if err != nil {
		return err
	}
	newM := s.mani.cloneForUpdate()
	newM.Schema = ns
	if err := s.publish(newM); err != nil {
		return err
	}
	s.logf("schema: RENAME %s -> %s (id unchanged) -> schema v%d",
		oldName, newName, ns.Version)
	return nil
}

// DropColumn 将列标记删除：新写入拒绝该列，读取不再返回该列。
func (s *Store) DropColumn(name string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mergeLock.Lock()
	defer s.mergeLock.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	ns, err := dropColumn(s.mani.Schema, name)
	if err != nil {
		s.logf("schema: DROP %s REJECT err=%v", name, err)
		return err
	}
	newM := s.mani.cloneForUpdate()
	newM.Schema = ns
	if err := s.publish(newM); err != nil {
		return err
	}
	s.logf("schema: DROP %s -> schema v%d (historical blocks retained on disk)", name, ns.Version)
	return nil
}

// Schema 返回当前 schema 的副本，便于测试与调用方检查。
func (s *Store) Schema() *Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mani.Schema.clone()
}

// Stats 是数据集的当前可见状态快照。
type Stats struct {
	SchemaVersion int
	SegmentCount  int
	TotalRows     int
}

func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		SchemaVersion: s.mani.Schema.Version,
		SegmentCount:  len(s.mani.Segments),
		TotalRows:     totalRows(s.mani),
	}
}
