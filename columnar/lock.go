package columnar

// 本文件提供基于 flock 的进程级互斥锁，防止多个进程同时写同一数据集。

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// fileLock 是对一个锁文件持有 flock 独占锁的句柄。
type fileLock struct {
	f *os.File
}

// acquireFileLock 在 dir/lock 上获取非阻塞独占锁；已被占用时返回错误。
func acquireFileLock(dir string) (*fileLock, error) {
	path := filepath.Join(dir, lockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("columnar: open lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("columnar: dataset %q is locked by another process", dir)
		}
		return nil, fmt.Errorf("columnar: acquire flock: %w", err)
	}
	return &fileLock{f: f}, nil
}

// unlock 释放锁并关闭文件。
func (l *fileLock) unlock() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}
