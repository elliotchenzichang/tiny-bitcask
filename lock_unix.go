//go:build unix

package tiny_bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func acquireDBLock(dir string, readOnly, exclusive bool) (*os.File, error) {
	if !exclusive {
		return nil, nil
	}
	path := filepath.Join(dir, ".tiny-bitcask.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	how := syscall.LOCK_EX
	if readOnly {
		how = syscall.LOCK_SH
	}
	if err := syscall.Flock(int(f.Fd()), how|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("tiny-bitcask: database lock: %w", err)
	}
	return f, nil
}
