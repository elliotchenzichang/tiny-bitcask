//go:build !unix

package tiny_bitcask

import "os"

// acquireDBLock is a no-op on non-Unix platforms; ExclusiveLock does not enforce single-writer access.
func acquireDBLock(_ string, _, _ bool) (*os.File, error) {
	return nil, nil
}
