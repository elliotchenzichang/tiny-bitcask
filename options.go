package tiny_bitcask

import "tiny-bitcask/storage"

const (
	DefaultSegmentSize = 256 * storage.MB
)

var (
	DefaultOptions = &Options{
		Dir:             "db",
		SegmentSize:     DefaultSegmentSize,
		VerifyCRC:       true,
		ExclusiveLock:   true,
	}
)

// Options configures the database. Zero value is not valid; use DefaultOptions or set fields explicitly.
type Options struct {
	Dir           string
	SegmentSize   int64
	VerifyCRC     bool // verify CRC32 on every read (default true when using DefaultOptions)
	ReadOnly      bool // open existing store read-only (ListKeys, Get, Fold allowed)
	ExclusiveLock bool // advisory flock on .tiny-bitcask.lock (Unix); shared lock when ReadOnly
}
