package tiny_bitcask

import "tiny-bitcask/storage"

const (
	DefaultSegmentSize = 256 * storage.MB
)

var (
	DefaultOptions = &Options{
		Dir:         "db",
		SegmentSize: DefaultSegmentSize,
	}
)

type Options struct {
	Dir         string
	SegmentSize int64
}
