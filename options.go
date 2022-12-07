package tiny_bitcask

const (
	DefaultSegmentSize = 256 * MB
)

type Options struct {
	Dir         string
	SegmentSize int64
}
