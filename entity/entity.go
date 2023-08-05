package entity

// Entity represents the objects that needs to store to disk
type Entity interface {
	Encode() []byte

	DecodePayload([]byte)

	DecodeMeta([]byte)

	Size() int64
}
