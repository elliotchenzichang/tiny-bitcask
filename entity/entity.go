package entity

type Entity interface {
	Encode() []byte

	DecodePayload([]byte)

	DecodeMeta([]byte)

	Size() int64
}
