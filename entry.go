package tiny_bitcask

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	MetaSize = 28
)

type Entry struct {
	key   []byte
	value []byte
	meta  *Meta
}

type Meta struct {
	crc       uint32
	position  uint64
	timeStamp uint64
	keySize   uint32
	valueSize uint32
}

func NewEntryWithData(key []byte, value []byte) *Entry {
	e := &Entry{}
	e.key = key
	e.value = value
	e.meta = &Meta{
		timeStamp: uint64(time.Now().Unix()),
		keySize:   uint32(len(key)),
		valueSize: uint32(len(value)),
	}
	return e
}

func NewEntry() *Entry {
	e := &Entry{
		meta: &Meta{},
	}
	return e
}

func (e *Entry) Encode() []byte {
	size := e.Size()
	buf := make([]byte, size)
	binary.LittleEndian.PutUint64(buf[4:12], e.meta.position)
	binary.LittleEndian.PutUint64(buf[12:20], e.meta.timeStamp)
	binary.LittleEndian.PutUint32(buf[20:24], e.meta.keySize)
	binary.LittleEndian.PutUint32(buf[24:28], e.meta.valueSize)
	copy(buf[MetaSize:MetaSize+len(e.key)], e.key)
	copy(buf[MetaSize+len(e.key):MetaSize+len(e.key)+len(e.value)], e.value)
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	return buf
}

func (e *Entry) DecodePayload(payload []byte) error {
	keyHighBound := int(e.meta.keySize)
	valueHighBound := keyHighBound + int(e.meta.valueSize)
	e.key = payload[0:keyHighBound]
	e.value = payload[keyHighBound:valueHighBound]
	return nil
}

func (e *Entry) DecodeMeta(bytes []byte) {
	e.meta.crc = binary.LittleEndian.Uint32(bytes[0:4])
	e.meta.position = binary.LittleEndian.Uint64(bytes[4:12])
	e.meta.timeStamp = binary.LittleEndian.Uint64(bytes[12:20])
	e.meta.keySize = binary.LittleEndian.Uint32(bytes[20:24])
	e.meta.valueSize = binary.LittleEndian.Uint32(bytes[24:28])
}

func (e *Entry) Size() int {
	return int(MetaSize + e.meta.keySize + e.meta.valueSize)
}

func (e *Entry) getCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.key)
	crc = crc32.Update(crc, crc32.IEEETable, e.value)
	return crc
}
