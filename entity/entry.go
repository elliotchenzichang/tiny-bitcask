package entity

import (
	"encoding/binary"
	"hash/crc32"
	"time"
)

const (
	MetaSize   = 29
	DeleteFlag = 1
)

type Hint struct {
	Fid int
	Off int64
}

type Entry struct {
	Key   []byte
	Value []byte
	Meta  *Meta
}

type Meta struct {
	Crc       uint32
	position  uint64
	TimeStamp uint64
	KeySize   uint32
	ValueSize uint32
	Flag      uint8
}

func NewEntryWithData(key []byte, value []byte) *Entry {
	now := uint64(time.Now().Unix())
	meta := NewMeta().WithTimeStamp(now).WithKeySize(uint32(len(key))).WithValueSize(uint32(len(value)))
	e := NewEntry().WithKey(key).WithValue(value).WithMeta(meta)
	return e
}

func (e *Entry) Encode() []byte {
	size := e.Size()
	buf := make([]byte, size)
	binary.LittleEndian.PutUint64(buf[4:12], e.Meta.position)
	binary.LittleEndian.PutUint64(buf[12:20], e.Meta.TimeStamp)
	binary.LittleEndian.PutUint32(buf[20:24], e.Meta.KeySize)
	binary.LittleEndian.PutUint32(buf[24:28], e.Meta.ValueSize)
	buf[28] = e.Meta.Flag
	if e.Meta.Flag != DeleteFlag {
		copy(buf[MetaSize:MetaSize+len(e.Key)], e.Key)
		copy(buf[MetaSize+len(e.Key):MetaSize+len(e.Key)+len(e.Value)], e.Value)
	}
	c32 := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], c32)
	return buf
}

func (e *Entry) DecodePayload(payload []byte) {
	keyHighBound := int(e.Meta.KeySize)
	valueHighBound := keyHighBound + int(e.Meta.ValueSize)
	e.Key = payload[0:keyHighBound]
	e.Value = payload[keyHighBound:valueHighBound]
}

func (e *Entry) DecodeMeta(bytes []byte) {
	e.Meta.Crc = binary.LittleEndian.Uint32(bytes[0:4])
	e.Meta.position = binary.LittleEndian.Uint64(bytes[4:12])
	e.Meta.TimeStamp = binary.LittleEndian.Uint64(bytes[12:20])
	e.Meta.KeySize = binary.LittleEndian.Uint32(bytes[20:24])
	e.Meta.ValueSize = binary.LittleEndian.Uint32(bytes[24:28])
}

func (e *Entry) Size() int64 {
	return int64(MetaSize + e.Meta.KeySize + e.Meta.ValueSize)
}

func (e *Entry) GetCrc(buf []byte) uint32 {
	crc := crc32.ChecksumIEEE(buf[4:])
	crc = crc32.Update(crc, crc32.IEEETable, e.Key)
	crc = crc32.Update(crc, crc32.IEEETable, e.Value)
	return crc
}

func NewEntry() *Entry {
	return new(Entry)
}

func (e *Entry) WithKey(key []byte) *Entry {
	e.Key = key
	return e
}

func (e *Entry) WithValue(value []byte) *Entry {
	e.Value = value
	return e
}

func (e *Entry) WithMeta(meta *Meta) *Entry {
	e.Meta = meta
	return e
}

func NewMeta() *Meta {
	return new(Meta)
}

func (m *Meta) WithPosition(pos uint64) *Meta {
	m.position = pos
	return m
}

func (m *Meta) WithTimeStamp(timestamp uint64) *Meta {
	m.TimeStamp = timestamp
	return m
}

func (m *Meta) WithKeySize(keySize uint32) *Meta {
	m.KeySize = keySize
	return m
}

func (m *Meta) WithValueSize(valueSize uint32) *Meta {
	m.ValueSize = valueSize
	return m
}

func (m *Meta) WithFlag(flag uint8) *Meta {
	m.Flag = flag
	return m
}

func NewHint() *Hint {
	return new(Hint)
}

func (h *Hint) WithFid(fid int) *Hint {
	h.Fid = fid
	return h
}

func (h *Hint) WithOff(off int64) *Hint {
	h.Off = off
	return h
}
