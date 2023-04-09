package index

import "tiny-bitcask/entity"

const (
	KeyNotFound = "key not found"
)

type Index interface {
	Find(key string) *DataPosition
	Delete(key string)
	Update(key string, dp *DataPosition)
	Add(key string, dp *DataPosition)
}

type indexer map[string]*DataPosition

func newIndexer() indexer {
	return indexer{}
}

type KeyDir struct {
	Index indexer
}

func NewKD() *KeyDir {
	kd := &KeyDir{}
	kd.Index = newIndexer()
	return kd
}

func (kd *KeyDir) Add(key string, dp *DataPosition) {
	kd.Index[key] = dp
}

// Find searches an index in KeyDir
func (kd *KeyDir) Find(key string) *DataPosition {
	dp := kd.Index[key]
	return dp
}

// Update inserts an index to KeyDir
func (kd *KeyDir) Update(key string, dp *DataPosition) {
	kd.Index[key] = dp
}

// Delete deletes an index in KeyDir
func (kd *KeyDir) Delete(key string) {
	delete(kd.Index, key)
}

// DataPosition means a certain position of an entity.Entry which stores in disk.
type DataPosition struct {
	Fid       int
	Off       int64
	Timestamp uint64
	KeySize   int
	ValueSize int
}

func (kd *KeyDir) AddIndexByData(hint *entity.Hint, entry *entity.Entry) {
	kd.AddIndexByRawInfo(hint.Fid, hint.Off, entry.Key, entry.Value)
}

func (kd *KeyDir) AddIndexByRawInfo(fid int, off int64, key, value []byte) {
	index := newDataPosition(fid, off, key, value)
	kd.Add(string(key), index)
}

func newDataPosition(fid int, off int64, key, value []byte) *DataPosition {
	dp := &DataPosition{}
	dp.Fid = fid
	dp.Off = off
	dp.KeySize = len(key)
	dp.ValueSize = len(value)
	return dp
}

func (i *DataPosition) IsEqualPos(fid int, off int64) bool {
	return i.Off == off && i.Fid == fid
}
