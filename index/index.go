package index

import (
	"sort"

	"tiny-bitcask/entity"
)

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
	kd.AddIndexByRawInfo(hint.Fid, hint.Off, entry.Key, entry.Value, entry.Meta.TimeStamp)
}

func (kd *KeyDir) AddIndexByRawInfo(fid int, off int64, key, value []byte, ts uint64) {
	index := newDataPosition(fid, off, key, value, ts)
	kd.Add(string(key), index)
}

// Range visits every key in arbitrary map order until fn returns false.
func (kd *KeyDir) Range(fn func(key string, dp *DataPosition) bool) {
	for k, dp := range kd.Index {
		if !fn(k, dp) {
			break
		}
	}
}

// SortedKeys returns keys in lexicographic order (copy).
func (kd *KeyDir) SortedKeys() []string {
	keys := make([]string, 0, len(kd.Index))
	for k := range kd.Index {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// AddIndexBySizes records keydir metadata without reading the value (e.g. hint recovery).
func (kd *KeyDir) AddIndexBySizes(fid int, off int64, key []byte, keySize, valueSize int, ts uint64) {
	dp := &DataPosition{
		Fid:       fid,
		Off:       off,
		Timestamp: ts,
		KeySize:   keySize,
		ValueSize: valueSize,
	}
	kd.Add(string(key), dp)
}

func newDataPosition(fid int, off int64, key, value []byte, ts uint64) *DataPosition {
	dp := &DataPosition{}
	dp.Fid = fid
	dp.Off = off
	dp.Timestamp = ts
	dp.KeySize = len(key)
	dp.ValueSize = len(value)
	return dp
}

func (i *DataPosition) IsEqualPos(fid int, off int64) bool {
	return i.Off == off && i.Fid == fid
}
