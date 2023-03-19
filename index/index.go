package index

import "tiny-bitcask/entity"

type KeyDir struct {
	Index map[string]*Index
}

func NewKD() *KeyDir {
	kd := &KeyDir{}
	kd.Index = map[string]*Index{}
	return kd
}

// Find searches an index in KeyDir
func (kd *KeyDir) Find(key string) *Index {
	i := kd.Index[key]
	return i
}

// Update inserts an index to KeyDir
func (kd *KeyDir) Update(key string, i *Index) {
	kd.Index[key] = i
}

// Delete deletes an index in KeyDir
func (kd *KeyDir) Delete(key string) {
	delete(kd.Index, key)
}

// Index means a certain position of an entity.Entry which stores in disk.
type Index struct {
	Fid       int
	Off       int64
	Timestamp uint64
	KeySize   int
	ValueSize int
}

// NewIndexByData create an Index via entity.Hint and entity.Entry
func NewIndexByData(hint *entity.Hint, entry *entity.Entry) *Index {
	return NewIndex(hint.Fid, hint.Off, len(entry.Key), len(entry.Value))
}

func NewIndex(fid int, off int64, keySize int, valueSize int) *Index {
	index := &Index{}
	index.Fid = fid
	index.Off = off
	index.KeySize = keySize
	index.ValueSize = valueSize
	return index
}

func (i *Index) IsEqualPos(fid int, off int64) bool {
	return i.Off == off && i.Fid == fid
}
