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

func (kd *KeyDir) Find(key string) *Index {
	i := kd.Index[key]
	return i
}

func (kd *KeyDir) Update(key string, i *Index) {
	kd.Index[key] = i
}

type Index struct {
	Fid       int
	Off       int64
	Timestamp uint64
	KeySize   int
	ValueSize int
}

func NewIndexByHint(hint *entity.Hint) *Index {
	return NewIndex(hint.Fid, hint.Off)
}

func NewIndex(fid int, off int64) *Index {
	index := &Index{}
	index.Fid = fid
	index.Off = off
	return index
}

func (i *Index) IsEqualPos(fid int, off int64) bool {
	return i.Off == off && i.Fid == fid
}
