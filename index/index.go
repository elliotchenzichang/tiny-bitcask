package index

type KeyDir struct {
	Index map[string]*Index
}

type Index struct {
	Fid       int
	Off       int64
	Timestamp uint64
	KeySize   int
	ValueSize int
}

func (kd *KeyDir) Find(key string) *Index {
	i := kd.Index[key]
	return i
}

func (kd *KeyDir) Update(key string, i *Index) {
	kd.Index[key] = i
}
