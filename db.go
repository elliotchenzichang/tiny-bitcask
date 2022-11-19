package tiny_bitcask

import (
	"errors"
	"sync"
)

var (
	KeyNotFound = errors.New("key not found")
)

type DB struct {
	rw sync.RWMutex
	kd *keyDir
	s  *Storage
}

func NewDB(opt *Options) (db *DB, err error) {
	db = &DB{
		rw: sync.RWMutex{},
		kd: &keyDir{index: map[string]*Index{}},
	}
	db.s, err = NewStorage(opt.Dir)
	if err != nil {
		return nil, err
	}
	return db, err
}

func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	entry := NewEntryWithData(key, value)
	buf := entry.Encode()
	index, err := db.s.writeAt(buf)
	if err != nil {
		return err
	}
	db.kd.update(string(key), index)
	return nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	db.rw.RLock()
	defer db.rw.RUnlock()
	i := db.kd.find(string(key))
	if i == nil {
		return nil, KeyNotFound
	}
	entry, err := db.s.readEntry(i.fid, i.off)
	if err != nil {
		return nil, err
	}
	return entry.value, nil
}
