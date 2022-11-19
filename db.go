package tiny_bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

var (
	KeyNotFound   = errors.New("key not found")
	NoNeedToMerge = errors.New("no need to merge")
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
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.s, err = NewStorage(opt.Dir, fileSize)
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

func getSegmentSize(size int64) int64 {
	var fileSize int64
	if size <= 0 {
		fileSize = 4 * KB
	} else {
		fileSize = size
	}
	return fileSize
}

func (db *DB) Merge() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	fids, err := getFids(db.s.dir)
	if err != nil {
		return err
	}
	if len(fids) < 2 {
		return NoNeedToMerge
	}
	sort.Ints(fids)
	for _, fid := range fids[:len(fids)-1] {
		var off int64 = 0
		for {
			entry, err := db.s.readEntry(fid, off)
			if err == nil {
				off += int64(entry.Size())
				oldIndex := db.kd.index[string(entry.key)]
				if oldIndex.fid == fid && oldIndex.off == off {
					newIndex, err := db.s.writeAt(entry.Encode())
					if err != nil {
						return err
					}
					db.kd.index[string(entry.key)] = newIndex
				}
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
		err = os.Remove(fmt.Sprintf("%s/%d%s", db.s.dir, fid, fileSuffix))
		if err != nil {
			return err
		}
	}
	return nil
}
