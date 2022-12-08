package tiny_bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"tiny-bitcask/entity"
	"tiny-bitcask/storage"
)

var (
	KeyNotFoundErr   = errors.New("key not found")
	NoNeedToMergeErr = errors.New("no need to merge")
)

type DB struct {
	rw sync.RWMutex
	kd *keyDir
	s  *storage.DataFile
}

func NewDB(opt *Options) (db *DB, err error) {
	db = &DB{
		rw: sync.RWMutex{},
		kd: &keyDir{index: map[string]*Index{}},
	}
	if isExist, _ := isDirExist(opt.Dir); isExist {
		if err := db.recovery(opt); err != nil {
			return nil, err
		}
		return db, nil
	}
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.s, err = storage.NewDataFile(opt.Dir, fileSize)
	if err != nil {
		return nil, err
	}
	return db, err
}

func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	entry := entity.NewEntryWithData(key, value)
	buf := entry.Encode()
	fid, off, err := db.s.WriteAt(buf)
	if err != nil {
		return err
	}
	index := &Index{
		Fid:       fid,
		Off:       off,
		keySize:   len(key),
		valueSize: len(value),
	}
	db.kd.update(string(key), index)
	return nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	db.rw.RLock()
	defer db.rw.RUnlock()
	i := db.kd.find(string(key))
	if i == nil {
		return nil, KeyNotFoundErr
	}
	dataSize := entity.MetaSize + i.keySize + i.valueSize
	buf := make([]byte, dataSize)
	entry, err := db.s.ReadFullEntry(i.Fid, i.Off, buf)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (db *DB) Delete(key []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	index := db.kd.find(string(key))
	if index == nil {
		return KeyNotFoundErr
	}
	e := entity.NewEntry()
	e.Meta.Flag = entity.DeleteFlag
	_, _, err := db.s.WriteAt(e.Encode())
	if err != nil {
		return err
	}
	delete(db.kd.index, string(key))
	return nil
}

func (db *DB) Merge() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	fids, err := getFids(db.s.Dir)
	if err != nil {
		return err
	}
	if len(fids) < 2 {
		return NoNeedToMergeErr
	}
	sort.Ints(fids)
	for _, fid := range fids[:len(fids)-1] {
		var off int64 = 0
		for {
			entry, err := db.s.ReadEntry(fid, off)
			if err == nil {
				off += int64(entry.Size())
				oldIndex := db.kd.index[string(entry.Key)]
				if oldIndex == nil {
					continue
				}
				if oldIndex.Fid == fid && oldIndex.Off == off {
					fid, off, err := db.s.WriteAt(entry.Encode())
					newIndex := &Index{
						Fid: fid,
						Off: off,
					}
					if err != nil {
						return err
					}
					db.kd.index[string(entry.Key)] = newIndex
				}
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
		err = os.Remove(fmt.Sprintf("%s/%d%s", db.s.Dir, fid, storage.FileSuffix))
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) recovery(opt *Options) (err error) {
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.s = &storage.DataFile{
		Dir:      opt.Dir,
		FileSize: fileSize,
		Fds:      map[int]*os.File{},
	}
	fids, err := getFids(opt.Dir)
	if err != nil {
		return err
	}
	sort.Ints(fids)
	for _, fid := range fids {
		var off int64 = 0
		path := fmt.Sprintf("%s/%d%s", opt.Dir, fid, storage.FileSuffix)
		fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
		db.s.Fds[fid] = fd
		for {
			entry, err := db.s.ReadEntry(fid, off)
			if err == nil {
				db.kd.index[string(entry.Key)] = &Index{
					Fid:       fid,
					Off:       off,
					timestamp: entry.Meta.TimeStamp,
				}
				off += int64(entry.Size())
			} else {
				if err == storage.DeleteEntryErr {
					continue
				}
				if err == io.EOF {
					break
				}
				return err
			}
		}
		if fid == fids[len(fids)-1] {
			af := &storage.ActiveFile{
				Fid: fid,
				F:   fd,
				Off: off,
			}
			db.s.Af = af
		}
	}
	return err
}
