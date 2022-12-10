package tiny_bitcask

import (
	"errors"
	"io"
	"sort"
	"sync"
	"tiny-bitcask/entity"
	"tiny-bitcask/index"
	"tiny-bitcask/storage"
)

var (
	KeyNotFoundErr   = errors.New("key not found")
	NoNeedToMergeErr = errors.New("no need to merge")
)

type DB struct {
	rw      sync.RWMutex
	kd      *index.KeyDir
	storage *storage.DataFiles
}

func NewDB(opt *Options) (db *DB, err error) {
	db = &DB{
		rw: sync.RWMutex{},
		kd: &index.KeyDir{Index: map[string]*index.Index{}},
	}
	if isExist, _ := isDirExist(opt.Dir); isExist {
		if err := db.recovery(opt); err != nil {
			return nil, err
		}
		return db, nil
	}
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.storage, err = storage.NewDataFiles(opt.Dir, fileSize)
	if err != nil {
		return nil, err
	}
	return db, err
}

func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	entry := entity.NewEntryWithData(key, value)
	h, err := db.storage.WriterEntity(entry)
	if err != nil {
		return err
	}
	index := &index.Index{
		Fid:       h.Fid,
		Off:       h.Off,
		KeySize:   len(key),
		ValueSize: len(value),
	}
	db.kd.Update(string(key), index)
	return nil
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	db.rw.RLock()
	defer db.rw.RUnlock()
	i := db.kd.Find(string(key))
	if i == nil {
		return nil, KeyNotFoundErr
	}
	entry, err := db.storage.ReadEntry(i)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (db *DB) Delete(key []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	index := db.kd.Find(string(key))
	if index == nil {
		return KeyNotFoundErr
	}
	e := entity.NewEntry()
	e.Meta.Flag = entity.DeleteFlag
	_, err := db.storage.WriterEntity(e)
	if err != nil {
		return err
	}
	delete(db.kd.Index, string(key))
	return nil
}

func (db *DB) Merge() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	fids := db.storage.GetOldFiles()
	if len(fids) < 2 {
		return NoNeedToMergeErr
	}
	sort.Ints(fids)
	for _, fid := range fids[:len(fids)-1] {
		var off int64 = 0
		reader := db.storage.GetOldFile(fid)
		for {
			entry, err := reader.ReadEntityWithOutLength(off)
			if err == nil {
				off += int64(entry.Size())
				oldIndex := db.kd.Index[string(entry.Key)]
				if oldIndex == nil {
					continue
				}
				if oldIndex.Fid == fid && oldIndex.Off == off {
					h, err := db.storage.WriterEntity(entry)
					newIndex := &index.Index{
						Fid: h.Fid,
						Off: h.Off,
					}
					if err != nil {
						return err
					}
					db.kd.Index[string(entry.Key)] = newIndex
				}
			} else {
				if err == io.EOF {
					break
				}
				return err
			}
		}
		err := db.storage.RemoveFile(fid)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) recovery(opt *Options) (err error) {
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.storage, err = storage.NewDataFileWithFiles(opt.Dir, fileSize)
	if err != nil {
		return err
	}
	fids := db.storage.GetOldFiles()
	if err != nil {
		return err
	}
	sort.Ints(fids)
	for _, fid := range fids {
		var off int64 = 0
		reader := db.storage.GetOldFile(fid)
		for {
			entry, err := reader.ReadEntityWithOutLength(off)
			if err == nil {
				db.kd.Index[string(entry.Key)] = &index.Index{
					Fid:       fid,
					Off:       off,
					Timestamp: entry.Meta.TimeStamp,
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
	}
	return err
}
