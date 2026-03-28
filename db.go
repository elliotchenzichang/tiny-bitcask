package tiny_bitcask

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"tiny-bitcask/entity"
	"tiny-bitcask/index"
	"tiny-bitcask/storage"
)

var (
	KeyNotFoundErr   = errors.New("key not found")
	NoNeedToMergeErr = errors.New("no need to merge")
	ReadOnlyDBErr    = errors.New("read-only database")
)

type DB struct {
	rw       sync.RWMutex
	kd       *index.KeyDir
	storage  *storage.DataFiles
	opt      *Options
	lockFile *os.File
}

// NewDB create a new DB instance with Options
func NewDB(opt *Options) (db *DB, err error) {
	db = &DB{}
	db.kd = index.NewKD()
	db.opt = opt

	exists, err := isDirExist(opt.Dir)
	if err != nil {
		return nil, err
	}
	if opt.ReadOnly && !exists {
		return nil, fmt.Errorf("tiny-bitcask: read-only open: %w", os.ErrNotExist)
	}

	if exists {
		lf, err := acquireDBLock(opt.Dir, opt.ReadOnly, opt.ExclusiveLock)
		if err != nil {
			return nil, err
		}
		db.lockFile = lf
		if err := db.recovery(opt); err != nil {
			_ = db.closeStorageAndLock()
			return nil, err
		}
		return db, nil
	}

	var fileSize = getSegmentSize(opt.SegmentSize)
	db.storage, err = storage.NewDataFiles(opt.Dir, fileSize, opt.VerifyCRC)
	if err != nil {
		return nil, err
	}
	lf, err := acquireDBLock(opt.Dir, false, opt.ExclusiveLock)
	if err != nil {
		_ = db.storage.Close()
		return nil, err
	}
	db.lockFile = lf
	return db, nil
}

func (db *DB) closeStorageAndLock() error {
	var first error
	if db.storage != nil {
		if err := db.storage.Close(); err != nil && first == nil {
			first = err
		}
		db.storage = nil
	}
	if db.lockFile != nil {
		_ = db.lockFile.Close()
		db.lockFile = nil
	}
	return first
}

// Sync flushes the active data file (fsync). Thread-safe.
func (db *DB) Sync() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	if db.storage == nil {
		return nil
	}
	return db.storage.Sync()
}

// Close syncs and releases file descriptors and the advisory lock.
func (db *DB) Close() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	if err := db.storageSyncBestEffort(); err != nil {
		_ = db.closeStorageAndLock()
		return err
	}
	return db.closeStorageAndLock()
}

func (db *DB) storageSyncBestEffort() error {
	if db.storage == nil {
		return nil
	}
	return db.storage.Sync()
}

// ListKeys returns all keys in lexicographic order (snapshot under read lock).
func (db *DB) ListKeys() [][]byte {
	db.rw.RLock()
	defer db.rw.RUnlock()
	keys := db.kd.SortedKeys()
	out := make([][]byte, len(keys))
	for i, k := range keys {
		out[i] = []byte(k)
	}
	return out
}

// Fold visits every key in sorted order and calls fn with the current value. Holds one read lock for the scan.
func (db *DB) Fold(fn func(key, value []byte) error) error {
	db.rw.RLock()
	defer db.rw.RUnlock()
	for _, k := range db.kd.SortedKeys() {
		dp := db.kd.Find(k)
		if dp == nil {
			continue
		}
		entry, err := db.storage.ReadEntry(dp)
		if err != nil {
			return err
		}
		if err := fn([]byte(k), entry.Value); err != nil {
			return err
		}
	}
	return nil
}

// Set sets a key-value pairs into DB
func (db *DB) Set(key []byte, value []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	if db.opt.ReadOnly {
		return ReadOnlyDBErr
	}
	entry := entity.NewEntryWithData(key, value)
	h, err := db.storage.WriterEntity(entry)
	if err != nil {
		return err
	}
	db.kd.AddIndexByData(h, entry)
	return nil
}

// Get gets value by using key
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

// Delete delete a key
func (db *DB) Delete(key []byte) error {
	db.rw.Lock()
	defer db.rw.Unlock()
	if db.opt.ReadOnly {
		return ReadOnlyDBErr
	}
	keyStr := string(key)
	index := db.kd.Find(keyStr)
	if index == nil {
		return KeyNotFoundErr
	}
	e := entity.NewTombstoneEntry(key)
	_, err := db.storage.WriterEntity(e)
	if err != nil {
		return err
	}
	db.kd.Delete(keyStr)
	return nil
}

// Merge compacts old segments: copies live records still stored only in mergeable
// files into the active file, then removes those segment files.
func (db *DB) Merge() error {
	db.rw.Lock()
	defer db.rw.Unlock()
	if db.opt.ReadOnly {
		return ReadOnlyDBErr
	}
	fids := db.storage.GetOldFiles()
	if len(fids) < 2 {
		return NoNeedToMergeErr
	}
	toMerge := append([]int(nil), fids...)
	sort.Ints(toMerge)
	for _, fid := range toMerge[:len(toMerge)-1] {
		if err := db.mergeOldFile(fid); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) mergeOldFile(fid int) error {
	reader := db.storage.GetOldFile(fid)
	var off int64
	for {
		entry, err := reader.ReadEntityWithOutLength(off)
		if err != nil {
			if err == io.EOF {
				return db.storage.RemoveFile(fid)
			}
			return err
		}
		if entry.Meta.Flag == entity.DeleteFlag {
			off += entry.Size()
			continue
		}
		// entryOff is the record start offset; keydir stores the same (see IsEqualPos).
		entryOff := off
		off += entry.Size()

		idx := db.kd.Find(string(entry.Key))
		if idx == nil || !idx.IsEqualPos(fid, entryOff) {
			continue
		}
		h, err := db.storage.WriterEntity(entry)
		if err != nil {
			return err
		}
		db.kd.AddIndexByData(h, entry)
	}
}

// recovery  will rebuild a db from existing dir
func (db *DB) recovery(opt *Options) (err error) {
	var fileSize = getSegmentSize(opt.SegmentSize)
	db.storage, err = storage.NewDataFileWithFiles(opt.Dir, fileSize, opt.VerifyCRC, opt.ReadOnly)
	if err != nil {
		return err
	}
	fids, err := storage.ListDataFileIDs(opt.Dir)
	if err != nil {
		return err
	}
	for i, fid := range fids {
		isActive := i == len(fids)-1
		if err := db.recoverSegment(fid, opt.Dir, isActive, opt.VerifyCRC); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) recoverFromHint(fid int, dir string) error {
	recs, err := storage.ReadHintFile(dir, fid)
	if err != nil {
		return err
	}
	datPath := storage.DataFilePath(dir, fid)
	st, err := os.Stat(datPath)
	if err != nil {
		return err
	}
	datSize := st.Size()
	for _, r := range recs {
		if r.Flag == entity.DeleteFlag {
			continue
		}
		if int(r.KeySize) != len(r.Key) {
			return errors.New("hint key length mismatch")
		}
		recLen := int64(entity.MetaSize + r.KeySize + r.ValueSize)
		if r.RecordOffset < 0 || r.RecordOffset+recLen > datSize {
			return errors.New("hint record out of range for data file")
		}
		db.kd.AddIndexBySizes(fid, r.RecordOffset, r.Key, int(r.KeySize), int(r.ValueSize), r.Timestamp)
	}
	return nil
}

func (db *DB) recoverSegment(fid int, dir string, isActive bool, verifyCRC bool) error {
	if !isActive && storage.HintFileExists(dir, fid) {
		if err := db.recoverFromHint(fid, dir); err == nil {
			return nil
		}
	}

	path := storage.DataFilePath(dir, fid)
	of, err := storage.NewOldFile(path, verifyCRC)
	if err != nil {
		return err
	}
	defer of.Close()
	var off int64
	for {
		entry, err := of.ReadEntityWithOutLength(off)
		if err == nil {
			if entry.Meta.Flag == entity.DeleteFlag {
				db.kd.Delete(string(entry.Key))
			} else {
				db.kd.AddIndexByRawInfo(fid, off, entry.Key, entry.Value, entry.Meta.TimeStamp)
			}
			off += entry.Size()
		} else {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}
