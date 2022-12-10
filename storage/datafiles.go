package storage

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"tiny-bitcask/entity"
	"tiny-bitcask/index"
)

var (
	ReadMissDataErr  = errors.New("miss data during read")
	WriteMissDataErr = errors.New("miss data during write")
	DeleteEntryErr   = errors.New("read an entry which had deleted")
	MissOldFileErr   = errors.New("miss old file error")
	CrcErr           = errors.New("crc error")
)

const (
	FileSuffix = ".dat"
	B          = 1
	KB         = 1024 * B
	MB         = 1024 * KB
	GB         = 1024 * MB
)

type DataFiles struct {
	dir         string
	oIds        []int
	segmentSize int64
	active      *ActiveFile
	olds        map[int]*OldFile
}

func (dfs *DataFiles) GetOldFiles() []int {
	return dfs.oIds
}

func (dfs *DataFiles) RemoveReader(fid int) error {
	delete(dfs.olds, fid)
	return nil
}

func (dfs *DataFiles) AddReader(fid int) error {
	path := getFilePath(dfs.dir, fid)
	reader, err := NewOldFile(path)
	if err != nil {
		return err
	}
	dfs.olds[fid] = reader
	return nil
}

// NewDataFileWithFiles create a DataFiles with existing dir
func NewDataFileWithFiles(dir string, segmentSize int64) (dfs *DataFiles, err error) {
	dfs = &DataFiles{
		dir:         dir,
		olds:        map[int]*OldFile{},
		segmentSize: segmentSize,
	}

	fids, err := getFids(dir)
	if err != nil {
		return nil, err
	}
	aFid := fids[len(fids)-1]
	dfs.active, err = NewActiveFile(dir, aFid)
	if err != nil {
		return nil, err
	}
	if len(fids) == 1 {
		return dfs, nil
	}
	oldFids := fids[:len(fids)-1]
	for _, fid := range oldFids {
		path := getFilePath(dir, fid)
		reader, err := NewOldFile(path)
		if err != nil {
			return nil, err
		}
		dfs.olds[fid] = reader
	}

	return dfs, nil
}

// NewDataFiles create a DataFiles Object with an empty dir
func NewDataFiles(path string, segmentSize int64) (dfs *DataFiles, err error) {
	err = os.Mkdir(path, os.ModePerm)
	if err != nil {
		return nil, err
	}
	af, err := NewActiveFile(path, 1)
	if err != nil {
		return nil, err
	}
	dfs = &DataFiles{
		dir:         path,
		oIds:        nil,
		active:      af,
		olds:        map[int]*OldFile{},
		segmentSize: segmentSize,
	}
	return dfs, nil
}

func (dfs *DataFiles) rotate() error {
	aFid := dfs.active.fid
	path := getFilePath(dfs.dir, aFid)
	fd, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	r := &OldFile{fd: fd}
	dfs.olds[dfs.active.fid] = r
	dfs.oIds = append(dfs.oIds, aFid)

	af, err := NewActiveFile(dfs.dir, aFid+1)
	if err != nil {
		return err
	}
	dfs.active = af
	return nil
}

func (dfs *DataFiles) ReadEntry(index *index.Index) (e *entity.Entry, err error) {
	dataSize := entity.MetaSize + index.KeySize + index.ValueSize
	if index.Fid == dfs.active.fid {
		return dfs.active.ReadEntity(index.Off, dataSize)
	}
	of, exist := dfs.olds[index.Fid]
	if !exist {
		return nil, MissOldFileErr
	}
	return of.ReadEntity(index.Off, dataSize)
}

func (dfs *DataFiles) GetOldFile(fid int) *OldFile {
	return dfs.olds[fid]
}

func (dfs *DataFiles) RemoveFile(fid int) error {
	of := dfs.olds[fid]
	err := of.fd.Close()
	if err != nil {
		return err
	}
	path := getFilePath(dfs.dir, fid)
	err = os.Remove(path)
	if err != nil {
		return err
	}
	delete(dfs.olds, fid)
	return nil
}

func (dfs *DataFiles) WriterEntity(e entity.Entity) (h *entity.Hint, err error) {
	h, err = dfs.active.WriterEntity(e)
	if err != nil {
		return nil, err
	}
	if dfs.active.off > dfs.segmentSize {
		err := dfs.rotate()
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

type ActiveFile struct {
	fid int
	fd  *os.File
	off int64
}

func NewActiveFile(dir string, fid int) (af *ActiveFile, err error) {
	path := getFilePath(dir, fid)
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	af = &ActiveFile{
		fd:  fd,
		off: fi.Size(),
		fid: fid,
	}
	return af, nil
}

func (af *ActiveFile) WriterEntity(e entity.Entity) (h *entity.Hint, err error) {
	buf := e.Encode()
	n, err := af.fd.WriteAt(buf, af.off)
	if n < len(buf) {
		return nil, WriteMissDataErr
	}
	if err != nil {
		return nil, err
	}
	h = &entity.Hint{Fid: af.fid, Off: af.off}
	af.off += e.Size()
	return h, nil
}

func (af *ActiveFile) ReadEntity(off int64, length int) (e *entity.Entry, err error) {
	return readEntry(af.fd, off, length)
}

type OldFile struct {
	fd *os.File
}

func NewOldFile(path string) (of *OldFile, err error) {
	fd, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	of = &OldFile{fd: fd}
	return of, nil
}

func (of *OldFile) ReadEntity(off int64, length int) (e *entity.Entry, err error) {
	return readEntry(of.fd, off, length)
}

func (of *OldFile) ReadEntityWithOutLength(off int64) (e *entity.Entry, err error) {
	e = &entity.Entry{Meta: &entity.Meta{}}
	buf := make([]byte, entity.MetaSize)
	n, err := of.fd.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}
	if n < entity.MetaSize {
		return nil, ReadMissDataErr
	}
	off += entity.MetaSize
	e.DecodeMeta(buf)
	payloadSize := e.Meta.KeySize + e.Meta.ValueSize
	buf = make([]byte, payloadSize)
	n, err = of.fd.ReadAt(buf, off)
	if err != nil {
		return nil, err
	}
	if n < int(payloadSize) {
		return nil, ReadMissDataErr
	}
	e.DecodePayload(buf)
	return e, nil
}

func readEntry(fd *os.File, off int64, length int) (e *entity.Entry, err error) {
	buf := make([]byte, length)
	n, err := fd.ReadAt(buf, off)
	if n < length {
		return nil, ReadMissDataErr
	}
	if err != nil {
		return nil, err
	}
	e = entity.NewEntry()
	e.DecodeMeta(buf[:entity.MetaSize])
	e.DecodePayload(buf[entity.MetaSize:])
	return e, nil
}

func getFids(dir string) (fids []int, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fileName := f.Name()
		filePath := path.Base(fileName)
		if path.Ext(filePath) == FileSuffix {
			filePrefix := strings.TrimSuffix(filePath, FileSuffix)
			fid, err := strconv.Atoi(filePrefix)
			if err != nil {
				return nil, err
			}
			fids = append(fids, fid)
		}
	}
	return fids, nil
}

func getFilePath(dir string, fid int) string {
	return fmt.Sprintf("%s/%d%s", dir, fid, FileSuffix)
}
