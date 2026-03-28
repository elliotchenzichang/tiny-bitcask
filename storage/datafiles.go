package storage

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
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

type oldFiles map[int]*OldFile

func newOldFiles() oldFiles {
	return oldFiles{}
}

type DataFiles struct {
	dir         string
	oIds        []int
	segmentSize int64
	active      *ActiveFile
	olds        map[int]*OldFile
	verifyCRC   bool
	readOnly    bool
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
	reader, err := NewOldFile(path, dfs.verifyCRC)
	if err != nil {
		return err
	}
	dfs.olds[fid] = reader
	return nil
}

// NewDataFileWithFiles create a DataFiles with existing dir
func NewDataFileWithFiles(dir string, segmentSize int64, verifyCRC bool, readOnly bool) (dfs *DataFiles, err error) {
	dfs = &DataFiles{
		dir:         dir,
		olds:        newOldFiles(),
		segmentSize: segmentSize,
		verifyCRC:   verifyCRC,
		readOnly:    readOnly,
	}

	fids, err := getFids(dir)
	if err != nil {
		return nil, err
	}
	if len(fids) == 0 {
		return nil, fmt.Errorf("storage: no %s files in %s", FileSuffix, dir)
	}
	aFid := fids[len(fids)-1]
	dfs.active, err = NewActiveFile(dir, aFid, readOnly, verifyCRC)
	if err != nil {
		return nil, err
	}
	if len(fids) > 1 {
		dfs.oIds = make([]int, len(fids)-1)
		copy(dfs.oIds, fids[:len(fids)-1])
	}
	if len(fids) == 1 {
		return dfs, nil
	}
	oldFids := fids[:len(fids)-1]
	for _, fid := range oldFids {
		path := getFilePath(dir, fid)
		reader, err := NewOldFile(path, verifyCRC)
		if err != nil {
			return nil, err
		}
		dfs.olds[fid] = reader
	}

	return dfs, nil
}

// NewDataFiles create a DataFiles Object with an empty dir
func NewDataFiles(path string, segmentSize int64, verifyCRC bool) (dfs *DataFiles, err error) {
	err = os.Mkdir(path, os.ModePerm)
	if err != nil {
		return nil, err
	}
	af, err := NewActiveFile(path, 1, false, verifyCRC)
	if err != nil {
		return nil, err
	}
	dfs = &DataFiles{
		dir:         path,
		oIds:        nil,
		active:      af,
		olds:        map[int]*OldFile{},
		segmentSize: segmentSize,
		verifyCRC:   verifyCRC,
		readOnly:    false,
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
	r := &OldFile{fd: fd, verifyCRC: dfs.verifyCRC}
	dfs.olds[dfs.active.fid] = r
	dfs.oIds = append(dfs.oIds, aFid)

	af, err := NewActiveFile(dfs.dir, aFid+1, dfs.readOnly, dfs.verifyCRC)
	if err != nil {
		return err
	}
	dfs.active = af
	if err := WriteHintFileForDataFile(dfs.dir, aFid, dfs.verifyCRC); err != nil {
		return err
	}
	return nil
}

func (dfs *DataFiles) ReadEntry(index *index.DataPosition) (e *entity.Entry, err error) {
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

// Sync flushes the active segment to stable storage.
func (dfs *DataFiles) Sync() error {
	return dfs.active.fd.Sync()
}

// Close releases file descriptors for the active and old segments.
func (dfs *DataFiles) Close() error {
	var first error
	if dfs.active != nil && dfs.active.fd != nil {
		if err := dfs.active.fd.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, of := range dfs.olds {
		if of != nil && of.fd != nil {
			if err := of.fd.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	dfs.olds = nil
	dfs.active = nil
	return first
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
	RemoveHintFile(dfs.dir, fid)
	delete(dfs.olds, fid)
	for i, id := range dfs.oIds {
		if id == fid {
			dfs.oIds = append(dfs.oIds[:i], dfs.oIds[i+1:]...)
			break
		}
	}
	return nil
}

func (dfs *DataFiles) WriterEntity(e entity.Entity) (h *entity.Hint, err error) {
	if dfs.readOnly {
		return nil, errors.New("storage: read-only database")
	}
	h, err = dfs.active.WriterEntity(e)
	if err != nil {
		return nil, err
	}
	if dfs.canRotate() {
		err := dfs.rotate()
		if err != nil {
			return nil, err
		}
	}
	return h, nil
}

func (dfs *DataFiles) canRotate() bool {
	return dfs.active.off > dfs.segmentSize
}

type ActiveFile struct {
	fid       int
	fd        *os.File
	off       int64
	verifyCRC bool
}

func NewActiveFile(dir string, fid int, readOnly, verifyCRC bool) (af *ActiveFile, err error) {
	path := getFilePath(dir, fid)
	flag := os.O_CREATE | os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	}
	fd, err := os.OpenFile(path, flag, os.ModePerm)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	af = &ActiveFile{
		fd:        fd,
		off:       fi.Size(),
		fid:       fid,
		verifyCRC: verifyCRC,
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
	h = entity.NewHint().WithFid(af.fid).WithOff(af.off)
	af.off += e.Size()
	return h, nil
}

func (af *ActiveFile) ReadEntity(off int64, length int) (e *entity.Entry, err error) {
	return readEntry(af.fd, off, length, af.verifyCRC)
}

type OldFile struct {
	fd        *os.File
	verifyCRC bool
}

func NewOldFile(path string, verifyCRC bool) (of *OldFile, err error) {
	fd, err := os.OpenFile(path, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	of = &OldFile{fd: fd, verifyCRC: verifyCRC}
	return of, nil
}

func (of *OldFile) Close() error {
	return of.fd.Close()
}

func (of *OldFile) ReadEntity(off int64, length int) (e *entity.Entry, err error) {
	return readEntry(of.fd, off, length, of.verifyCRC)
}

func (of *OldFile) ReadEntityWithOutLength(off int64) (e *entity.Entry, err error) {
	e = entity.NewEntry().WithMeta(entity.NewMeta())
	metaBuf := make([]byte, entity.MetaSize)
	n, err := of.fd.ReadAt(metaBuf, off)
	if err != nil {
		return nil, err
	}
	if n < entity.MetaSize {
		return nil, ReadMissDataErr
	}
	payloadOff := off + entity.MetaSize
	e.DecodeMeta(metaBuf)
	payloadSize := e.Meta.KeySize + e.Meta.ValueSize
	payloadBuf := make([]byte, payloadSize)
	n, err = of.fd.ReadAt(payloadBuf, payloadOff)
	if err != nil {
		return nil, err
	}
	if n < int(payloadSize) {
		return nil, ReadMissDataErr
	}
	if of.verifyCRC {
		full := append(metaBuf, payloadBuf...)
		if !entity.VerifyRecordCRC(full) {
			return nil, CrcErr
		}
	}
	e.DecodePayload(payloadBuf)
	return e, nil
}

func readEntry(fd *os.File, off int64, length int, verifyCRC bool) (e *entity.Entry, err error) {
	buf := make([]byte, length)
	n, err := fd.ReadAt(buf, off)
	if n < length {
		return nil, ReadMissDataErr
	}
	if err != nil {
		return nil, err
	}
	if verifyCRC && !entity.VerifyRecordCRC(buf) {
		return nil, CrcErr
	}
	e = entity.NewEntry().WithMeta(entity.NewMeta())
	e.DecodeMeta(buf[:entity.MetaSize])
	e.DecodePayload(buf[entity.MetaSize:])
	return e, nil
}

// ListDataFileIDs returns numeric file IDs for all segment files in dir.
func ListDataFileIDs(dir string) (fids []int, err error) {
	return getFids(dir)
}

// DataFilePath returns the path to a segment file.
func DataFilePath(dir string, fid int) string {
	return getFilePath(dir, fid)
}

func getFids(dir string) (fids []int, err error) {
	files, err := os.ReadDir(dir)
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
	sort.Ints(fids)
	return fids, nil
}

func getFilePath(dir string, fid int) string {
	return fmt.Sprintf("%s/%d%s", dir, fid, FileSuffix)
}
