package storage

import (
	"errors"
	"fmt"
	"os"
	"tiny-bitcask/entity"
)

var (
	ReadMissDataErr  = errors.New("miss data during read")
	WriteMissDataErr = errors.New("miss data during write")
	CrcErr           = errors.New("crc error")
	DeleteEntryErr   = errors.New("read an entry which had deleted")
)

const (
	FileSuffix = ".dat"
	B          = 1
	KB         = 1024 * B
	MB         = 1024 * KB
	GB         = 1024 * MB
)

type DataFile struct {
	Dir      string
	FileSize int64
	Af       *ActiveFile
	Fds      map[int]*os.File
}

func NewDataFile(dir string, size int64) (s *DataFile, err error) {
	err = os.Mkdir(dir, os.ModePerm)
	if err != nil {
		return nil, err
	}
	s = &DataFile{
		Dir:      dir,
		FileSize: size,
		Fds:      map[int]*os.File{},
	}
	s.Dir = dir
	s.Af = &ActiveFile{
		Fid: 0,
		Off: 0,
	}
	path := s.getPath()
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	s.Af.F = fd
	s.Fds[0] = fd
	return s, nil
}

type ActiveFile struct {
	Fid int
	F   *os.File
	Off int64
}

func (s *DataFile) ReadEntry(fid int, off int64) (e *entity.Entry, err error) {
	buf := make([]byte, entity.MetaSize)
	err = s.readAt(fid, off, buf)
	if err != nil {
		return nil, err
	}
	e = entity.NewEntry()
	e.DecodeMeta(buf)
	if e.Meta.Flag == entity.DeleteFlag {
		return nil, DeleteEntryErr
	}
	off += entity.MetaSize
	payloadSize := e.Meta.KeySize + e.Meta.ValueSize
	payload := make([]byte, payloadSize)
	err = s.readAt(fid, off, payload)
	if err != nil {
		return nil, err
	}
	err = e.DecodePayload(payload)
	if err != nil {
		return nil, err
	}
	crc := e.GetCrc(buf)
	if e.Meta.Crc != crc {
		return nil, CrcErr
	}
	return e, nil
}

func (s *DataFile) ReadFullEntry(fid int, off int64, buf []byte) (e *entity.Entry, err error) {
	err = s.readAt(fid, off, buf)
	if err != nil {
		return nil, err
	}
	e = entity.NewEntry()
	e.DecodeMeta(buf[0:entity.MetaSize])
	payloadSize := e.Meta.KeySize + e.Meta.KeySize
	err = e.DecodePayload(buf[entity.MetaSize : entity.MetaSize+payloadSize])
	if err != nil {
		return nil, err
	}
	crc := e.GetCrc(buf[:entity.MetaSize])
	if e.Meta.Crc != crc {
		return nil, CrcErr
	}
	return e, nil
}

func (s *DataFile) readAt(fid int, off int64, bytes []byte) (err error) {
	if fd := s.Fds[fid]; fd != nil {
		n, err := fd.ReadAt(bytes, off)
		if err != nil {
			return err
		}
		if n < len(bytes) {
			return ReadMissDataErr
		}
		return nil
	}
	path := fmt.Sprintf("%s/%d", s.Dir, fid)
	fd, err := os.OpenFile(path, os.O_RDWR, os.ModePerm)
	s.Fds[fid] = fd
	n, err := fd.ReadAt(bytes, off)
	if err != nil {
		return err
	}
	if n < len(bytes) {
		return ReadMissDataErr
	}
	return nil
}

func (s *DataFile) WriteAt(bytes []byte) (fid int, off int64, err error) {
	err = s.Af.writeAt(bytes)
	if err != nil {
		return 0, 0, err
	}
	fid, off = s.Af.Fid, s.Af.Off
	s.Af.Off += int64(len(bytes))
	if s.Af.Off >= s.FileSize {
		err := s.rotate()
		if err != nil {
			return 0, 0, err
		}
	}
	return fid, off, nil
}

func (af *ActiveFile) writeAt(bytes []byte) error {
	n, err := af.F.WriteAt(bytes, af.Off)
	if n < len(bytes) {
		return WriteMissDataErr
	}
	return err
}

func (s *DataFile) rotate() error {
	af := &ActiveFile{
		Fid: s.Af.Fid + 1,
		Off: 0,
	}
	fd, err := os.OpenFile(s.getPath(), os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	af.F = fd
	s.Fds[af.Fid] = fd
	s.Af = af
	return nil
}

func (s *DataFile) getPath() string {
	path := fmt.Sprintf("%s/%d%s", s.Dir, s.Af.Fid, FileSuffix)
	return path
}
