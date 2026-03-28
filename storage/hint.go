package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"tiny-bitcask/entity"
)

const (
	hintMagic     = "TBHK"
	hintVersion   = byte(1)
	hintHeaderLen = 8
)

var (
	ErrInvalidHintFile = errors.New("storage: invalid or unsupported hint file")
)

// HintRecord is one row in a .hint file (compact keydir metadata for a segment).
type HintRecord struct {
	Timestamp    uint64
	KeySize      uint32
	ValueSize    uint32
	RecordOffset int64
	Flag         uint8
	Key          []byte
}

// HintFilePath returns the path to the hint file for segment fid.
func HintFilePath(dir string, fid int) string {
	return fmt.Sprintf("%s/%d.hint", dir, fid)
}

// WriteHintFileForDataFile scans a sealed .dat file and writes a companion .hint file
// (timestamp, sizes, record offset, flag, key only — no values).
func WriteHintFileForDataFile(dir string, fid int, verifyCRC bool) error {
	datPath := getFilePath(dir, fid)
	of, err := NewOldFile(datPath, verifyCRC)
	if err != nil {
		return err
	}
	defer of.Close()

	tmpPath := HintFilePath(dir, fid) + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	header := make([]byte, hintHeaderLen)
	copy(header[0:4], hintMagic)
	header[4] = hintVersion
	if _, err := f.Write(header); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	var off int64
	for {
		entry, err := of.ReadEntityWithOutLength(off)
		if err != nil {
			if err == io.EOF {
				break
			}
			f.Close()
			os.Remove(tmpPath)
			return err
		}
		recOff := off
		off += entry.Size()

		if entry.Meta.Flag == entity.DeleteFlag {
			continue
		}

		rec := make([]byte, 25+len(entry.Key))
		binary.LittleEndian.PutUint64(rec[0:8], entry.Meta.TimeStamp)
		binary.LittleEndian.PutUint32(rec[8:12], entry.Meta.KeySize)
		binary.LittleEndian.PutUint32(rec[12:16], entry.Meta.ValueSize)
		binary.LittleEndian.PutUint64(rec[16:24], uint64(recOff))
		rec[24] = entry.Meta.Flag
		copy(rec[25:], entry.Key)
		if _, err := f.Write(rec); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	hintPath := HintFilePath(dir, fid)
	if err := os.Rename(tmpPath, hintPath); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}

// ReadHintFile reads and parses a .hint file. Caller must validate it matches the .dat.
func ReadHintFile(dir string, fid int) ([]HintRecord, error) {
	p := HintFilePath(dir, fid)
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(hintHeaderLen) {
		return nil, ErrInvalidHintFile
	}

	header := make([]byte, hintHeaderLen)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	if string(header[0:4]) != hintMagic || header[4] != hintVersion {
		return nil, ErrInvalidHintFile
	}

	var out []HintRecord
	for {
		fixed := make([]byte, 25)
		_, err := io.ReadFull(f, fixed)
		if err == io.EOF {
			break
		}
		if err != nil {
			if err == io.ErrUnexpectedEOF {
				return nil, ErrInvalidHintFile
			}
			return nil, err
		}
		ts := binary.LittleEndian.Uint64(fixed[0:8])
		ks := binary.LittleEndian.Uint32(fixed[8:12])
		vs := binary.LittleEndian.Uint32(fixed[12:16])
		recOff := int64(binary.LittleEndian.Uint64(fixed[16:24]))
		flag := fixed[24]

		key := make([]byte, ks)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil, ErrInvalidHintFile
		}
		out = append(out, HintRecord{
			Timestamp:    ts,
			KeySize:      ks,
			ValueSize:    vs,
			RecordOffset: recOff,
			Flag:         flag,
			Key:          key,
		})
	}
	return out, nil
}

// HintFileExists reports whether a hint file is present for the segment.
func HintFileExists(dir string, fid int) bool {
	st, err := os.Stat(HintFilePath(dir, fid))
	return err == nil && !st.IsDir()
}

// RemoveHintFile removes the hint file for a segment if it exists.
func RemoveHintFile(dir string, fid int) {
	_ = os.Remove(HintFilePath(dir, fid))
}
