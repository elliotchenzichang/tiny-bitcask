package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tiny-bitcask/entity"
)

func TestHintFile_WriteReadRoundtrip(t *testing.T) {
	tests := []struct {
		name          string
		key           []byte
		value         []byte
		wantKeySize   uint32
		wantValueSize uint32
		wantOffset    int64
	}{
		{
			name:          "ascii_key_value",
			key:           []byte("hello"),
			value:         []byte("world"),
			wantKeySize:   5,
			wantValueSize: 5,
			wantOffset:    0,
		},
		{
			name:          "empty_value",
			key:           []byte("k"),
			value:         []byte{},
			wantKeySize:   1,
			wantValueSize: 0,
			wantOffset:    0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			fid := 1
			datPath := getFilePath(dir, fid)
			f, err := os.OpenFile(datPath, os.O_CREATE|os.O_RDWR, 0o644)
			require.NoError(t, err)

			e := entity.NewEntryWithData(tt.key, tt.value)
			buf := e.Encode()
			_, err = f.WriteAt(buf, 0)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			require.NoError(t, WriteHintFileForDataFile(dir, fid, true))

			assert.True(t, HintFileExists(dir, fid))

			recs, err := ReadHintFile(dir, fid)
			require.NoError(t, err)
			require.Len(t, recs, 1)
			r := recs[0]
			assert.Equal(t, e.Meta.TimeStamp, r.Timestamp)
			assert.Equal(t, tt.wantKeySize, r.KeySize)
			assert.Equal(t, tt.wantValueSize, r.ValueSize)
			assert.Equal(t, tt.wantOffset, r.RecordOffset)
			assert.Equal(t, byte(0), r.Flag)
			assert.Equal(t, string(tt.key), string(r.Key))
		})
	}
}

func TestHintFile_SkipsTombstone(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, f *os.File) error
		wantLen int
		wantKey string
	}{
		{
			name: "live_then_tombstone",
			setup: func(t *testing.T, f *os.File) error {
				t.Helper()
				live := entity.NewEntryWithData([]byte("k1"), []byte("v1"))
				off := int64(0)
				if _, err := f.WriteAt(live.Encode(), off); err != nil {
					return err
				}
				off += live.Size()
				tomb := entity.NewTombstoneEntry([]byte("k2"))
				_, err := f.WriteAt(tomb.Encode(), off)
				return err
			},
			wantLen: 1,
			wantKey: "k1",
		},
		{
			name: "only_tombstone",
			setup: func(t *testing.T, f *os.File) error {
				t.Helper()
				tomb := entity.NewTombstoneEntry([]byte("k2"))
				_, err := f.WriteAt(tomb.Encode(), 0)
				return err
			},
			wantLen: 0,
			wantKey: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			fid := 2
			datPath := getFilePath(dir, fid)
			f, err := os.OpenFile(datPath, os.O_CREATE|os.O_RDWR, 0o644)
			require.NoError(t, err)
			require.NoError(t, tt.setup(t, f))
			require.NoError(t, f.Close())

			require.NoError(t, WriteHintFileForDataFile(dir, fid, true))
			recs, err := ReadHintFile(dir, fid)
			require.NoError(t, err)
			require.Len(t, recs, tt.wantLen)
			if tt.wantLen > 0 {
				assert.Equal(t, tt.wantKey, string(recs[0].Key))
			}
		})
	}
}

func TestHintFile_InvalidHeader(t *testing.T) {
	tests := []struct {
		name    string
		fid     int
		content []byte
	}{
		{
			name:    "wrong_magic",
			fid:     9,
			content: []byte("BAD!"),
		},
		{
			name:    "too_short",
			fid:     10,
			content: []byte("TBH"),
		},
		{
			name:    "bad_version",
			fid:     11,
			content: append([]byte("TBHK"), 0xFF, 0, 0, 0),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, filepath.Base(HintFilePath(dir, tt.fid)))
			require.NoError(t, os.WriteFile(p, tt.content, 0o644))
			_, err := ReadHintFile(dir, tt.fid)
			assert.ErrorIs(t, err, ErrInvalidHintFile)
		})
	}
}
