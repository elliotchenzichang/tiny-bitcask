package storage

import (
	"os"
	"path/filepath"
	"testing"

	"tiny-bitcask/entity"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHintFilePath(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		fid  int
		want string
	}{
		{name: "simple", dir: "/tmp/db", fid: 3, want: "/tmp/db/3.hint"},
		{name: "fid_zero", dir: "/x", fid: 0, want: "/x/0.hint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HintFilePath(tt.dir, tt.fid))
		})
	}
}

func TestWriteHintFileForDataFile_ReadHintFile_RoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		verifyCRC bool
	}{
		{name: "crc_on", verifyCRC: true},
		{name: "crc_off", verifyCRC: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			datPath := filepath.Join(dir, "1.dat")
			f, err := os.OpenFile(datPath, os.O_CREATE|os.O_RDWR, 0o644)
			require.NoError(t, err)

			e1 := entity.NewEntryWithData([]byte("a"), []byte("1"))
			e2 := entity.NewEntryWithData([]byte("b"), []byte("2"))
			_, err = f.WriteAt(e1.Encode(), 0)
			require.NoError(t, err)
			off2 := e1.Size()
			_, err = f.WriteAt(e2.Encode(), off2)
			require.NoError(t, err)
			require.NoError(t, f.Close())

			require.NoError(t, WriteHintFileForDataFile(dir, 1, tt.verifyCRC))

			recs, err := ReadHintFile(dir, 1)
			require.NoError(t, err)
			require.Len(t, recs, 2)
			assert.Equal(t, int64(0), recs[0].RecordOffset)
			assert.Equal(t, []byte("a"), recs[0].Key)
			assert.Equal(t, int64(off2), recs[1].RecordOffset)
			assert.Equal(t, []byte("b"), recs[1].Key)
		})
	}
}

func TestWriteHintFileForDataFile_SkipsTombstones(t *testing.T) {
	dir := t.TempDir()
	datPath := filepath.Join(dir, "2.dat")
	f, err := os.OpenFile(datPath, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	eLive := entity.NewEntryWithData([]byte("keep"), []byte("v"))
	eDel := entity.NewTombstoneEntry([]byte("gone"))
	_, err = f.WriteAt(eLive.Encode(), 0)
	require.NoError(t, err)
	off2 := eLive.Size()
	_, err = f.WriteAt(eDel.Encode(), off2)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	require.NoError(t, WriteHintFileForDataFile(dir, 2, true))
	recs, err := ReadHintFile(dir, 2)
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, []byte("keep"), recs[0].Key)
}

func TestReadHintFile_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		content []byte
		wantErr error
	}{
		{
			name:    "too_short",
			content: []byte("TB"),
			wantErr: ErrInvalidHintFile,
		},
		{
			name:    "bad_magic",
			content: append([]byte("XXXX"), make([]byte, 4)...),
			wantErr: ErrInvalidHintFile,
		},
		{
			name:    "truncated_record",
			content: append([]byte("TBHK\x01\x00\x00\x00\x00"), []byte{1, 2, 3}...),
			wantErr: ErrInvalidHintFile,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			p := filepath.Join(dir, "9.hint")
			require.NoError(t, os.WriteFile(p, tt.content, 0o644))
			_, err := ReadHintFile(dir, 9)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestHintFileExists_RemoveHintFile(t *testing.T) {
	tests := []struct {
		name     string
		setup    func(dir string) error
		wantEx   bool
		afterRm  bool
	}{
		{
			name: "exists_then_removed",
			setup: func(dir string) error {
				return os.WriteFile(filepath.Join(dir, "5.hint"), []byte("TBHK\x01\x00\x00\x00\x00"), 0o644)
			},
			wantEx:  true,
			afterRm: false,
		},
		{
			name: "missing",
			setup: func(dir string) error {
				return nil
			},
			wantEx:  false,
			afterRm: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, tt.setup(dir))
			assert.Equal(t, tt.wantEx, HintFileExists(dir, 5))
			RemoveHintFile(dir, 5)
			assert.Equal(t, tt.afterRm, HintFileExists(dir, 5))
		})
	}
}
