package tiny_bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"tiny-bitcask/storage"
)

// newTestDB opens a DB under an isolated t.TempDir() with optional option overrides.
func newTestDB(t *testing.T, customize func(*Options)) *DB {
	t.Helper()
	opt := *DefaultOptions
	// Use a non-existent subdir: t.TempDir() exists but is empty, which would
	// incorrectly trigger recovery in NewDB on an empty store.
	opt.Dir = filepath.Join(t.TempDir(), "db")
	if customize != nil {
		customize(&opt)
	}
	db, err := NewDB(&opt)
	assert.NoError(t, err)
	return db
}

func TestDB_SetGetUpdate(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, db *DB)
	}{
		{
			name: "read_after_write",
			run: func(t *testing.T, db *DB) {
				assert.NoError(t, db.Set([]byte("test_key"), []byte("test_value")))
				value, err := db.Get([]byte("test_key"))
				assert.NoError(t, err)
				assert.Equal(t, "test_value", string(value))
			},
		},
		{
			name: "update_overwrites",
			run: func(t *testing.T, db *DB) {
				assert.NoError(t, db.Set([]byte("test_key"), []byte("test_value")))
				assert.NoError(t, db.Set([]byte("test_key"), []byte("test_value_2")))
				value, err := db.Get([]byte("test_key"))
				assert.NoError(t, err)
				assert.Equal(t, "test_value_2", string(value))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t, nil)
			tt.run(t, db)
		})
	}
}

func TestDB_SegmentSize(t *testing.T) {
	tests := []struct {
		name        string
		keys        int
		segmentSize int64
	}{
		{
			name:        "many_keys_small_segment",
			keys:        1000,
			segmentSize: 4 * storage.KB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t, func(o *Options) {
				o.SegmentSize = tt.segmentSize
			})
			for i := 0; i < tt.keys; i++ {
				key := fmt.Sprintf("test_key_%d", i)
				value := fmt.Sprintf("test_value_%d", i)
				assert.NoError(t, db.Set([]byte(key), []byte(value)))
			}
		})
	}
}

func TestDB_Merge(t *testing.T) {
	tests := []struct {
		name        string
		updates     int
		segmentSize int64
		want        string
	}{
		{
			name:        "repeated_key_compacted_to_latest",
			updates:     1000,
			segmentSize: 4 * storage.KB,
			want:        "test_value_999",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t, func(o *Options) {
				o.SegmentSize = tt.segmentSize
			})
			key := []byte("test_key")
			for i := 0; i < tt.updates; i++ {
				value := fmt.Sprintf("test_value_%d", i)
				assert.NoError(t, db.Set(key, []byte(value)))
			}
			assert.NoError(t, db.Merge())
			value, err := db.Get(key)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, string(value))
		})
	}
}

// TestDB_Merge_LiveKeyOnlyInOldSegment checks that merge copies a record whose
// keydir entry still points at an old segment (not the active file) before
// that segment is removed.
func TestDB_Merge_LiveKeyOnlyInOldSegment(t *testing.T) {
	db := newTestDB(t, func(o *Options) {
		o.SegmentSize = 4 * storage.KB
	})
	staleKey := []byte("written_once_then_idle")
	assert.NoError(t, db.Set(staleKey, []byte("keep_me")))

	busyKey := []byte("busy")
	for i := 0; i < 800; i++ {
		assert.NoError(t, db.Set(busyKey, []byte(fmt.Sprintf("v_%d", i))))
	}

	assert.NoError(t, db.Merge())

	got, err := db.Get(staleKey)
	assert.NoError(t, err)
	assert.Equal(t, "keep_me", string(got))
}

func TestDB_Recovery(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "reopen_loads_existing_data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "data")
			opt := *DefaultOptions
			opt.Dir = dataDir

			db1, err := NewDB(&opt)
			assert.NoError(t, err)
			assert.NoError(t, db1.Set([]byte("k"), []byte("v")))
			assert.NoError(t, db1.Close())

			db2, err := NewDB(&opt)
			assert.NoError(t, err)
			v, err := db2.Get([]byte("k"))
			assert.NoError(t, err)
			assert.Equal(t, "v", string(v))
		})
	}
}

// TestDB_Recovery_UsesHintFile checks sealed segments get a .hint on rotation and
// recovery rebuilds the keydir from hints (active segment still full-scanned).
func TestDB_Recovery_UsesHintFile(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "hintdb")
	opt := *DefaultOptions
	opt.Dir = dataDir
	opt.SegmentSize = 4 * storage.KB

	db1, err := NewDB(&opt)
	require.NoError(t, err)
	// Fill segment 1 then rotate into segment 2.
	busy := []byte("busy")
	for i := 0; i < 800; i++ {
		require.NoError(t, db1.Set(busy, []byte(fmt.Sprintf("v_%d", i))))
	}
	staleKey := []byte("only_in_segment_1")
	require.NoError(t, db1.Set(staleKey, []byte("pinned")))

	assert.True(t, storage.HintFileExists(dataDir, 1), "rotation should write 1.hint")

	require.NoError(t, db1.Close())

	db2, err := NewDB(&opt)
	require.NoError(t, err)
	got, err := db2.Get(staleKey)
	require.NoError(t, err)
	assert.Equal(t, "pinned", string(got))
}

// TestDB_Recovery_InvalidHintFallsBackToScan ensures a corrupt hint does not brick recovery.
// TestDB_Recovery_OldFileIDsEnableMerge checks that after reopening a store with
// multiple segment files, GetOldFiles is populated so Merge does not return
// NoNeedToMergeErr until new rotations (gap fixed: recovery lists sealed IDs).
func TestDB_Recovery_OldFileIDsEnableMerge(t *testing.T) {
	tests := []struct {
		name        string
		writes      int
		segmentSize int64
		wantValue   string
	}{
		{
			name:        "merge_runs_after_reopen_multi_segment",
			writes:      2500,
			segmentSize: 4 * storage.KB,
			wantValue:   "v_2499",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "merge_recovery")
			opt := *DefaultOptions
			opt.Dir = dataDir
			opt.SegmentSize = tt.segmentSize

			db1, err := NewDB(&opt)
			require.NoError(t, err)
			key := []byte("merge_key")
			for i := 0; i < tt.writes; i++ {
				require.NoError(t, db1.Set(key, []byte(fmt.Sprintf("v_%d", i))))
			}

			require.NoError(t, db1.Close())

			db2, err := NewDB(&opt)
			require.NoError(t, err)
			err = db2.Merge()
			require.NoError(t, err, "merge should run after recovery when multiple sealed segments exist")

			got, err := db2.Get(key)
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, string(got))
		})
	}
}

func TestDB_Recovery_InvalidHintFallsBackToScan(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "badhint")
	opt := *DefaultOptions
	opt.Dir = dataDir
	opt.SegmentSize = 4 * storage.KB

	db1, err := NewDB(&opt)
	require.NoError(t, err)
	busy := []byte("busy")
	for i := 0; i < 800; i++ {
		require.NoError(t, db1.Set(busy, []byte(fmt.Sprintf("v_%d", i))))
	}
	require.NoError(t, db1.Set([]byte("keep"), []byte("me")))

	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "1.hint"), []byte("TBHK\x01\x00\x00\x00\x00trunc"), 0o644))

	require.NoError(t, db1.Close())

	db2, err := NewDB(&opt)
	require.NoError(t, err)
	got, err := db2.Get([]byte("keep"))
	require.NoError(t, err)
	assert.Equal(t, "me", string(got))
}

func TestDB_Delete(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, db *DB)
	}{
		{
			name: "get_returns_not_found_after_delete",
			run: func(t *testing.T, db *DB) {
				assert.NoError(t, db.Set([]byte("test_key"), []byte("test_value")))
				value, err := db.Get([]byte("test_key"))
				assert.NoError(t, err)
				assert.Equal(t, "test_value", string(value))

				assert.NoError(t, db.Delete([]byte("test_key")))

				value, err = db.Get([]byte("test_key"))
				assert.Nil(t, value)
				assert.ErrorIs(t, err, KeyNotFoundErr)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t, nil)
			tt.run(t, db)
		})
	}
}

func TestDB_Delete_Merge(t *testing.T) {
	tests := []struct {
		name        string
		updates     int
		segmentSize int64
	}{
		{
			name:        "deleted_key_stays_absent_after_merge",
			updates:     1000,
			segmentSize: 4 * storage.KB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t, func(o *Options) {
				o.SegmentSize = tt.segmentSize
			})
			key := []byte("test_key")
			for i := 0; i < tt.updates; i++ {
				value := fmt.Sprintf("test_value_%d", i)
				assert.NoError(t, db.Set(key, []byte(value)))
			}
			assert.NoError(t, db.Delete(key))
			assert.NoError(t, db.Merge())

			value, err := db.Get(key)
			assert.Nil(t, value)
			assert.ErrorIs(t, err, KeyNotFoundErr)
		})
	}
}

func TestDB_Recovery_TombstoneRemovesKey(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "delete_then_reopen"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "tombdb")
			opt := *DefaultOptions
			opt.Dir = dataDir
			db1, err := NewDB(&opt)
			require.NoError(t, err)
			require.NoError(t, db1.Set([]byte("k"), []byte("v")))
			require.NoError(t, db1.Delete([]byte("k")))
			require.NoError(t, db1.Close())

			db2, err := NewDB(&opt)
			require.NoError(t, err)
			defer db2.Close()
			_, err = db2.Get([]byte("k"))
			assert.ErrorIs(t, err, KeyNotFoundErr)
		})
	}
}

func TestDB_Get_CRCCorrupt(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "read_returns_crc_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "crcdb")
			opt := *DefaultOptions
			opt.Dir = dataDir
			db, err := NewDB(&opt)
			require.NoError(t, err)
			require.NoError(t, db.Set([]byte("k"), []byte("v")))
			_, err = db.Get([]byte("k"))
			require.NoError(t, err)

			dat := filepath.Join(dataDir, "1.dat")
			b, err := os.ReadFile(dat)
			require.NoError(t, err)
			require.NotEmpty(t, b)
			b[len(b)-1] ^= 0xFF
			require.NoError(t, os.WriteFile(dat, b, 0o644))

			_, err = db.Get([]byte("k"))
			assert.ErrorIs(t, err, storage.CrcErr)
			require.NoError(t, db.Close())
		})
	}
}

func TestDB_ListKeys_Fold(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "sorted_keys_and_fold"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := newTestDB(t, nil)
			require.NoError(t, db.Set([]byte("b"), []byte("2")))
			require.NoError(t, db.Set([]byte("a"), []byte("1")))

			keys := db.ListKeys()
			require.Len(t, keys, 2)
			assert.Equal(t, "a", string(keys[0]))
			assert.Equal(t, "b", string(keys[1]))

			var sum string
			err := db.Fold(func(k, v []byte) error {
				sum += string(k) + "=" + string(v) + ";"
				return nil
			})
			require.NoError(t, err)
			assert.Contains(t, sum, "a=1")
			assert.Contains(t, sum, "b=2")
		})
	}
}

func TestDB_ReadOnly(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "get_ok_writes_rejected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataDir := filepath.Join(t.TempDir(), "rodb")
			opt := *DefaultOptions
			opt.Dir = dataDir
			db1, err := NewDB(&opt)
			require.NoError(t, err)
			require.NoError(t, db1.Set([]byte("k"), []byte("v")))
			require.NoError(t, db1.Close())

			ro := *DefaultOptions
			ro.Dir = dataDir
			ro.ReadOnly = true
			db2, err := NewDB(&ro)
			require.NoError(t, err)
			defer db2.Close()

			v, err := db2.Get([]byte("k"))
			require.NoError(t, err)
			assert.Equal(t, "v", string(v))

			assert.ErrorIs(t, db2.Set([]byte("x"), []byte("y")), ReadOnlyDBErr)
			assert.ErrorIs(t, db2.Delete([]byte("k")), ReadOnlyDBErr)
			assert.ErrorIs(t, db2.Merge(), ReadOnlyDBErr)
		})
	}
}
