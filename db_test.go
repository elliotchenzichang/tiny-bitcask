package tiny_bitcask

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_Base(t *testing.T) {
	opt := &Options{Dir: "db"}
	db, err := NewDB(opt)
	assert.NoError(t, err)
	err = db.Set([]byte("test_key"), []byte("test_value"))
	assert.NoError(t, err)
	value, err := db.Get([]byte("test_key"))
	assert.NoError(t, err)
	assert.Equal(t, "test_value", string(value))

	err = db.Set([]byte("test_key"), []byte("test_value_2"))
	assert.NoError(t, err)

	value, err = db.Get([]byte("test_key"))
	assert.NoError(t, err)
	assert.Equal(t, "test_value_2", string(value))
}

func TestDB_SegmentSize(t *testing.T) {
	opt := &Options{
		Dir:         "db",
		SegmentSize: 4 * KB,
	}
	db, err := NewDB(opt)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		value := fmt.Sprintf("test_value_%d", i)
		err = db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}
}

func TestDB_Merge(t *testing.T) {
	dir := "db"
	opt := &Options{
		Dir:         dir,
		SegmentSize: 4 * KB,
	}
	db, err := NewDB(opt)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	key := "test_key"
	for i := 0; i < 1000; i++ {
		value := fmt.Sprintf("test_value_%d", i)
		err = db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}
	err = db.Merge()
	assert.NoError(t, err)

	value, err := db.Get([]byte("test_key"))
	assert.NoError(t, err)
	assert.Equal(t, "test_value_999", string(value))
}

func TestDB_Delete(t *testing.T) {
	opt := &Options{Dir: "db"}
	db, err := NewDB(opt)
	assert.NoError(t, err)
	err = db.Set([]byte("test_key"), []byte("test_value"))
	assert.NoError(t, err)
	value, err := db.Get([]byte("test_key"))

	assert.NoError(t, err)
	assert.Equal(t, "test_value", string(value))

	err = db.Delete([]byte("test_key"))
	assert.NoError(t, err)

	value, err = db.Get([]byte("test_key"))
	assert.Nil(t, value)
	assert.ErrorAs(t, KeyNotFoundErr, err)

}

func TestDB_Delete_Merge(t *testing.T) {
	opt := &Options{Dir: "db"}
	db, err := NewDB(opt)
	key := "test_key"
	for i := 0; i < 1000; i++ {
		value := fmt.Sprintf("test_value_%d", i)
		err = db.Set([]byte(key), []byte(value))
		assert.NoError(t, err)
	}
	err = db.Delete([]byte("test_key"))

	db.Merge()

	value, err := db.Get([]byte("test_key"))
	assert.Nil(t, value)
	assert.Equal(t, KeyNotFoundErr, err)
}
