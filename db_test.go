package tiny_bitcask

import (
	"fmt"
	"testing"
)

func TestDB_Base(t *testing.T) {
	opt := &Options{Dir: "db"}
	db, err := NewDB(opt)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = db.Set([]byte("test_key"), []byte("test_value"))
	if err != nil {
		fmt.Println(err)
		return
	}
	value, err := db.Get([]byte("test_key"))

	err = db.Set([]byte("test_key"), []byte("test_value_2"))
	if err != nil {
		fmt.Println(err)
		return
	}

	value, err = db.Get([]byte("test_key"))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(value))
}

func TestDB_SegmentSize(t *testing.T) {
	opt := &Options{
		Dir:         "db",
		SegmentSize: 4 * KB,
	}
	db, err := NewDB(opt)
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		value := fmt.Sprintf("test_value_%d", i)
		err = db.Set([]byte(key), []byte(value))
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}
