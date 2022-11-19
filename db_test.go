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
	fmt.Println(string(value))
}
