package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"tiny-bitcask"
)

func main() {
	parent, err := os.MkdirTemp("", "tb")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(parent)
	// NewDB creates the data directory when it does not exist; an empty temp dir
	// would otherwise be treated as an existing store and recovery would fail.
	dir := filepath.Join(parent, "data")

	o := *tiny_bitcask.DefaultOptions
	o.Dir = dir
	db, err := tiny_bitcask.NewDB(&o)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		log.Fatal(err)
	}
	v, err := db.Get([]byte("k"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(v))
}
