package tiny_bitcask

import (
	"os"
)

func isDirExist(dir string) (bool, error) {
	_, err := os.Stat(dir)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func getSegmentSize(size int64) int64 {
	var fileSize int64
	if size <= 0 {
		fileSize = DefaultSegmentSize
	} else {
		fileSize = size
	}
	return fileSize
}
