package tiny_bitcask

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

func getFids(dir string) (fids []int, err error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fileName := f.Name()
		filePath := path.Base(fileName)
		if path.Ext(filePath) == fileSuffix {
			filePrefix := strings.TrimSuffix(filePath, fileSuffix)
			fid, err := strconv.Atoi(filePrefix)
			if err != nil {
				return nil, err
			}
			fids = append(fids, fid)
		}
	}
	return fids, nil
}

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
