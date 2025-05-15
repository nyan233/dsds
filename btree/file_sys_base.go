//go:build !windows

package btree

import "os"

func fileAllocate(file *os.File, off, size int64) error {
	return nil
}

func createFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_DIRECT, 0644)
}
