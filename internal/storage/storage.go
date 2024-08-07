package storage

import (
	"fmt"
	"os"
	"sync"
)

const (
	DefaultBlock = 4 << 10
)

type Encoder interface {
	Encode() []byte
}

type Option struct {
	BlockSize     uint16
	DataItemSize  uint16
	ContainerPath string
}

type SetStorage struct {
	// 只读
	readFd   *os.File
	data     *os.File
	wMu      sync.Mutex
	buffer   []byte
	dataSize int
	opt      *Option
}

func NewSetStorage(opt *Option) *SetStorage {
	if opt.BlockSize%1024 != 0 {
		panic("block size not 1024 multiple")
	}
	if opt.DataItemSize%32 != 0 {
		panic("data item size not 32 multiple")
	}
	if opt.BlockSize%opt.DataItemSize != 0 {
		panic("block size not DataItemSize multiple")
	}
	return &SetStorage{
		buffer: make([]byte, 0, opt.BlockSize),
	}
}

func (s *SetStorage) Write(e Encoder) error {
	if e == nil {
		return nil
	}
	bytes := e.Encode()
	if len(bytes) != s.dataSize {
		return fmt.Errorf("length fixed : %d", s.dataSize)
	}
	n, err := s.data.Write(bytes)
	if err != nil {
		return err
	}
	for len(bytes)-n > 0 {
		bytes = bytes[:n]
		n, err = s.data.Write(bytes)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SetStorage) Sync() error {
	return s.data.Sync()
}
