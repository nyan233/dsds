package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	freelistAllocNewPageOp byte = iota + 1
	freelistFreePage
	defaultMaxAllocLogSize = 1024 * 16
)

type diskAllocLog struct {
	op     byte
	pageId uint32
}

func (d *diskAllocLog) Marshal() []byte {
	buf := make([]byte, 0, 5)
	buf = append(buf, d.op)
	buf = binary.BigEndian.AppendUint32(buf, d.pageId)
	return buf
}

func (d *diskAllocLog) Unmarshal(buf []byte) error {
	if len(buf) < 5 {
		return fmt.Errorf("buf size too small : %d", len(buf))
	}
	d.op = buf[0]
	d.pageId = binary.BigEndian.Uint32(buf[1:5])
	return nil
}

type diskAllocLogger struct {
	file    *os.File
	offset  int64
	handler func(logs []diskAllocLog) error
}

func newDiskAllocLogger(file *os.File, fn func(logs []diskAllocLog) error) *diskAllocLogger {
	return &diskAllocLogger{
		file:    file,
		handler: fn,
	}
}

func (p *diskAllocLogger) init() error {
	var (
		fileData []byte
		logs     []diskAllocLog
	)
	fileData, err := io.ReadAll(p.file)
	if err != nil {
		return err
	}
	for i := 0; i < len(fileData); i += 5 {
		log := diskAllocLog{}
		err := log.Unmarshal(fileData[i : i+5])
		if err != nil {
			return err
		}
		logs = append(logs, log)
	}
	if len(logs) <= 0 {
		return nil
	}
	err = p.handler(logs)
	if err != nil {
		return err
	}
	return p.file.Truncate(0)
}

func (p *diskAllocLogger) commit() error {
	// no-restart not require log item
	err := p.handler(nil)
	if err != nil {
		return err
	}
	p.offset = 0
	err = p.file.Truncate(0)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (p *diskAllocLogger) writeLog(log diskAllocLog) error {
	return p.writeLogs([]diskAllocLog{log})
}

func (p *diskAllocLogger) writeLogs(logs []diskAllocLog) error {
	var buf bytes.Buffer
	for _, log := range logs {
		buf.Write(log.Marshal())
	}
	stat, err := p.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size()+int64(buf.Len()) > defaultMaxAllocLogSize {
		err = p.commit()
		if err != nil {
			return err
		}
	}
	write, err := p.file.WriteAt(buf.Bytes(), p.offset)
	if err != nil {
		return err
	}
	if write != buf.Len() {
		return fmt.Errorf("write %d bytes but only %d bytes written", write, buf.Len())
	} else {
		p.offset += int64(write)
		return nil
	}
}
