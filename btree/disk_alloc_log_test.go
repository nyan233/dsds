package btree

import (
	"os"
	"testing"
)

func TestDiskAllocLog(t *testing.T) {
	file, err := os.OpenFile("test/btnode.log", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	logger := newDiskAllocLogger(file, func(logs []diskAllocLog) error {
		t.Log("commit logs")
		return nil
	})
	err = logger.init()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1<<16; i++ {
		err = logger.writeLog(diskAllocLog{
			op:     freelistAllocNewPageOp,
			pageId: 128,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
