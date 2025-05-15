package btree

import (
	"fmt"
	"golang.org/x/sys/windows"
	"os"
	"syscall"
)

func fileAllocate(file *os.File, off, size int64) error {
	handle := syscall.Handle(file.Fd())

	// 设置文件指针到目标大小
	// SetFilePointer 需要拆分 int64 为高低 32 位
	low := int32(size & 0xFFFFFFFF)
	high := int32(size >> 32)
	_, err := syscall.SetFilePointer(handle, low, &high, syscall.FILE_BEGIN)
	if err != nil {
		return fmt.Errorf("SetFilePointer failed: %v", err)
	}

	// 设置文件末尾
	err = syscall.SetEndOfFile(handle)
	if err != nil {
		return fmt.Errorf("SetEndOfFile failed: %v", err)
	}

	// 将指针移回文件开头（可选，视后续操作）
	_, err = syscall.SetFilePointer(handle, 0, nil, syscall.FILE_BEGIN)
	if err != nil {
		return fmt.Errorf("Reset file pointer failed: %v", err)
	}
	return file.Sync()
}

func LockFile(file *os.File) error {
	return nil
}

func UnlockFile(file *os.File) error {
	return nil
}

func createFile(path string) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	fileHd, err := windows.CreateFile(
		p,
		windows.GENERIC_READ|windows.GENERIC_WRITE, // 自己可读写
		windows.FILE_SHARE_READ,                    // 允许其它进程读取
		nil,
		windows.OPEN_ALWAYS, // 文件存在则打开, 不存在则创建
		windows.FILE_FLAG_NO_BUFFERING|windows.FILE_FLAG_WRITE_THROUGH, // 类似linux O_DIRECT
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fileHd), path), nil
}
