package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

// MMap IO 内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt //只能用来读取数据
}

func NewMMapIOManager(filename string) (*MMap, error) {
	//映射文件不存在，先创建
	_, err := os.OpenFile(filename, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}

	//将文件映射到虚拟地址空间当中
	readerAt, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (mmap *MMap) Read(b []byte, offset int64) (int, error) {
	return mmap.readerAt.ReadAt(b, offset)
}

// Write 写入字节数组到文件中
func (mmap *MMap) Write(b []byte) (int, error) {
	panic("not implemented")
}

// Sync 持久化数据
func (mmap *MMap) Sync() error {
	panic("not implemented")
}

// Close 关闭文件
func (mmap *MMap) Close() error {
	return mmap.readerAt.Close()
}

// Size 获取文件大小
func (mmap *MMap) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
