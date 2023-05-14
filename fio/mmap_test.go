package fio

import (
	"github.com/stretchr/testify/assert"
	"io"
	"path/filepath"
	"testing"
)

func TestMMap_Read(t *testing.T) {
	//filename := filepath.Join("/Users/yefeixiang/coding/kv-projects/bitcask-go/tmp", "mmap-1.data")
	filename := filepath.Join("/tmp", "mmap-1.data")
	defer destroyFile(filename)
	t.Log(filename)

	//文件为空的情况
	mmapIO, err := NewMMapIOManager(filename)
	assert.Nil(t, err)

	b1 := make([]byte, 10)
	n1, err := mmapIO.Read(b1, 0)
	//t.Log(n1)
	//t.Log(err)
	assert.Equal(t, 0, n1)
	assert.Equal(t, io.EOF, err)

	//有文件的情况
	fio, err := NewFileIOManager(filename)
	assert.Nil(t, err)
	_, err = fio.Write([]byte("aa"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("bb"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("cc"))
	assert.Nil(t, err)

	mmapIO2, err := NewMMapIOManager(filename)
	assert.Nil(t, err)
	t.Log(mmapIO2.Size())

	b2 := make([]byte, 2)
	n2, err := mmapIO2.Read(b2, 0)
	t.Log(n2)
	t.Log(err)
	t.Log(string(b2))

	//mmapIO2, err := NewMMapIOManager(path)
	//assert.Nil(t, err)
	//size, err := mmapIO2.Size()
	//assert.Nil(t, err)
	//assert.Equal(t, int64(6), size)
	//
	//b2 := make([]byte, 2)
	//n2, err := mmapIO2.Read(b2, 0)
	//t.Log(n2)
	//t.Log(err)
	//t.Log(string(b2))
	//assert.Nil(t, err)
	//assert.Equal(t, 2, n2)
}
