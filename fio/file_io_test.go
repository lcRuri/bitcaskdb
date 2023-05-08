package fioo

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIOManager(t *testing.T) {
	path := filepath.Join("/Users/yefeixiang/coding/kv-projects/bitcask-go/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/Users/yefeixiang/coding/kv-projects/bitcask-go/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	t.Log(n, err)
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	t.Log(n, err)
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/Users/yefeixiang/coding/kv-projects/bitcask-go/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n1, err := fio.Read(b1, 0)
	t.Log(string(b1), n1)
	assert.Equal(t, 5, n1)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n2, err := fio.Read(b2, 5)
	t.Log(string(b2), n2)
	assert.Equal(t, 5, n2)
	assert.Equal(t, []byte("key-b"), b2)

}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/Users/yefeixiang/coding/kv-projects/bitcask-go/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/Users/yefeixiang/coding/kv-projects/bitcask-go/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}