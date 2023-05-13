package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join("/tmp")
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aaa"), &data.LogRecordPos{1, 222})
	tree.Put([]byte("abc"), &data.LogRecordPos{1, 222})
	tree.Put([]byte("ccc"), &data.LogRecordPos{1, 222})

}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join("/tmp")
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)
	t.Log(pos)

	tree.Put([]byte("ac"), &data.LogRecordPos{3, 9})
	tree.Put([]byte("abc"), &data.LogRecordPos{4341, 222})
	tree.Put([]byte("abc"), &data.LogRecordPos{131, 222})
	pos1 := tree.Get([]byte("ac"))
	t.Log(pos1)
	pos2 := tree.Get([]byte("abc"))
	t.Log(pos2)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join("/tmp")
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)
	t.Log(pos)

	tree.Put([]byte("ac"), &data.LogRecordPos{3, 9})
	tree.Put([]byte("abc"), &data.LogRecordPos{4341, 222})
	tree.Put([]byte("abc"), &data.LogRecordPos{131, 222})
	pos2 := tree.Get([]byte("abc"))
	t.Log(pos2)
	tree.Delete([]byte("abc"))
	pos1 := tree.Get([]byte("ac"))
	t.Log(pos1)
	pos2 = tree.Get([]byte("abc"))
	t.Log(pos2)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join("/tmp")
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path, false)

	size1 := tree.Size()
	t.Log(size1)
	tree.Put([]byte("ac"), &data.LogRecordPos{3, 9})

	//size2 := tree.Size()
	//t.Log(size2)

	tree.Put([]byte("abc"), &data.LogRecordPos{4341, 222})
	tree.Put([]byte("abc"), &data.LogRecordPos{131, 222})

	size3 := tree.Size()
	t.Log(size3)

}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join("/tmp")
	defer func() {
		_ = os.RemoveAll(path)

	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("aaa"), &data.LogRecordPos{1, 222})
	tree.Put([]byte("abc"), &data.LogRecordPos{21, 322})

	bpi := tree.Iterator(false)

	t.Log(string(bpi.Key()))
	t.Log(bpi.Value())

	for bpi.Rewind(); bpi.Valid(); bpi.Next() {
		t.Log(string(bpi.Key()))
		t.Log(bpi.Value())
	}

}
