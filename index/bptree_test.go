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

	res1 := tree.Put([]byte("aaa"), &data.LogRecordPos{1, 222})
	assert.Nil(t, res1)
	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{1, 222})
	assert.Nil(t, res2)
	res3 := tree.Put([]byte("abc"), &data.LogRecordPos{14, 422})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(222))

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

	res1, ok1 := tree.Delete([]byte("not exist"))
	assert.Nil(t, res1)
	assert.False(t, ok1)

	tree.Put([]byte("ac"), &data.LogRecordPos{3, 9})
	tree.Put([]byte("abc"), &data.LogRecordPos{4341, 222})
	tree.Put([]byte("abc"), &data.LogRecordPos{131, 222})
	pos2 := tree.Get([]byte("abc"))
	t.Log(pos2)
	res2, ok2 := tree.Delete([]byte("abc"))
	assert.True(t, ok2)
	assert.NotNil(t, res2)
	assert.Equal(t, res2.Fid, uint32(131))
	assert.Equal(t, res2.Offset, int64(222))

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
