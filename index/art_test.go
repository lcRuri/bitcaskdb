package index

import (
	"bitcask-go/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAdaptiveRadix_Put(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})

}

func TestAdaptiveRadix_Get(t *testing.T) {
	art := NewART()
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	//t.Log(pos)
	assert.NotNil(t, pos)

	pos1 := art.Get([]byte("not exist"))
	assert.Nil(t, pos1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1123, Offset: 990})
	pos2 := art.Get([]byte("key-1"))
	//t.Log(pos2)
	assert.NotNil(t, pos2)

}

func TestAdaptiveRadix_Delete(t *testing.T) {
	art := NewART()

	res1 := art.Delete([]byte("not exist"))
	t.Log(res1)
	assert.False(t, res1)

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	pos := art.Get([]byte("key-1"))
	t.Log(pos)
	assert.NotNil(t, pos)

	art.Delete([]byte("key-1"))
	pos = art.Get([]byte("key-1"))
	//t.Log(pos)
	assert.Nil(t, pos)
	pos = art.Get([]byte("key-1"))
	t.Log(pos)
}

func TestAdaptiveRadixTree_Size(t *testing.T) {
	art := NewART()

	t.Log(art.Size())
	assert.Equal(t, 0, art.Size())

	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-2"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("key-1"), &data.LogRecordPos{Fid: 1, Offset: 12})

	t.Log(art.Size())
	assert.Equal(t, 2, art.Size())
}

func TestAdaptiveRadix_Iterator(t *testing.T) {
	art := NewART()

	art.Put([]byte("ccde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("aabc"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("zzde"), &data.LogRecordPos{Fid: 1, Offset: 12})
	art.Put([]byte("kktc"), &data.LogRecordPos{Fid: 1, Offset: 12})

	iter := art.Iterator(true)

	for iter.Rewind(); iter.Valid(); iter.Next() {
		t.Log(string(iter.Key()))
	}

}
