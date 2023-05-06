package index

import (
	"bitcask-go/data"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res2)
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	res1 := bt.Put(nil, &data.LogRecordPos{1, 100})
	assert.True(t, res1)

	pos1 := bt.Get(nil)
	fmt.Println(pos1)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{1, 3})
	assert.True(t, res3)

	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res0 := bt.Put(nil, &data.LogRecordPos{1, 1})
	assert.True(t, res0)

	dres0 := bt.Delete(nil)
	assert.True(t, dres0)

	res := bt.Put([]byte("a"), &data.LogRecordPos{1, 2})
	assert.True(t, res)

	pos := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos.Fid)
	assert.Equal(t, int64(2), pos.Offset)

	dres := bt.Delete([]byte("a"))
	assert.True(t, dres)
}
