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

func TestBTree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	//空的btree
	iter1 := bt1.Iterator(false)
	t.Log(iter1.Valid())
	assert.Equal(t, false, iter1.Valid())

	//有数据的btree
	bt1.Put([]byte("ccde"), &data.LogRecordPos{1, 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	t.Log(iter2.Key())
	t.Log(iter2.Value())

	iter2.Next()
	t.Log(iter2.Valid())

	//有多条数据
	bt1.Put([]byte("ccde"), &data.LogRecordPos{1, 10})
	bt1.Put([]byte("xxxxx"), &data.LogRecordPos{1, 10})
	bt1.Put([]byte("aassss"), &data.LogRecordPos{1, 10})

	//正向遍历
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
	}
	//反向遍历
	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		t.Log("key = ", string(iter4.Key()))
	}

	//测试seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("cc")); iter5.Valid(); iter5.Next() {
		t.Log(string(iter5.Key()))
	}

	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("zz")); iter6.Valid(); iter6.Next() {
		t.Log(string(iter6.Key()))
	}

}
