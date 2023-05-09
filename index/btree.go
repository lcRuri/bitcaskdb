package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

// BTree 索引，主要封装google的btree库
type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		//控制叶子节点数量
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{
		key: key,
		pos: pos,
	}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{
		key: key,
	}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{
		key: key,
	}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Iterator(reserve bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reserve)
}

// BTree的索引迭代器
type btreeIterator struct {
	currIndex int     //当前遍历的位置
	reserve   bool    //是否是一个反向的遍历
	values    []*Item //key+位置索引信息

}

func newBTreeIterator(tree *btree.BTree, reserve bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	saveValues := func(it btree.Item) bool {
		//将所有数据存放到values中
		values[idx] = it.(*Item)
		idx++
		return true

	}

	//反向存放
	if reserve {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		currIndex: 0,
		reserve:   reserve,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据的位置
func (bti *btreeIterator) Rewind() {
	bti.currIndex = 0
}

// Seek 根据传入的key查找到第一个大于(小于)等于的目标的key，从这个key开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reserve {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.currIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}

}

// Next 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (bti *btreeIterator) Valid() bool {
	return bti.currIndex < len(bti.values)
}

// Key 当前遍历位置的Key数据
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.currIndex].key
}

// Value 当前遍历位置的Value信息
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.currIndex].pos
}

// Close 关闭迭代器，并释放相关资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
