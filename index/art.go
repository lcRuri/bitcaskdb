package index

import (
	"bitcask-go/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

// AdaptiveRadix 自适应基数树
// 主要封装了https://github.com/plar/go-adaptive-radix-tree
type AdaptiveRadix struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadix {
	return &AdaptiveRadix{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}
func (art *AdaptiveRadix) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.RLock()
	defer art.lock.RUnlock()
	art.tree.Insert(key, pos)

	return true
}

func (art *AdaptiveRadix) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()

	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadix) Delete(key []byte) bool {
	art.lock.RLock()
	defer art.lock.RUnlock()

	_, deleted := art.tree.Delete(key)
	return deleted
}

func (art *AdaptiveRadix) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()

	return art.tree.Size()
}

func (art *AdaptiveRadix) Iterator(reserve bool) Iterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reserve)
}

// Art 索引迭代器
type artIterator struct {
	currIndex int     //当前遍历的位置
	reserve   bool    //是否是一个反向的遍历
	values    []*Item //key+位置索引信息

}

func newARTIterator(tree goart.Tree, reserve bool) *artIterator {
	var idx int
	if reserve {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reserve {
			idx--
		} else {
			idx++
		}

		return true
	}

	tree.ForEach(saveValues)

	return &artIterator{
		currIndex: 0,
		reserve:   reserve,
		values:    values,
	}
}

// Rewind 重新回到迭代器的起点，即第一个数据的位置
func (ai *artIterator) Rewind() {
	ai.currIndex = 0
}

// Seek 根据传入的key查找到第一个大于(小于)等于的目标的key，从这个key开始遍历
func (ai *artIterator) Seek(key []byte) {
	if ai.reserve {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) <= 0
		})
	} else {
		ai.currIndex = sort.Search(len(ai.values), func(i int) bool {
			return bytes.Compare(ai.values[i].key, key) >= 0
		})
	}

}

// Next 跳转到下一个key
func (ai *artIterator) Next() {
	ai.currIndex += 1
}

// Valid 是否有效，即是否已经遍历完了所有的key，用于退出遍历
func (ai *artIterator) Valid() bool {
	return ai.currIndex < len(ai.values)
}

// Key 当前遍历位置的Key数据
func (ai *artIterator) Key() []byte {
	return ai.values[ai.currIndex].key
}

// Value 当前遍历位置的Value信息
func (ai *artIterator) Value() *data.LogRecordPos {
	return ai.values[ai.currIndex].pos
}

// Close 关闭迭代器，并释放相关资源
func (ai *artIterator) Close() {
	ai.values = nil
}
