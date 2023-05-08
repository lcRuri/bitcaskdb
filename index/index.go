package index

import (
	"bitcask-go/data"
	"bytes"
	"github.com/google/btree"
)

// Indexer 抽象索引接口 后续如果想要接入其他的数据结构 则之间实现这个接口
type Indexer interface {
	// Put 向索引中存储key对应的数据位置信息
	Put(key []byte, pos *data.LogRecordPos) bool

	// Get 根据key取出对应的索引位置信息
	Get(key []byte) *data.LogRecordPos

	// Delete 根据key删除对应的索引位置信息
	Delete(key []byte) bool
}

type IndexType = int8

const (
	// Btree 索引
	Btree IndexType = iota + 1

	// ART 自适应基树树索引
	ART
)

// NewIndexer 根据索引类型初始化索引
func NewIndexer(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		//todo
		return nil
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}
