package bitcask_go

import "os"

type Options struct {
	DirPath string //数据库目录文件

	DataFileSize int64 //数据文件的大小

	SyncWrites bool //每次写数据是否持久化

	IndexType IndexerType //索引类型
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	//遍历前缀为指定值的key，默认为空
	Prefix []byte
	//是否反向遍历，默认false为正向
	Reserve bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reserve: false,
}
