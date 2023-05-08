package bitcask_go

import "os"

type Options struct {
	DirPath string //数据库目录文件

	DataFileSize int64 //数据文件的大小

	SyncWrites bool //每次写数据是否持久化

	IndexType IndexerType //索引类型
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
