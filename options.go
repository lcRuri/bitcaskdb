package bitcask_go

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

// WriteBatchOptions 批量写配置
type WriteBatchOptions struct {
	//一个批次当中最大的数据量
	MaxBatchNum int

	//提交事务时是否持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      " tmp/",
	DataFileSize: 256 * 1024 * 1024,
	SyncWrites:   false,
	IndexType:    Btree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reserve: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
