package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     //文件id，用于加载索引
	activeFile *data.DataFile            //当前活跃文件，可用于写入
	olderFile  map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index      index.Indexer             //内存索引
}

// Open 打开bitcask存储引擎实例的
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	//判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//初始化DB实例结构
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexType),
	}

	//加载对应的数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	//从数据文件中加载索引
	if err := db.loadIndexFromDataFile(); err != nil {
		return nil, err
	}

	return db, nil
}

// Put 写入key/value数据
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}

	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil

}

// Get 根据key读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	//先从索引中拿，没有说明不存在
	logRecordPos := db.index.Get(key)

	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	//根据文件id找到数据文件
	var dataFile *data.DataFile
	//当前活跃文件的文件id是否等于key对应的文件id
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		//不是的话去olderFile里面找
		dataFile = db.olderFile[logRecordPos.Fid]
	}

	//数据文件为空说明没有找到
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	//找到了数据文件，根据偏移量来读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	//判断LogRecord类型
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	return logRecord.Value, nil
}

// 追加写入到活跃文件当中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	//判断当前活跃文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	//如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//获取到了active文件，进行读写操作
	//对logRecord进行编码
	encRecord, size := data.EncodeLogRecord()

	//如果写入的数据已经到达了活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if db.activeFile.WriteOff+size > db.options.DataFileSize {
		//先将当前活跃文件持久化 保证已有的数据持久化到磁盘
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		//将当前活跃文件转化为旧的文件
		db.olderFile[db.activeFile.FileId] = db.activeFile

		//打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	//根据配置决定是否持久化
	if db.options.SyncWrites == true {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	//构造内存索引信息并返回
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}

	return pos, nil

}

// 设置当前活跃文件
// 对db实例的共享文件访问的时候要持有锁
func (db *DB) setActiveDataFile() error {
	//活跃文件为空则Id从0开始
	var initalFileld uint32 = 0
	//当前活跃文件不为空
	if db.activeFile != nil {
		initalFileld = db.activeFile.FileId + 1
	}

	//打开新的数据文件
	//将获取的dataFile给数据库实例的activeFile
	dataFile, err := data.OpenDataFile(db.options.DirPath, initalFileld)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// 从磁盘中加载数据文件
func (db *DB) loadDataFile() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int

	//遍历目录中的索引文件，找到以.data结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			//0000.data-->0000 文件id
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			//数据目录可能被损坏
			if err != nil {
				return ErrDataDirectoryCorrupted
			}

			fileIds = append(fileIds, fileId)
		}
	}

	//对文件id进行排序，从小到大以此加载
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//遍历每个文件id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		//当遍历到了最后一个文件，那就是活跃文件，否则加载到旧的数据文件中
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFile[uint32(i)] = dataFile
		}
	}

	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所以记录，并更新到内存中
func (db *DB) loadIndexFromDataFile() error {
	//说明当前是空的数据库
	if len(db.fileIds) == 0 {
		return nil
	}

	//遍历所以文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFile[fileId]
		}

		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//构建对应的内存索引
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}
			if logRecord.Type == data.LogRecordDeleted {
				db.index.Delete(logRecord.Key)
			} else {
				db.index.Put(logRecord.Key, logRecordPos)
			}

			//递增offset，下一次从新的位置开始读取
			offset += size
		}

		//如果是当前活跃文件，更新这个文件的offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}

	return nil
}
