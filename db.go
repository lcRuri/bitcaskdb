package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/fio"
	"bitcask-go/index"
	"errors"
	"fmt"
	"github.com/gofrs/flock"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	seqNoKey     = "seq.no"
	fileLockName = "flock"
)

// DB bitcask 存储引擎实例
type DB struct {
	options         Options
	mu              *sync.RWMutex
	fileIds         []int                     //文件id，用于加载索引
	activeFile      *data.DataFile            //当前活跃文件，可用于写入
	olderFile       map[uint32]*data.DataFile //旧的数据文件，只能用于读
	index           index.Indexer             //内存索引
	seqNo           uint64                    //事务序列号 全局递增
	isMerging       bool                      //是否正在进行merge
	seqNoFileExists bool                      //存储事务序列号的文件是否存在
	isInitial       bool                      //是否第一次初始化次目录
	fileLock        *flock.Flock              //文件锁对象保证多进场之间的互斥
	bytesWrites     uint                      //累计写了多少字节
}

// Open 打开bitcask存储引擎实例
func Open(options Options) (*DB, error) {
	//对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	var isInitial bool
	//判断数据目录是否存在，如果不存在，则创建这个目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		isInitial = true
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	//判断当前数据目录是否在使用 文件锁
	fileLock := flock.New(filepath.Join(options.DirPath, fileLockName))
	//尝试获取锁
	hold, err := fileLock.TryLock()
	if err != nil {
		return nil, err
	}
	if !hold {
		return nil, ErrDatabaseIsUsing
	}

	//可能目录存在但是里面没有内容
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		isInitial = true
	}

	//初始化DB实例结构
	db := &DB{
		options:   options,
		mu:        new(sync.RWMutex),
		olderFile: make(map[uint32]*data.DataFile),
		index:     index.NewIndexer(options.IndexType, options.DirPath, options.SyncWrites),
		isInitial: isInitial,
		fileLock:  fileLock,
	}

	//加载merge数据目录
	//??? 为什么在这调用，还有为什么merge里面的方法不加锁
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}

	//加载对应的数据文件
	if err := db.loadDataFile(); err != nil {
		return nil, err
	}

	//b+树索引不需要从数据文件加载索引
	if options.IndexType != BPlusTree {
		//从hint索引文件中加载索引
		if err := db.loadIndexFromHintFile(); err != nil {
			return nil, err
		}

		//从数据文件中加载索引
		if err := db.loadIndexFromDataFile(); err != nil {
			return nil, err
		}

		//重置 IO 类型为标准 IO 类型
		if db.options.MMapAtStartUp {
			if err := db.resetIoType(); err != nil {
				return nil, err
			}
		}
	}

	//取出当前事务序列号
	if options.IndexType == BPlusTree {
		if err := db.loadSeqNo(); err != nil {
			return nil, err
		}

		//在b+树模式下，更新活跃文件的offset
		if db.activeFile != nil {
			size, err := db.activeFile.IoManager.Size()
			if err != nil {
				return nil, err
			}
			db.activeFile.WriteOff = size
		}
	}

	return db, nil
}

func (db *DB) Close() error {
	defer func() {
		if err := db.fileLock.Unlock(); err != nil {
			panic(fmt.Sprintf("failed to unlock the directory,%v", err))
		}
	}()
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	//在关闭数据库的时候，需要将索引也关闭
	//如果是b+树，它实际上也是对应的bboltdb数据库的一个实例
	//不然重启打开的话，再打开b+树实例，可能堵塞，因为只允许一个线程进行访问
	if err := db.index.Close(); err != nil {
		return err
	}

	//保存当前事务序列号
	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}
	record := &data.LogRecord{
		Key:   []byte(seqNoKey),
		Value: []byte(strconv.FormatUint(db.seqNo, 10)),
	}

	encRecord, _ := data.EncodeLogRecord(record)
	if err := seqNoFile.Write(encRecord); err != nil {
		return err
	}
	if err := seqNoFile.Sync(); err != nil {
		return err
	}

	//关闭活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}

	//关闭旧的数据文件
	for _, file := range db.olderFile {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil

}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	return db.activeFile.Sync()
}

// Put 写入key/value数据
func (db *DB) Put(key []byte, value []byte) error {
	//判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	//追加写入到当前活跃数据文件当中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil

}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	//先检查key是否存在，不存在返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	//构造LogRecord，标识其为删除的
	logRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Type: data.LogRecordDeleted,
	}

	//加入都数据文件中
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err

	}

	//从内存索引中删除
	ok := db.index.Delete(key)
	if !ok {
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

	return db.getValueByPosition(logRecordPos)
}

// ListKeys 获取数据库中所有的key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	defer iterator.Close()

	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据 并执行用户指定的操作 函数返回false时 终止遍历
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	defer iterator.Close()

	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}

		if !fn(iterator.Key(), value) {
			break
		}
	}

	return nil
}

// 根据索引位置信息获取对应的value
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
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

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

// 追加写入到活跃文件当中
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	//判断当前活跃文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	//如果为空则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	//获取到了active文件，进行读写操作
	//对logRecord进行编码
	encRecord, size := data.EncodeLogRecord(logRecord)

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

	//对累计数量进行递增
	db.bytesWrites += uint(size)
	//根据配置决定是否持久化
	var needSync = db.options.SyncWrites
	if !needSync && db.options.BytesPerSync > 0 && db.bytesWrites >= db.options.BytesPerSync {
		needSync = true
	}

	if db.options.SyncWrites == true {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		//清空累计值
		if db.bytesWrites > 0 {
			db.bytesWrites = 0
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
	dataFile, err := data.OpenDataFile(db.options.DirPath, initalFileld, fio.StandardIO)
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
		ioType := fio.StandardIO
		if db.options.MMapAtStartUp {
			ioType = fio.MemoryMap
		}
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid), ioType)
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

	//查看是否发生过merge
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.options.DirPath, data.MergeFinishedFileName)
	//如果存在
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileId(db.options.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}

		if !ok {
			panic("failed to update index at startup")
		}
	}

	//暂存事务数据
	TransactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	//遍历所以文件id，处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		//如果比最近未参与merge的文件id更小，则说明已经从hint文件中加载索引了
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
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

			//可能会拿大commit的一部分数据 所以要暂存起来
			//解析key 拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				//非事务操作 直接更新索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				//事务完成 对应的seqNo的数据可以更新到内存索引当中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecords := range TransactionRecords[seqNo] {
						updateIndex(txnRecords.Record.Key, txnRecords.Record.Type, txnRecords.Pos)
					}

					delete(TransactionRecords, seqNo)
				} else {
					//正常写入的数据 但是还未提交成功
					logRecord.Key = realKey
					TransactionRecords[seqNo] = append(TransactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			//更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}
			//递增offset，下一次从新的位置开始读取
			offset += size
		}

		//如果是当前活跃文件，更新这个文件的offset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOff = offset
		}
	}

	//更新事务序列号
	db.seqNo = currentSeqNo

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

func (db *DB) loadSeqNo() error {
	filename := filepath.Join(db.options.DirPath, data.SeqNoFileName)
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil
	}

	seqNoFile, err := data.OpenSeqNoFile(db.options.DirPath)
	if err != nil {
		return err
	}

	record, _, err := seqNoFile.ReadLogRecord(0)

	seqNo, err := strconv.ParseUint(string(record.Value), 10, 64)
	if err != nil {
		return err
	}

	db.seqNo = seqNo
	db.seqNoFileExists = true

	//seqNoFile会一直追加写，所有加载后将这个文件删掉
	return os.Remove(filename)
}

// 将数据文件的IO类型设置为标准文件IO类型
func (db *DB) resetIoType() error {
	if db.activeFile == nil {
		return nil
	}

	if err := db.activeFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
		return err
	}

	for _, dataFile := range db.olderFile {
		if err := dataFile.SetIOManager(db.options.DirPath, fio.StandardIO); err != nil {
			return err
		}
	}

	return nil
}
