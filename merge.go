package bitcask_go

import (
	"bitcask-go/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirname     = "-merge"
	mergeFinishedKey = "merge.finished"
)

// Merge 清理无效数据 生成hint文件
func (db *DB) Merge() error {
	//如果数据库为空 返回
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	//如果发现正在merge，则直接返回
	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeIsProgress
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	//开始merge流程
	//0 1 2 ，2当前活跃文件
	//merge打开新的活跃文件3

	//持久化当前活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}

	//将当前活跃文件转换为旧的活跃文件
	db.olderFile[db.activeFile.FileId] = db.activeFile
	//打开新的活跃文件
	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}
	//记录最近没有参与merge的文件id
	nonMergeFileId := db.activeFile.FileId

	//取出所有需要merge的活跃文件
	//相当于备份,避免读取别的线程读取旧数据时候产生问题
	var mergeFiles []*data.DataFile

	for _, file := range db.olderFile {
		mergeFiles = append(mergeFiles, file)
	}

	db.mu.Unlock()

	//待merge的文件从小到大进行排序，依次merge
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})

	mergePath := db.getMergePath()

	//如果目录存在，说明发生过merge，将其删除掉
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	//打开一个新的临时的bitcask实例
	mergeOptions := db.options
	mergeOptions.DirPath = mergePath
	mergeOptions.SyncWrites = false
	mergeDB, err := Open(mergeOptions)
	if err != nil {
		return err
	}

	//打开hint文件存储索引
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	//遍历处理每个数据
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}

				return err
			}

			//解析拿到实际的key
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//和内存中的索引位置进行比较，如果有效则重写
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				//清除事务标记号
				logRecord.Key = logRecordKeyWithSeq(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				//将当前数据索引写到Hint文件中去
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}
			}

			//递增offset
			offset += size
		}
	}

	//循环结束之后对数据进行持久化，保证数据正确写入到磁盘
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	//新增一个标识merge完成的标识文件，存在才说明merge有效
	//写标识merge完成的文件
	mergeFinishedFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	//value是没有参与merge的活跃文件id
	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encLogRecord, _ := data.EncodeLogRecord(mergeFinRecord)
	if err := mergeFinishedFile.Write(encLogRecord); err != nil {
		return err
	}

	if err := mergeFinishedFile.Sync(); err != nil {
		return err
	}

	return nil

}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)
	return filepath.Join(dir, base+mergeDirname)

}

// 加载merge数据目录
func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	//merge目录不存在的话直接返回
	if _, err := os.Stat(mergePath); err != nil {
		return nil
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()

	//将整个merge目录读取出来
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	//查找标识merge完成的文件是否存在，判断merge是否完成
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}

		mergeFileNames = append(mergeFileNames, entry.Name())
	}

	//merge没有完成直接返回
	if mergeFinished == false {
		return nil
	}

	//拿到没有参与merge的文件id
	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//删除旧的的数据文件
	//删除比它小的文件id
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		//拿到文件名字
		fileName := data.GetDataFileName(db.options.DirPath, fileId)
		if _, err := os.Stat(fileName); err != nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}

	//将新的数据文件移动到正常读取的目录下面
	for _, fileName := range mergeFileNames {
		// temp/bitcask-merge 00.data 11.data
		// temp/bitcask 00.data 11.data
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.options.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}

	return nil

}

// 拿到没有参与merge的文件id
func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	mergeFinishedFile, err := data.OpenMergeFinishedFile(dirPath)
	if err != nil {
		return 0, err
	}
	//从merge完成的文件中读取数据，因为只写入了一条数据，所有偏移地址为0
	record, _, err := mergeFinishedFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}

	return uint32(nonMergeFileId), nil
}

// 从hint文件中加载索引
func (db *DB) loadIndexFromHintFile() error {
	//查看hint索引文件是否存在
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); err != nil {
		return err
	}

	//打开对应的hint索引文件
	//??? hint文件不应该是在-merge目录下面吗，但是db.options.DirPath应该是原来的目录
	//hint文件也是在finishedMerge目录之前，并且默认文件id为0，也就是fid比finishedMerge小，在loadMergeFiles的时候挪到了db.options.DirPath下
	hintFile, err := data.OpenHintFile(db.options.DirPath)
	if err != nil {
		return err
	}

	//构造内存索引
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		//logRecord是经过编码的
		//解码拿到实际的索引信息
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}

	return nil

}
