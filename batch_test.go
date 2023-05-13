package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_WriteBatch(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//写数据后并不提交
	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	//t.Log(val)
	//t.Log(err)
	assert.NotNil(t, err)
	assert.Nil(t, val)

	//正常提交数据
	err = wb.Commit()
	assert.Nil(t, err)

	val, err = db.Get(utils.GetTestKey(1))
	//t.Log(string(val))
	//t.Log(err)
	assert.Nil(t, err)
	assert.NotNil(t, val)

	//删除操作
	wb2 := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	wb2.Commit()

	val2, err := db.Get(utils.GetTestKey(1))
	t.Log(string(val2))
	t.Log(err)
	assert.NotNil(t, err)
	assert.Nil(t, val2)
}

func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//在索引里面put一个数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	//为什么从暂存里面删除的key能够影响索引里面的数据呢
	//在commit的时候，会将暂存的key对应的索引数据找出来并删除
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	//重启
	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// 重启
	err = db.Close()
	assert.Nil(t, err)

	db2, err := Open(opts)
	assert.Nil(t, err)

	val, err := db2.Get(utils.GetTestKey(1))
	t.Log(val)
	t.Log(err)
	val1, err := db2.Get(utils.GetTestKey(2))
	t.Log(string(val1))
	t.Log(err)

	t.Log(db.seqNo)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	//defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	//wbops := DefaultWriteBatchOptions
	//wbops.MaxBatchNum = 1000000
	//wb := db.NewWriteBatch(wbops)
	//for i := 0; i < 500000; i++ {
	//	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	//	assert.Nil(t, err)
	//}
	//
	//err = wb.Commit()
	//assert.Nil(t, err)
}
