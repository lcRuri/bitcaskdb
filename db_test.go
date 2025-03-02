package bitcask_go

import (
	"bitcask-go/utils"
	"bytes"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

// 测试完成之后销毁 DB 数据目录
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()

		}
		err := os.RemoveAll(db.options.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	t.Log(opts.DirPath)
}

func TestDB_Put(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常 Put 一条数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.重复 Put key 相同的数据
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key 为空
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.value 为空
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	//// 5.写到数据文件进行了转换
	//for i := 0; i < 1000000; i++ {
	//	err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
	//	assert.Nil(t, err)
	//}
	//assert.Equal(t, 2, len(db.olderFile))

	// 6.重启后再 Put 数据
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer db2.Close()
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常读取一条数据
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.读取一个不存在的 key
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrKeyNotFound, err)

	// 3.值被重复 Put 后在读取
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4.值被删除后再 Get
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrKeyNotFound, err)

	//// 5.转换为了旧的数据文件，从旧的数据文件上获取 value
	//for i := 100; i < 1000000; i++ {
	//	err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
	//	assert.Nil(t, err)
	//}
	//assert.Equal(t, 2, len(db.olderFile))
	//val5, err := db.Get(utils.GetTestKey(101))
	//assert.Nil(t, err)
	//assert.NotNil(t, val5)

	// 6.重启后，前面写入的数据都能拿到
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer db2.Close()
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.正常删除一个存在的 key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	// 2.删除一个不存在的 key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.删除一个空的 key
	err = db.Delete(nil)
	assert.Equal(t, ErrKeyIsEmpty, err)

	// 4.值被删除之后重新 Put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.重启之后，再进行校验
	err = db.Close()
	assert.Nil(t, err)

	// 重启数据库
	db2, err := Open(opts)
	defer db2.Close()
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}

func TestDB_ListKeys(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	assert.Equal(t, 0, len(keys))

	err = db.Put(utils.GetTestKey(10), utils.RandomValue(11))
	assert.Nil(t, err)

	keys1 := db.ListKeys()
	for _, k := range keys1 {
		t.Log(string(k))
	}

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(11))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(11))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(11))
	assert.Nil(t, err)

	keys2 := db.ListKeys()
	for _, k := range keys2 {
		t.Log(string(k))
	}

}

func TestDB_Fold(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(11))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(11))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(11))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(11))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		t.Log(string(key))
		t.Log(string(value))
		if bytes.Compare(key, utils.GetTestKey(22)) == 0 {
			return false
		}
		return true
	})

	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(11))
	assert.Nil(t, err)

}

func TestDB_Sync(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(11))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)

}

func TestDB_FileLock(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db2, err := Open(opts)
	t.Log(db2)
	t.Log(err)
}

func TestDB_Open2(t *testing.T) {
	opts := DefaultOptions
	//写入1.16G数据
	//now := time.Now()
	//for j := 0; j < 1000; j++ {
	//	for i := 0; i < 20000; i++ {
	//		err = db.Put(utils.GetTestKey(i), utils.RandomValue(10))
	//		assert.Nil(t, err)
	//	}
	//}
	//
	//t.Log("cost: ", time.Since(now))

	now := time.Now()
	db, err := Open(opts)
	t.Log("open time ", time.Since(now))
	assert.Nil(t, err)
	assert.NotNil(t, db)

	//使用MMap：  7.115577333s
	//不使用MMap：40.743409792s

}

func TestDB_Stat(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(10))
		assert.Nil(t, err)
	}

	for i := 100; i < 1000; i++ {
		err = db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}

	for i := 1000; i < 5000; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(10))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	t.Log(stat)
	assert.NotNil(t, stat)

}

func TestDB_BackUp(t *testing.T) {
	opts := DefaultOptions
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 1; i < 100; i++ {
		err = db.Put(utils.GetTestKey(i), utils.RandomValue(10))
		assert.Nil(t, err)
	}

	dir := "/Users/yefeixiang/coding/kv-projects/bitcask-go/backup"
	err = db.BackUp(dir)
	assert.Nil(t, err)

	opts1 := DefaultOptions
	opts1.DirPath = dir
	db1, err := Open(opts1)
	assert.Nil(t, err)
	assert.NotNil(t, db1)
}
