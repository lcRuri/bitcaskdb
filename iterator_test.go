package bitcask_go

import (
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()

	assert.NotNil(t, iterator)
	assert.Equal(t, false, iterator.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.RandomValue(10))
	assert.Nil(t, err)

	iterator := db.NewIterator(DefaultIteratorOptions)
	defer iterator.Close()

	assert.NotNil(t, iterator)
	assert.Equal(t, true, iterator.Valid())
	value, _ := iterator.Value()
	t.Log(string(iterator.Key()))
	t.Log(string(value))

}

func TestDB_Iterator_Multi_Value(t *testing.T) {
	opts := DefaultOptions
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("annde"), utils.RandomValue(10))
	db.Put([]byte("cnedc"), utils.RandomValue(10))
	db.Put([]byte("aeeue"), utils.RandomValue(10))
	db.Put([]byte("esuns"), utils.RandomValue(10))
	db.Put([]byte("dadad"), utils.RandomValue(10))

	iter1 := db.NewIterator(DefaultIteratorOptions)
	defer iter1.Close()

	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		t.Log("key = ", string(iter1.Key()))
	}
	t.Log("-----------------------")
	iter1.Rewind()
	for iter1.Seek([]byte("c")); iter1.Valid(); iter1.Next() {
		t.Log("key = ", string(iter1.Key()))
	}

	t.Log("-----------------------")

	//反向迭代
	iter_opt1 := DefaultIteratorOptions
	iter_opt1.Reserve = true
	iter2 := db.NewIterator(iter_opt1)
	defer iter2.Close()

	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		t.Log("key = ", string(iter2.Key()))
	}

	t.Log("-----------------------")
	iter2.Rewind()
	for iter2.Seek([]byte("c")); iter2.Valid(); iter2.Next() {
		t.Log("key = ", string(iter2.Key()))
	}

	//指定prefix
	iterOpts2 := DefaultIteratorOptions
	iterOpts2.Prefix = []byte("ae")
	iter3 := db.NewIterator(iterOpts2)
	defer iter3.Close()

	t.Log("-----------------------")
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		t.Log("key = ", string(iter3.Key()))
	}
}
