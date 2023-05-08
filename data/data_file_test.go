package data

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestOpenDataFile(t *testing.T) {
	dataFile, err := OpenDataFile("LogRecord", 0)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	dataFile1, err := OpenDataFile("LogRecord", 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	dataFile2, err := OpenDataFile("LogRecord", 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile2)
}

func TestDataFile_Write(t *testing.T) {
	dataFile1, err := OpenDataFile("LogRecord", 111)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile1)

	err = dataFile1.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("bbb"))
	assert.Nil(t, err)

	err = dataFile1.Write([]byte("ccc"))
	assert.Nil(t, err)

}

func TestDataFile_Close(t *testing.T) {
	dataFile, err := OpenDataFile("LogRecord", 123)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Close()
	assert.Nil(t, err)

}

func TestDataFile_Sync(t *testing.T) {
	dataFile, err := OpenDataFile(os.TempDir(), 456)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)

	t.Log(os.TempDir())
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	dataFile, err := OpenDataFile("kv", 3333)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	//只有一条logRecord
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask kv go"),
	}

	res1, size1 := EncodeLogRecord(rec1)
	err = dataFile.Write(res1)
	assert.Nil(t, err)

	t.Log(size1)

	readRec1, readSize1, err := dataFile.ReadLogRecord(0)
	assert.Nil(t, err)

	assert.Equal(t, rec1, readRec1)
	assert.Equal(t, size1, readSize1)

	//多条LogRecord，从不同位置读取
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
	}

	res2, size2 := EncodeLogRecord(rec2)
	err = dataFile.Write(res2)
	assert.Nil(t, err)

	t.Log(size2)

	readRec2, readSize2, err := dataFile.ReadLogRecord(24)
	assert.Nil(t, err)
	assert.Equal(t, size2, readSize2)
	assert.Equal(t, rec2, readRec2)

	//被删除的数据在数据文件的末尾
	rec3 := &LogRecord{
		Key:   []byte("1"),
		Value: []byte("value"),
		Type:  LogRecordDeleted,
	}

	res3, size3 := EncodeLogRecord(rec3)
	err = dataFile.Write(res3)
	assert.Nil(t, err)

	t.Log(size3)

	readRec3, readSize3, err := dataFile.ReadLogRecord(size1 + size2)
	assert.Nil(t, err)
	assert.Equal(t, size3, readSize3)
	assert.Equal(t, rec3, readRec3)
}
