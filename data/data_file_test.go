package data

import (
	"github.com/stretchr/testify/assert"
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
	dataFile, err := OpenDataFile("LogRecord", 456)
	assert.Nil(t, err)
	assert.NotNil(t, dataFile)

	err = dataFile.Write([]byte("aaa"))
	assert.Nil(t, err)

	err = dataFile.Sync()
	assert.Nil(t, err)
}
