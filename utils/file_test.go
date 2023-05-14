package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDirSize(t *testing.T) {
	dir0, err0 := os.Getwd()
	assert.Nil(t, err0)
	t.Log(dir0)

	dirSize, err := DirSize(dir0)
	assert.Nil(t, err)
	t.Log(dirSize)
}

func TestAvailableDiskSize(t *testing.T) {
	size, err := AvailableDiskSize()
	assert.Nil(t, err)
	t.Log(size / 1024 / 1024 / 1024)
	assert.NotNil(t, size)
}
