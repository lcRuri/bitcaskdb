package redis

import (
	bitcask "bitcask-go"
	"bitcask-go/utils"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(33))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	// del
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, bitcask.ErrKeyNotFound, err)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok1, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok1, err)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok2, err)

	ok3, err := rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(100))
	t.Log(ok3, err)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	t.Log(string(val1))
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	t.Log(string(val2))
	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field-not-exist"))
	t.Log(string(val3), err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	del1, err := rds.HDel(utils.GetTestKey(200), nil)
	t.Log(del1, err)

	ok2, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(100))
	t.Log(ok2, err)
	del2, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	t.Log(del2, err)
}

func TestRedisDataStructure_SAdd(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	ok, err := rds.SAdd(utils.GetTestKey(1), []byte("a"))
	t.Log(ok, err)
	ok1, err := rds.SAdd(utils.GetTestKey(1), []byte("a"))
	t.Log(ok1, err)
	ok2, err := rds.SAdd(utils.GetTestKey(1), []byte("c"))
	t.Log(ok2, err)

	ok, err = rds.SIsMember(utils.GetTestKey(2), []byte("a"))
	t.Log(ok, err)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("a"))
	t.Log(ok, err)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("c"))
	t.Log(ok, err)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("aaaa"))
	t.Log(ok, err)

	ok, err = rds.SRem(utils.GetTestKey(2), []byte("axs"))
	t.Log(ok, err)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("axs"))
	t.Log(ok, err)
	ok, err = rds.SRem(utils.GetTestKey(1), []byte("a"))
	t.Log(ok, err)
	ok, err = rds.SIsMember(utils.GetTestKey(1), []byte("a"))
	t.Log(ok, err)
}

func TestRedisDataStructure_List(t *testing.T) {
	opts := bitcask.DefaultOptions
	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)

	res, err := rds.LPush(utils.GetTestKey(1), []byte("a"))
	t.Log(res, err)
	res, err = rds.LPush(utils.GetTestKey(1), []byte("b"))
	t.Log(res, err)

	pop1, err := rds.RPop(utils.GetTestKey(1))
	t.Log(string(pop1), err)

	res, err = rds.LPush(utils.GetTestKey(1), []byte("c"))
	t.Log(res, err)

	pop, err := rds.LPop(utils.GetTestKey(1))
	t.Log(string(pop), err)
	pop, err = rds.LPop(utils.GetTestKey(1))
	t.Log(string(pop), err)
	pop, err = rds.LPop(utils.GetTestKey(1))
	t.Log(string(pop), err)
}
