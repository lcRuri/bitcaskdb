package redis

import (
	bitcask_go "bitcask-go"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataType = byte

const (
	String RedisDataType = iota
	Hash
	List
	Set
	ZSet
)

// RedisDataStructure Redis 数据结构服务
type RedisDataStructure struct {
	db *bitcask_go.DB
}

// NewRedisDataStructure 初始化 Redis 数据结构服务
func NewRedisDataStructure(opts bitcask_go.Options) (*RedisDataStructure, error) {
	db, err := bitcask_go.Open(opts)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

// ======================= String 数据结构 =======================

func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}
	// 编码 value : type + expire + payload
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	index := 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)

	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储接口写入数据
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	//解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	index := 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	// 判断是否过期
	if expire > 0 && expire < time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], err
}

// ======================= Hash 数据结构 =======================

func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 查找对应的元数据,不存在则新建
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 构造 Hash 数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	enckey := hk.encode()

	//先查找数据是否存在
	var exist = true
	if _, err := rds.db.Get(enckey); err == bitcask_go.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	//如果不存在，更新元数据
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encode())
	}

	_ = wb.Put(enckey, value)

	if err = wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil

}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}
	//元数据的size为0，说明这个key压根没存数据
	if meta.size == 0 {
		return nil, nil
	}

	// 构造 Hash 数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	return rds.db.Get(hk.encode())
}

func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}
	//元数据的size为0，说明这个key压根没存数据
	if meta.size == 0 {
		return false, nil
	}

	// 构造 Hash 数据部分的key
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}

	encKey := hk.encode()

	//查看是否存在,存在删除才为true，否则是false
	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask_go.ErrKeyNotFound {
		exist = false
	}

	if exist {
		wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
		meta.size--
		_ = wb.Put(key, meta.encode())
		_ = wb.Delete(key)

		if err = wb.Commit(); err != nil {
			return false, err
		}

	}

	return exist, nil
}

// ======================= Set 数据结构 =======================

func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	var ok bool
	if _, err = rds.db.Get(sk.encode()); err == bitcask_go.ErrKeyNotFound {
		//不存在的话则更新
		wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
		meta.size++
		_ = wb.Put(key, meta.encode())
		_ = wb.Put(sk.encode(), nil)

		if err = wb.Commit(); err != nil {
			return false, err
		}

		ok = true
	}

	return ok, nil
}

func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, nil
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	_, err = rds.db.Get(sk.encode())

	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return false, err
	}

	if err == bitcask_go.ErrKeyNotFound {
		return false, nil
	}

	return true, nil
}

func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	meta, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, nil
	}
	if meta.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: meta.version,
		member:  member,
	}

	if _, err = rds.db.Get(sk.encode()); err == bitcask_go.ErrKeyNotFound {
		return false, nil
	}

	//更新元数据和数据部分
	wb := rds.db.NewWriteBatch(bitcask_go.DefaultWriteBatchOptions)
	meta.size--
	_ = wb.Put(key, meta.encode())
	_ = wb.Delete(sk.encode())
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return true, nil

}

func (rds *RedisDataStructure) findMetadata(key []byte, dataType RedisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true
	if err == bitcask_go.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(metaBuf)
		//判断数据类型
		if meta.dataType != Hash {
			return nil, ErrWrongTypeOperation
		}
		//判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: Hash,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}

		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}

	return meta, nil
}
