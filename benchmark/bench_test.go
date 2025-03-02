package benchmark

import (
	bitcask_go "bitcask-go"
	"bitcask-go/utils"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

var db *bitcask_go.DB

func init() {
	//初始化存储引擎实例
	var err error
	options := bitcask_go.DefaultOptions
	db, err = bitcask_go.Open(options)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}

}

func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask_go.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
