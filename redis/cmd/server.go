package main

import (
	bitcask_go "bitcask-go"
	"fmt"

	bitcask_redis "bitcask-go/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const port = "6380"
const addr = "127.0.0.1:" + port

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	//打开redis数据结构服务
	redisDataStructure, err := bitcask_redis.NewRedisDataStructure(bitcask_go.DefaultOptions)
	if err != nil {
		panic(err)
	}

	//初始化bitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	//初始化一个redis服务
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.Listen()

}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	fmt.Println("1")
	fmt.Println(conn)
	fmt.Println()
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()

	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true

}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}

	//fmt.Println("3")
	//_ = svr.server.Close()
}

func (svr *BitcaskServer) Listen() {
	log.Println("bitcask server running, ready to accept connections.")

	_ = svr.server.ListenAndServe()
}
