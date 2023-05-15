package main

import (
	bitcask_go "bitcask-go"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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

func handlePut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, "method not allowed", http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, "method not allowed", http.StatusInternalServerError)
			log.Printf("failed to put value in db: %v\n", err)
			return
		}
	}
	_ = json.NewEncoder(writer).Encode("Put OK")
}

func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to get value in db: %v\n", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(string(value))
}

func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete value in db: %v\n", err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("Delete OK")
}

func handleIndexType(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	index := bitcask_go.DefaultOptions.IndexType

	if index == 1 {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode("BTree")
	} else if index == 2 {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode("ART")
	} else if index == 3 {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode("BPlusTree")
	}
}

func handleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}

	_ = json.NewEncoder(writer).Encode(result)

}

func handleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)

}

func handleMerge(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	err := db.Merge()
	if err != nil {
		if err == bitcask_go.ErrMergeIsProgress {
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(bitcask_go.ErrMergeIsProgress)
			return
		} else if err == bitcask_go.ErrMergeRatioUnreached {
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(bitcask_go.ErrMergeRatioUnreached)
			return
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("failed to merge in db: %v\n", err)
			return
		}
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("Merge Success")
}

func handleBackup(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	dir := "/Users/yefeixiang/coding/kv-projects/bitcask-go/backup"
	err := db.BackUp(dir)
	if err != nil {
		if err == bitcask_go.ErrNoEnoughSpaceForMerge {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("no enough disk space!")
			return
		} else {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Printf("unable to backup,err %v\n", err)
			return
		}
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("Backup Success")
}

func handleBatchPut(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	opts := bitcask_go.DefaultWriteBatchOptions
	wb := db.NewWriteBatch(opts)
	//默认自动提交，后面可以更根据选择决定是否提交
	defer wb.Commit()

	var wdata map[string]string
	if err := json.NewDecoder(request.Body).Decode(&wdata); err != nil {
		http.Error(writer, "method not allowed", http.StatusBadRequest)
		return
	}

	for key, value := range wdata {
		if err := wb.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, "method not allowed", http.StatusInternalServerError)
			log.Printf("failed to put value in db: %v\n", err)
			return
		}
	}
	_ = json.NewEncoder(writer).Encode("BatchPut OK")
}

func handleBatchDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	opts := bitcask_go.DefaultWriteBatchOptions
	wb := db.NewWriteBatch(opts)
	//默认自动提交，后面可以更根据选择决定是否提交
	defer wb.Commit()

	key := request.URL.Query().Get("key")
	err := wb.Delete([]byte(key))
	if err != nil && err != bitcask_go.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Printf("failed to delete value in db: %v\n", err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("BatchDelete OK")

}

func main() {
	//注册处理方法
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/getIndexType", handleIndexType)
	http.HandleFunc("/bitcask/batchput", handleBatchPut)
	http.HandleFunc("/bitcask/batchdelete", handleBatchDelete)

	http.HandleFunc("/bitcask/stat", handleStat)
	http.HandleFunc("/bitcask/merge", handleMerge)
	http.HandleFunc("/bitcask/backup", handleBackup)

	//启动http服务
	_ = http.ListenAndServe("localhost:8080", nil)
}
