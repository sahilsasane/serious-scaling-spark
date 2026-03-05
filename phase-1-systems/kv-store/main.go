package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

type KVStore struct {
	mu      sync.RWMutex
	data    map[string]string
	walFile *os.File
}

func NewKVStore(walPath string) *KVStore {
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	kv := &KVStore{data: make(map[string]string), walFile: f}
	kv.replay()
	return kv
}

func (kv *KVStore) log(entry string) {
	fmt.Fprintln(kv.walFile, entry)
}

func (kv *KVStore) replay() {
	kv.walFile.Seek(0, 0)
	scanner := bufio.NewScanner(kv.walFile)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		switch strings.ToUpper(parts[0]) {
		case "SET":
			if len(parts) >= 3 {
				kv.data[parts[1]] = parts[2]
			}
		case "DEL":
			if len(parts) >= 2 {
				delete(kv.data, parts[1])
			}
		}
	}
}

func (kv *KVStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.log(fmt.Sprintf("SET %s %s", key, value))
	kv.data[key] = value
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}

func (kv *KVStore) Del(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.log(fmt.Sprintf("DEL %s", key))
	delete(kv.data, key)
}

func (kv *KVStore) Keys() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	keys := make([]string, 0, len(kv.data))
	for k := range kv.data {
		keys = append(keys, k)
	}
	return keys
}

func handleConn(conn net.Conn, kv *KVStore) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		switch strings.ToUpper(parts[0]) {
		case "SET":
			if len(parts) < 3 {
				fmt.Fprintln(conn, "ERR usage: SET key value")
				continue
			}
			kv.Set(parts[1], parts[2])
			fmt.Fprintln(conn, "OK")

		case "GET":
			if len(parts) < 2 {
				fmt.Fprintln(conn, "ERR usage: GET key value")
				continue
			}
			val, ok := kv.Get(parts[1])
			if !ok {
				fmt.Fprintln(conn, "NULL")
			} else {
				fmt.Fprintln(conn, val)
			}

		case "DEL":
			if len(parts) < 2 {
				fmt.Fprintln(conn, "ERR usage: DEL key value")
				continue
			}
			kv.Del(parts[1])
			fmt.Fprintln(conn, "OK")

		case "KEYS":
			fmt.Fprintln(conn, strings.Join(kv.Keys(), " "))

		default:
			fmt.Fprintln(conn, "ERR unknown command")
		}
	}
}

func main() {
	kv := NewKVStore("wal.log")

	ln, err := net.Listen("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	fmt.Println("KV store listening on :6379")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleConn(conn, kv)
	}
}
