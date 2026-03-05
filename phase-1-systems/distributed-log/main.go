package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("usage: go run . <id> <port> [peer1] [peer2] ...")
		os.Exit(1)
	}

	id, _ := strconv.Atoi(os.Args[1])
	port := os.Args[2]
	peers := os.Args[3:]

	node := NewNode(id, peers)

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Printf("[node %d] listening on :%s\n", id, port)

	go node.runElectionTimer()

	// let nodes start up before connecting
	time.Sleep(100 * time.Millisecond)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleRPC(conn, node)
	}
}

type RPCMessage struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

func handleRPC(conn net.Conn, node *Node) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	var msg RPCMessage
	if err := json.NewDecoder(reader).Decode(&msg); err != nil {
		return
	}

	switch msg.Type {
	case "vote_request":
		var req VoteRequest
		json.Unmarshal(msg.Data, &req)
		resp := node.handleVoteRequest(req)
		json.NewEncoder(conn).Encode(resp)

	case "append_entries":
		var req AppendEntriesRequest
		json.Unmarshal(msg.Data, &req)
		resp := node.handleAppendEntries(req)
		json.NewEncoder(conn).Encode(resp)
	}
}
