package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesResponse struct {
	Term    int
	Success bool
}

func (n *Node) runHeartbeat() {
	for {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return
		}
		fmt.Printf("[node %d] sending heartbeat (term %d)\n", n.id, n.currentTerm)
		n.mu.Unlock()

		for _, peer := range n.peers {
			go n.sendAppendEntries(peer)
		}
		time.Sleep(n.heartbeatTimeout)
	}
}

func (n *Node) sendAppendEntries(peer string) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}

	peerIdx := n.peerIndex(peer)
	prevLogIndex := n.nextIndex[peerIdx] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	}

	entries := []LogEntry{}
	if n.nextIndex[peerIdx] < len(n.log) {
		entries = n.log[n.nextIndex[peerIdx]:]
	}

	req := AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	n.mu.Unlock()

	resp, err := sendAppendEntriesRPC(peer, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		n.matchIndex[peerIdx] = prevLogIndex + len(entries)
		n.nextIndex[peerIdx] = n.matchIndex[peerIdx] + 1
		n.maybeCommit()
	} else {
		if n.nextIndex[peerIdx] > 0 {
			n.nextIndex[peerIdx]--
		}
	}
}

func (n *Node) maybeCommit() {
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue
		}
		count := 1
		for _, m := range n.matchIndex {
			if m >= idx {
				count++
			}
		}
		if count > (len(n.peers)+1)/2 {
			n.commitIndex = idx
			fmt.Printf("[node %d] committed up tp index %d\n", n.id, idx)
			break
		}
	}
}

func (n *Node) handleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return AppendEntriesResponse{Term: n.currentTerm, Success: false}
	}
	n.becomeFollower(req.Term) // resets election timer

	// check prevLogIndex/prevLogTerm consistency
	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= len(n.log) {
			return AppendEntriesResponse{Term: n.currentTerm, Success: false}
		}
		if n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
			n.log = n.log[:req.PrevLogIndex] // delete conflicting entries
			return AppendEntriesResponse{Term: n.currentTerm, Success: false}
		}
	}

	// append new entries
	n.log = append(n.log[:req.PrevLogIndex+1], req.Entries...)

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, len(n.log)-1)
	}

	return AppendEntriesResponse{Term: n.currentTerm, Success: true}
}

func (n *Node) peerIndex(addr string) int {
	for i, p := range n.peers {
		if p == addr {
			return i
		}
	}
	return -1
}

func sendAppendEntriesRPC(addr string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer conn.Close()

	msg := map[string]interface{}{"type": "append_entries", "data": req}
	json.NewEncoder(conn).Encode(msg)

	var resp AppendEntriesResponse
	json.NewDecoder(conn).Decode(&resp)
	return resp, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
