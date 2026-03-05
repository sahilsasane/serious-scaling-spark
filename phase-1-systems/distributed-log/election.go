package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"time"
)

type VoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResponse struct {
	Term        int
	VoteGranted bool
}

func randomTimeout(minMs, maxMs int) time.Duration {
	return time.Duration(minMs+rand.Intn(maxMs-minMs)) * time.Millisecond
}

func (n *Node) runElectionTimer() {
	for {
		time.Sleep(10 * time.Millisecond)
		n.mu.Lock()
		if n.role == Leader {
			n.mu.Unlock()
			return
		}
		if time.Since(n.lastHeartbeat) >= n.electionTimeout {
			n.mu.Unlock()
			n.startElection()
			return
		}
		n.mu.Unlock()
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.role = Candidate
	n.currentTerm++
	n.votedFor = n.id
	term := n.currentTerm
	lastIndex := n.lastLogIndex()
	lastTerm := n.lastLogTerm()

	n.mu.Unlock()
	fmt.Printf("[node %d] starting election for term %d\n", n.id, term)

	votes := 1
	total := len(n.peers) + 1
	majority := total/2 + 1

	type result struct{ granted bool }
	results := make(chan result, len(n.peers))

	for _, peer := range n.peers {
		go func(peer string) {
			req := VoteRequest{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			resp, err := sendVoteRequest(peer, req)
			if err != nil {
				results <- result{false}
				return
			}
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term)
				n.mu.Unlock()
				results <- result{false}
				return
			}
			n.mu.Unlock()
			results <- result{resp.VoteGranted}
		}(peer)
	}

	for i := 0; i < len(n.peers); i++ {
		r := <-results
		if r.granted {
			votes++
		}
		if votes >= majority {
			n.mu.Lock()
			if n.role == Candidate && n.currentTerm == term {
				n.becomeLeader()
			}
			n.mu.Unlock()
			return
		}
	}

	n.mu.Lock()
	if n.role == Candidate {
		n.role = Follower
		n.lastHeartbeat = time.Now()
		n.electionTimeout = randomTimeout(150, 300)
	}
	n.mu.Unlock()
	go n.runElectionTimer()
}

func (n *Node) becomeLeader() {
	n.role = Leader
	for i := range n.nextIndex {
		n.nextIndex[i] = len(n.log)
		n.matchIndex[i] = -1
	}
	fmt.Printf("[node %d] become leader for term %d\n", n.id, n.currentTerm)
	go n.runHeartbeat()
}

func (n *Node) handleVoteRequest(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return VoteResponse{Term: n.currentTerm, VoteGranted: false}
	}
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	logOk := req.LastLogIndex > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if (n.votedFor == -1 || n.votedFor == req.CandidateID) && logOk {
		n.votedFor = req.CandidateID
		n.lastHeartbeat = time.Now()
		return VoteResponse{Term: n.currentTerm, VoteGranted: true}
	}
	return VoteResponse{Term: n.currentTerm, VoteGranted: false}
}

func sendVoteRequest(addr string, req VoteRequest) (VoteResponse, error) {
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		return VoteResponse{}, err
	}
	defer conn.Close()

	msg := map[string]interface{}{"type": "vote_request", "data": req}
	json.NewEncoder(conn).Encode(msg)

	var resp VoteResponse
	json.NewDecoder(conn).Decode(&resp)
	return resp, nil
}
