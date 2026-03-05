package main

import (
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command string
}

type Node struct {
	mu sync.Mutex

	// identity
	id    int
	peers []string

	// persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state
	role        Role
	commitIndex int
	lastApplied int

	// leader-only state
	nextIndex  []int
	matchIndex []int

	electionTimeout  time.Duration
	lastHeartbeat    time.Time
	heartbeatTimeout time.Duration
}

func NewNode(id int, peers []string) *Node {
	return &Node{
		id:               id,
		peers:            peers,
		votedFor:         -1,
		role:             Follower,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		electionTimeout:  randomTimeout(150, 300),
		lastHeartbeat:    time.Now(),
		heartbeatTimeout: 50 * time.Millisecond,
	}
}

func (n *Node) lastLogIndex() int {
	return len(n.log) - 1
}

func (n *Node) lastLogTerm() int {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) becomeFollower(term int) {
	n.currentTerm = term
	n.role = Follower
	n.votedFor = -1
	n.lastHeartbeat = time.Now()
	n.electionTimeout = randomTimeout(150, 300)
}
