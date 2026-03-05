# serious-scaling-spark

---

## Phase 1 — Systems Foundations

### KV Store
- In-memory key-value store over raw TCP
- Commands: GET, SET, DEL, KEYS
- WAL (write-ahead log) for crash recovery and restart persistence
- RWMutex for concurrent client handling
- Replication: leader forwards writes to follower node over TCP
- Verified: WAL survives kill/restart, mutex blocks concurrent writers, follower stays in sync

### Distributed Log — Raft
- 3-node Raft implementation in Go
- Leader election with randomized timeouts (150–300ms)
- Heartbeat-based log replication (50ms interval)
- AppendEntries + RequestVote RPCs over TCP/JSON
- Verified: leader elected on startup, new election triggered on leader death, restarted node rejoins as follower

---

## Up Next
- custom RPC framework (retries, timeouts, load balancing)
- Linux systems (perf, strace, epoll)