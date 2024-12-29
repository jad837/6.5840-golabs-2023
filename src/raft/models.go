package raft

type AppendEntriesRequest struct {
	term              uint64
	leaderId          uint64
	prevLogIndex      int
	prevLogTerm       uint64
	entries           []LogEntry
	leaderCommitIndex uint64
}

type AppendEntriesResponse struct {
	term    uint64
	success bool
}
