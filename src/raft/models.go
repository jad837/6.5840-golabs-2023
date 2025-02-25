package raft

import "fmt"

type AEChanReply struct {
	MatchIndex int
	Success    bool
	NextIndex  int
	PeerId     int
	Term       int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply{Term: %d, VoteGranted: %t}", reply.Term, reply.VoteGranted)
}

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Leader    int = 2
	Candidate int = 1
	Follower  int = 0
)

const (
	dTrace logLevel = "TRACE"
	dDebug logLevel = "DEBUG"
	dWarn  logLevel = "WARN"
	dInfo  logLevel = "INFO"
	dError logLevel = "ERROR"
)

const (
	dClient   logTopic = "CLNT"
	dVote     logTopic = "VOTE"
	dElec     logTopic = "ELEC"
	dTerm     logTopic = "TERM"
	dLeader   logTopic = "LEAD"
	dHtbt     logTopic = "HTBT"
	dCommit   logTopic = "CMIT"
	dDrop     logTopic = "DROP"
	dPersist  logTopic = "PERS"
	dSnap     logTopic = "SNAP"
	dTimer    logTopic = "TIMR"
	dLog      logTopic = "LOG"
	dApplyMsg logTopic = "APMC"
)
