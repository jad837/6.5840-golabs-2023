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

func (ae *AppendEntriesArgs) String() string {
	entriesStr := "["
	for i, entry := range ae.Entries {
		entriesStr += entry.String()
		if i < len(ae.Entries)-1 {
			entriesStr += ", "
		}
	}
	entriesStr += "]"

	return fmt.Sprintf("{Term: %d, LeaderId: %d, PrevLogIndex: %d, PrevLogTerm: %d, Entries: %s, LeaderCommit: %d}",
		ae.Term, ae.LeaderId, ae.PrevLogIndex, ae.PrevLogTerm, entriesStr, ae.LeaderCommit)
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	PeerId        int
	LastLogIndex  int
}

func (ar *AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%d,Success:%v,ConflictIndex:%d,PeerId:%d,LastLogIndex:%d}", ar.Term, ar.Success, ar.ConflictIndex, ar.PeerId, ar.LastLogIndex)
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

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply{Term: %d, VoteGranted: %t}", reply.Term, reply.VoteGranted)
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}

func (le *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %+v, Index:%d}", le.Term, le.Command, le.Index)
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
	dTester   logTopic = "TEST"
)
