package raft

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

type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Leader    uint8 = 2
	Candidate uint8 = 1
	Follower  uint8 = 0
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
	dCommit   logTopic = "CMIT"
	dDrop     logTopic = "DROP"
	dLeader   logTopic = "LEAD"
	dPersist  logTopic = "PERS"
	dSnap     logTopic = "SNAP"
	dTerm     logTopic = "TERM"
	dTimer    logTopic = "TIMR"
	dLog      logTopic = "LOG"
	dHtbt     logTopic = "HTBT"
	dApplyMsg logTopic = "APMC"
)
