package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	state       int
	votedFor    int
	log         []LogEntry

	//volatile
	commitIndex int
	lastApplied int

	//volatile leader state
	nextIndex  []int
	matchIndex []int

	// timers
	electionTimer   *time.Timer
	heartbeatTicker *time.Ticker
	electionCancel  context.CancelFunc
	electionCtx     context.Context
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) setFollowerState(term int, votedFor int) {
	rf.state = Follower
	rf.votedFor = votedFor
	rf.currentTerm = term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// TODO's add log mismatch, before voting
	if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		DLogF(dVote, dDebug, rf.me, "Granted vote again for this term %d, to %d", args.Term, rf.votedFor)
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}

	if args.Term < rf.currentTerm || (rf.currentTerm == args.Term && rf.votedFor != -1) {
		DLogF(dVote, dDebug, rf.me, "Rejected to=%d, term=%d, alreadyVotedForterm=%v", args.CandidateId, args.Term, rf.currentTerm == args.Term && rf.votedFor != -1)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		DLogF(dVote, dDebug, rf.me, "Higher term found, perhaps new leader leadterm=%d, myterm=%d", args.Term, rf.currentTerm)
		rf.currentTerm = args.Term

		rf.votedFor = -1
		rf.state = Follower
		rf.ResetElectionTime()
	}
	// what are the chances that election timeout goes out right in this patch? not many but still too many, thats why called resetElectionTimer
	if rf.votedFor == -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.ResetElectionTime()
		DLogF(dVote, dDebug, rf.me, "Granted to:%d, for term:%d", rf.votedFor, rf.currentTerm)
	} else {
		DLogF(dVote, dDebug, rf.me, "Already voted for this term %d, to %d", args.Term, rf.votedFor)
	}
}

func getRandomizedElectionTimer() time.Duration {
	ms := time.Duration(250 + rand.Int()%100)
	return time.Duration(ms * time.Millisecond)
}

func getHeartbeattimer() time.Duration {
	return time.Duration(100 * time.Millisecond)
}

func (rf *Raft) ResetElectionTime() {
	ms := getRandomizedElectionTimer()
	DLogF(dTimer, dDebug, rf.me, "Electiontimer reset to %v", ms)
	rf.electionTimer.Reset(ms)
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		DLogF(dLog, dDebug, rf.me, "Rejecting AppendEntries from=%d", args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	reply.Term = args.Term
	if args.Term > rf.currentTerm {
		// I am lagging as leader so I should abandone being leader
		DLogF(dLog, dDebug, rf.me, "New leader found, leader=%d, term=%d", args.LeaderId, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower
	if len(args.Entries) == 0 {
		// empty log Heartbeat
		DLogF(dHtbt, dDebug, rf.me, "Received from=%d", args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.ResetElectionTime()
		return
	}
}

func (rf *Raft) GetLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) setCandidateState() {
	DLogF(dElec, dDebug, rf.me, "Going candidate..")
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) conductElection() {
	rf.mu.Lock()
	if rf.state != Leader {
		if rf.electionCancel != nil {
			DLogF(dElec, dDebug, rf.me, "Timeout when conducting election term=%d", rf.currentTerm)
			rf.electionCancel()
			rf.electionCancel = nil
		}

		rf.electionCtx, rf.electionCancel = context.WithTimeout(context.Background(), time.Duration(200*time.Millisecond))
		rf.ResetElectionTime()

		rf.setCandidateState()
		myTerm := rf.currentTerm

		args := &RequestVoteArgs{
			Term:        myTerm,
			CandidateId: rf.me,
		}
		rf.mu.Unlock()

		resultChan := make(chan *RequestVoteReply, len(rf.peers)-1)
		var wg sync.WaitGroup
		// ask for votes using goroutine
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, args, &reply)
				if !ok {
					reply.VoteGranted = false
					reply.Term = -1
				}
				resultChan <- &reply
			}(server)
		}
		wg.Wait()
		close(resultChan)
		//self voting

		votesGranted := 1

		for result := range resultChan {
			if result.VoteGranted {
				votesGranted++
			} else if result.Term > args.Term {
				//abandone election as someone with higher term is already a leader
				DLogF(dElec, dDebug, rf.me, "Abandone Counting votes leaderterm=%d", result.Term)
				rf.mu.Lock()
				if rf.currentTerm < result.Term {
					rf.setFollowerState(result.Term, -1)
					rf.ResetElectionTime()
				}
				rf.mu.Unlock()
				//Although you wont be getting votes just settings this to 1 so as to remove funny business
				votesGranted = 1
				break
			}
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if votesGranted > len(rf.peers)/2 && rf.state == Candidate {
			DLogF(dElec, dDebug, rf.me, "Election won...")
			rf.electionCancel = nil
			rf.setLeaderState()
			rf.ResetElectionTime()
			go rf.broadcastHeartbeat()
			return
		} else {
			rf.electionCancel = nil
			DLogF(dElec, dDebug, rf.me, "Election lost...state=%v,votesG=%d", rf.state, votesGranted)
			return
		}
	} else {
		rf.mu.Unlock()
		DLogF(dLog, dDebug, rf.me, "Election timeout popped for leader")
	}
}

func (rf *Raft) broadcastHeartbeat() {
	rf.mu.Lock()
	logs := make([]LogEntry, 0)
	args := &AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		Entries:  logs,
	}
	rf.mu.Unlock()
	DLogF(dHtbt, dDebug, rf.me, "Sent htbt")
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, args, &reply)
		}(server)
	}
}

func (rf *Raft) setLeaderState() {
	rf.state = Leader
	// start heartbeattimer?
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.state = Follower
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if !rf.killed() {
				go rf.conductElection()
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			if !rf.killed() && rf.state == Leader {
				go rf.broadcastHeartbeat()
			}
			rf.mu.Unlock()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.heartbeatTicker = time.NewTicker(getHeartbeattimer())
	rf.electionTimer = time.NewTimer(getRandomizedElectionTimer())
	rf.setFollowerState(1, -1)
	rf.log = make([]LogEntry, 0)

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
