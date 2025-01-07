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
type LogEntry struct {
	Command interface{}
	Term    int
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
	// non-volatile
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile
	commitIndex     int
	lastApplied     int
	state           int8 // 0 follower, 1 candidate, 2 leader
	heartbeatTicker *time.Ticker
	electionTimeout *time.Timer

	// volatile for leader
	nextIndex  []int
	matchIndex []int

	//2B for applychan
	applyChan          chan ApplyMsg
	applyCommandsTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == 2
	return term, isleader
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	DLogF(dVote, dDebug, rf.me, "vote request myTerm %d, candidateTerm %d", rf.currentTerm, args.Term)
	if args.Term >= rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.setFollowerState(args.Term, -1)
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastTerm := 0
			lastIndex := 0
			if len(rf.log) > 0 {
				lastTerm = rf.log[len(rf.log)-1].Term
				lastIndex = len(rf.log) - 1
			}
			DLogF(dVote, dDebug, rf.me, "args.lastLogTerm %d argsLastLogIndex %d, voter %d, voterLastIndex : %d, voterLastTerm :%d", args.LastLogTerm, args.LastLogIndex, rf.me, lastIndex, lastTerm)
			if args.LastLogTerm >= lastTerm && lastIndex <= args.LastLogIndex {
				reply.VoteGranted = true
				rf.votedFor = args.CandidateId
			}
		} else {
			reply.VoteGranted = false
		}
	}
	DLogF(dVote, dInfo, rf.me, "Voted %v to candidate %d for term %d", reply.VoteGranted, args.CandidateId, args.Term)

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

/*
AppendEntriesRPC
2 modes heartbeat and appendlog
in heartbeat mode, the leader sends empty entries to all followers
in appendlog mode, the leader sends log entries to followers
*/
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

func (rf *Raft) ResetElectionTimer() {
	rf.electionTimeout.Reset(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	// first lets focus on append entries mode only.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// args.Term == rf.currentTerm && rf.state == 2 -> 2nd leader somewhere
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.state == 2) {
		DLogF(dHtbt, dInfo, rf.me, "Term mismatch denying")
		DLogF(dHtbt, dDebug, rf.me, "Term mismatch, args.Term:%d, myTerm: %d, myState:%d", args.Term, rf.currentTerm, rf.state)
		reply.Success = false
		return
	}
	rf.ResetElectionTimer()
	if args.Entries == nil {
		// considering as heartbeat, reset timer for heartbeats
		DLogF(dHtbt, dInfo, rf.me, "Accepted from %d", args.LeaderId)
		if rf.state == 1 {
			DLogF(dHtbt, dTrace, rf.me, "Changing back to follower from candidate")
			rf.setFollowerState(args.Term, rf.votedFor)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		reply.Success = true
		reply.Term = rf.currentTerm
		return
	}
	DLogF(dLog, dDebug, rf.me, "Trying to append logs")
	lastLogIndex := len(rf.log) - 1
	if args.PrevLogIndex > lastLogIndex {
		DLogF(dLog, dDebug, rf.me, "Append Failured, reason: prevLogIndex:%d > lastApplied:%d ", args.PrevLogIndex, rf.lastApplied)
		reply.Success = false
		// as for previous term logs from leader how to??
		return
	} else if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// cant proceed with appendEntries due to term mismatch
		reply.Success = false
		return
	} else {
		// agreement on logs is done now applying the logs
		if lastLogIndex < 0 {
			// initial state no logs appended so append all logs
			DLogF(dLog, dDebug, rf.me, "Initial state, Appending all log entries %d, argsprevindex %d, argsprevterm %d ", rf.lastApplied, args.PrevLogIndex, args.PrevLogTerm)
			rf.log = append(rf.log, args.Entries...)
		} else {
			DLogF(dLog, dDebug, rf.me, "Conflict index %d, argsprevindex %d, argsprevterm %d ", lastLogIndex, args.PrevLogIndex, args.PrevLogTerm)
			rf.log = append(rf.log[:lastLogIndex+1], args.Entries[lastLogIndex:]...)
		}
		rf.setFollowerState(args.Term, rf.votedFor)

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		reply.Term = rf.currentTerm
		reply.Success = true
		DLogF(dLog, dDebug, rf.me, "Log state after applying is %d, %d", len(rf.log), rf.lastApplied)
	}

}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DLogF(dLeader, dDebug, rf.me, "AppendEntries sent to %d, success: %v, reply:%v", server, ok, reply)
	return ok
}

func (rf *Raft) broadcastHeartBeat() {
	// not append entries logs
	DLogF(dHtbt, dInfo, rf.me, "Leader heartbeat broadcast")
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	DLogF(dHtbt, dDebug, rf.me, "Broadcasting heartbeat with term %d, leaderId %d, commitIndex %d", args.Term, args.LeaderId, args.LeaderCommit)
	for server := range rf.peers {
		go func(peerId int) {
			if peerId == rf.me {
				return
			}
			reply := AppendEntriesReply{}
			rf.sendAppendEntry(server, &args, &reply)
		}(server)
	}

}

type AEChanReply struct {
	MatchIndex int
	Success    bool
	NextIndex  int
	PeerId     int
	Term       int
}

func (rf *Raft) broadcastLogEntries() {
	rf.mu.Lock()
	// basic appendEntries which should be common for all servers
	// once this is done matchindex will be used to update each args based on peerid
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		Entries:      rf.log,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	DLogF(dLog, dInfo, rf.me, "Broadcasting, %d, %v", len(args.Entries), args.Entries)
	appendEntriesReplies := make(chan AEChanReply)
	successPeers := 1
	receivedPeers := 1
	// send append entries and then copy the results to make sure majority has logs then update commit index
	for server := range rf.peers {
		// DLogF(dLeader, dDebug, rf.me, "Broadcasting logs to peers")
		DLogF(dLeader, dDebug, rf.me, "Broadcasting logs with entries Term: %d, LeaderId: %d, LeaderCommit: %d", args.Term, args.LeaderId, args.LeaderCommit)

		peerNextIndex := rf.nextIndex[server]
		go func(peerId int, nextIndex int) {
			if peerId == rf.me {
				return
			}
			reply := AppendEntriesReply{}
			prevLogTerm := -1
			if nextIndex != 0 {
				prevLogTerm = args.Entries[nextIndex-1].Term
			}
			peerArgs := AppendEntriesArgs{
				Term:         args.Term,
				LeaderId:     args.LeaderId,
				Entries:      args.Entries,
				LeaderCommit: args.LeaderCommit,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  prevLogTerm,
			}
			ok := rf.sendAppendEntry(server, &peerArgs, &reply)
			if ok && reply.Success {
				// check the reply.success
				appendEntriesReplies <- AEChanReply{
					MatchIndex: len(args.Entries) - 1,
					NextIndex:  len(args.Entries),
					Success:    true,
					PeerId:     peerId,
					Term:       reply.Term,
				}
			} else {
				DLogF(dLog, dTrace, server, "failed to appendEntries for nextindex %d", nextIndex)
				appendEntriesReplies <- AEChanReply{
					Success: false,
					PeerId:  peerId,
					Term:    reply.Term,
				}
			}
		}(server, peerNextIndex)
	}
	for {
		reply := <-appendEntriesReplies
		receivedPeers++
		if reply.Success {
			DLogF(dLog, dTrace, reply.PeerId, "Successfully appended log entry for %d", reply.NextIndex)
			rf.mu.Lock()
			rf.nextIndex[reply.PeerId] = reply.NextIndex
			rf.matchIndex[reply.PeerId] = reply.MatchIndex
			rf.mu.Unlock()
			successPeers++
		} else {
			// huge gamble using -10 but couldnt logically make anything better
			rf.mu.Lock()
			rf.nextIndex[reply.PeerId] = min(0, rf.nextIndex[reply.PeerId]-10)
			rf.mu.Unlock()
		}
		if receivedPeers == len(rf.peers) {
			break
		}
	}
	DLogF(dLog, dDebug, rf.me, "Success peers is %d", successPeers)
	if successPeers > len(rf.peers)/2 {
		DLogF(dLog, dInfo, rf.me, "Majority achieved")
		// majority peers done
		rf.mu.Lock()
		if rf.state == 2 {
			rf.commitIndex = len(args.Entries) - 1
		}
		rf.mu.Unlock()
		DLogF(dLog, dInfo, rf.me, "Commit index %d, & args.Entries %d", rf.commitIndex, len(args.Entries))
	} else {
		DLogF(dLog, dDebug, rf.me, "Unable to commit index due to insufficient append entries")
	}
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
	rf.mu.Lock()
	isLeader = rf.state == 2
	rf.mu.Unlock()
	if !isLeader {
		DLogF(dClient, dDebug, rf.me, "GOT COMMAND EVEN THOUGH NOT LEADER")
		return index, term, isLeader
	}
	// Your code here (2B).
	// only if leader then process the command
	DLogF(dClient, dInfo, rf.me, "Received command")
	rf.mu.Lock()
	rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
	index = len(rf.log) - 1
	term = rf.currentTerm
	rf.mu.Unlock()
	DLogF(dLeader, dDebug, rf.me, "Received command currently log size %d", len(rf.log))
	// as I believe that log is updated, I will send append entries to all followers
	go rf.broadcastLogEntries()
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
	defer rf.mu.Unlock()
	rf.electionTimeout.Stop()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) setCandidateState() {
	// promote to candidate
	rf.state = 1
	rf.currentTerm++
	//self voting
	rf.votedFor = rf.me
}

func (rf *Raft) setFollowerState(term int, votedFor int) {
	rf.state = 0
	rf.currentTerm = term
	rf.votedFor = votedFor
}

func (rf *Raft) initializeCandidateState() *RequestVoteArgs {
	rf.setCandidateState()
	lastLogTerm := 0
	lastLogIndex := 0

	if len(rf.log) > 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
		lastLogIndex = len(rf.log) - 1
	}

	voteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	return &voteArgs
}

func (rf *Raft) conductVoting(voteArgs *RequestVoteArgs) bool {
	voteReceived := 1
	voteGranted := 1
	voteResultChan := make(chan bool)
	rf.mu.Lock()
	totalPeers := len(rf.peers)
	rf.mu.Unlock()

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		go func(peerId int) {
			DLogF(dVote, dDebug, rf.me, "Vote asked to %d", peerId)
			voteReply := RequestVoteReply{}
			ok := rf.sendRequestVote(peerId, voteArgs, &voteReply)
			if ok {
				voteResultChan <- voteReply.VoteGranted
			} else {
				voteResultChan <- false
			}
		}(peer)
	}

	// calculate results
	DLogF(dVote, dInfo, rf.me, "start of calculating voting results")
	for {
		result := <-voteResultChan
		voteReceived++
		if result {
			voteGranted++
		}
		if voteGranted > totalPeers/2 {
			break
		}
		if voteReceived >= totalPeers {
			break
		}
	}
	return voteGranted > totalPeers/2
}

func (rf *Raft) conductElection() {
	rf.mu.Lock()
	rf.ResetElectionTimer()
	if rf.state == 2 {
		// you are leader dont need to do election here
		DLogF(dVote, dDebug, rf.me, "Abandoning election as I am leader")
		rf.mu.Unlock()
		return
	}
	DLogF(dVote, dInfo, rf.me, "Conducting election")
	voteArgs := rf.initializeCandidateState()
	rf.mu.Unlock()
	hasMajority := rf.conductVoting(voteArgs)

	rf.mu.Lock()
	if rf.state != 1 {
		rf.mu.Unlock()
		DLogF(dVote, dDebug, rf.me, "Candidacy revoked for term %d", rf.currentTerm)
	} else if rf.state == 1 && hasMajority {
		rf.state = 2
		rf.ResetElectionTimer()
		rf.mu.Unlock()
		DLogF(dVote, dInfo, rf.me, "Elected as leader for term %v ", rf.currentTerm)
		rf.broadcastHeartBeat()
	} else if rf.state == 1 && !hasMajority {
		rf.mu.Unlock()
		DLogF(dVote, dDebug, rf.me, "Lost election for term :%d ", rf.currentTerm)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.electionTimeout.C:
			rf.conductElection()
		case <-rf.heartbeatTicker.C:
			DLogF(dHtbt, dInfo, rf.me, "Ticked")
			DLogF(dHtbt, dDebug, rf.me, "Commit index %d, lastApplied %d", rf.commitIndex, rf.lastApplied)
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			if state == 2 {
				rf.broadcastHeartBeat()
			}
			// case <-rf.applyCommandsTimer.C:
			// 	go rf.applyCommands()
		}

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyCommands() {
	DLogF(dApplyMsg, dInfo, rf.me, "Apply msg state %d, %d ", rf.lastApplied, rf.commitIndex)
	rf.applyCommandsTimer.Reset(50 * time.Millisecond)
	if rf.lastApplied < rf.commitIndex {

		rf.mu.Lock()
		startIndex := rf.lastApplied + 1
		endIndex := rf.commitIndex
		rf.mu.Unlock()
		DLogF(dApplyMsg, dInfo, rf.me, "Aply Msg from: %d to: %d", startIndex, endIndex)
		for i := startIndex + 1; i <= endIndex; i++ {
			rf.applyChan <- ApplyMsg{
				Command:      rf.log[i].Command,
				CommandIndex: i,
			}
			rf.mu.Lock()
			rf.lastApplied++
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

	// Your initialization code here (2A, 2B, 2C).
	rf.state = 0
	rf.votedFor = -1
	rf.currentTerm = 1
	rf.electionTimeout = time.NewTimer(time.Duration(300+rand.Int31n(150)) * time.Millisecond)
	rf.heartbeatTicker = time.NewTicker(100 * time.Millisecond)

	// 2B
	rf.log = make([]LogEntry, 0)
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = 0
	}
	rf.applyChan = applyCh
	rf.applyCommandsTimer = time.NewTimer(20 * time.Millisecond)

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
