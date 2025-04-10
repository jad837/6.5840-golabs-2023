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
	electionTimer     *time.Timer
	heartbeatTicker   *time.Ticker
	commitIndexTicker *time.Ticker

	applyChannel chan ApplyMsg
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

func (rf *Raft) isLogUptodate(candidateTerm int, candidateIndex int) bool {
	if candidateTerm == rf.GetLastLogTerm() {
		return candidateIndex >= rf.GetLastLogIndex()
	}
	return candidateTerm > rf.GetLastLogTerm()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		DLogF(dVote, dDebug, rf.me, "Higher term found, perhaps new leader leadterm=%d, myterm=%d", args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.ResetElectionTime()
	}
	reply.Term = rf.currentTerm
	// what are the chances that election timeout goes out right in this patch? not many but still too many, thats why called resetElectionTimer
	if !rf.isLogUptodate(args.LastLogTerm, args.LastLogIndex) {
		DLogF(dVote, dDebug, rf.me, "Log is forward than candidate:{id:%d,lastTerm:%d,lastIndex:%d} me:{lastIndex:%d,lastTerm:%d}", args.CandidateId, args.LastLogTerm, args.LastLogIndex, rf.GetLastLogIndex(), rf.GetLastLogTerm())
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isLogUptodate(args.LastLogTerm, args.LastLogIndex) && args.Term >= rf.currentTerm {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.ResetElectionTime()
		DLogF(dVote, dDebug, rf.me, "Granted to:%d, for term:%d", rf.votedFor, rf.currentTerm)
	} else {
		DLogF(dVote, dDebug, rf.me, "Vote denied for term %d, to %d, alreadyVoted:%d", args.Term, args.CandidateId, rf.votedFor)
		reply.VoteGranted = false
	}
}

func getRandomizedElectionTimer() time.Duration {
	ms := time.Duration(400 + rand.Int()%100)
	return time.Duration(ms * time.Millisecond)
}

func getHeartbeattimer() time.Duration {
	return time.Duration(200 * time.Millisecond)
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
	DLogF(dLog, dTrace, rf.me, "AppendEntries received :%v", args)
	if args.Term < rf.currentTerm {
		DLogF(dLog, dDebug, rf.me, "Rejecting AppendEntries from=%d", args.LeaderId)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if args.Term > rf.currentTerm {
		// I am lagging as leader so I should abandone being leader
		DLogF(dLog, dDebug, rf.me, "New leader found, leader=%d, term=%d", args.LeaderId, args.Term)
		rf.setFollowerState(args.Term, -1)
	}

	rf.ResetElectionTime()
	reply.Term = rf.currentTerm

	/*
		// if my GetLastLogIndex = 0 when no logs are there.
		if this is anything its painful because of how bad I have written this.
		if my PrevLogIndex = 1 then my GetLastLogIndex can be equal to 0
		How do you solve this shit problem?
	*/
	if args.PrevLogIndex > rf.GetLastLogIndex() {
		// prevLogIndex is not included in my rf.log
		// prevLogIndex is greater means my logIndex should be the lastLogIndex for this whole sham
		DLogF(dLog, dDebug, rf.me, "PrevLogIndex is too high, conflict index will be my last log")
		reply.Success = false
		reply.ConflictIndex = rf.GetLastLogIndex()
		return
	}

	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// now I can match the index and term
		// DLogF(dLog, dTrace, rf.me, "Checking log mismatch")
		conflictIndex := args.PrevLogIndex - 1
		conflictTerm := rf.log[conflictIndex].Term
		for conflictIndex > 0 && rf.log[conflictIndex].Term == conflictTerm {
			conflictIndex--
		}
		DLogF(dLog, dDebug, rf.me, "MismatchIndex: %d, MismatchTerm: %d", conflictIndex, conflictTerm)
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		return
	}

	if args.PrevLogIndex < rf.GetLastLogIndex() {
		DLogF(dLog, dDebug, rf.me, "Removing conflicting entries {prevLogIndex: %d, lastLogIndex:%d}", args.PrevLogIndex, rf.GetLastLogIndex())
		rf.log = rf.log[:args.PrevLogIndex]
	}

	DLogF(dLog, dDebug, rf.me, "Entries added %v", len(args.Entries))

	//appended new entries
	rf.log = append(rf.log, args.Entries...)
	rf.commitIndex = min(args.LeaderCommit, rf.GetLastLogIndex())
	reply.Success = true
	reply.ConflictIndex = -1
	reply.Term = rf.currentTerm
	reply.LastLogIndex = rf.GetLastLogIndex()
	reply.PeerId = rf.me
	DLogF(dLog, dDebug, rf.me, "Success for logs from leader :%d, commitIndex:%d", args.LeaderId, rf.commitIndex)
	//TODO's Call applyMessages coroutine
	go rf.applyLog()
}

func (rf *Raft) GetLastLogTerm() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) GetLastLogIndex() int {
	if len(rf.log) == 0 {
		return 0
	}
	return rf.log[len(rf.log)-1].Index
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
		rf.ResetElectionTime()
		rf.setCandidateState()

		myTerm := rf.currentTerm
		args := &RequestVoteArgs{
			Term:        myTerm,
			CandidateId: rf.me,
		}
		rf.mu.Unlock()

		resultChan := make(chan *RequestVoteReply)
		// ask for votes using goroutine
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			rf.mu.Lock()
			peerArgs := &RequestVoteArgs{
				Term:         args.Term,
				CandidateId:  args.CandidateId,
				LastLogIndex: rf.GetLastLogIndex(),
				LastLogTerm:  rf.GetLastLogTerm(),
			}
			rf.mu.Unlock()
			go func(server int, resultChan chan *RequestVoteReply) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, peerArgs, &reply)
				if !ok {
					reply.VoteGranted = false
					reply.Term = -1
				}
				resultChan <- &reply
			}(server, resultChan)
		}
		//self voting

		votesGranted := 1
		votesReceived := 1
		for {
			result := <-resultChan
			DLogF(dLeader, dDebug, rf.me, "received from channel %v", result)
			votesReceived++
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
				break
			} else {
				DLogF(dElec, dDebug, rf.me, "Didnt receive")
			}
			if votesReceived == len(rf.peers) || votesGranted > len(rf.peers)/2 {
				break
			}
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if votesGranted > len(rf.peers)/2 && rf.state == Candidate {
			DLogF(dElec, dDebug, rf.me, "Election won...")
			rf.setLeaderState()
			DLogF(dElec, dTrace, rf.me, "Leader state after winning commitIndex:%d, term:%d, lastApplied:%d", rf.commitIndex, rf.currentTerm, rf.lastApplied)
			DLogF(dElec, dTrace, rf.me, "Leader Logs after winning %v", rf.log)
			rf.ResetElectionTime()
			go rf.broadcastLogEntries()
			return
		} else {
			DLogF(dElec, dDebug, rf.me, "Election lost...state=%v,votesG=%d", rf.state, votesGranted)
			return
		}
	} else {
		rf.mu.Unlock()
		DLogF(dLog, dDebug, rf.me, "Election timeout popped for leader")
	}
}

func (rf *Raft) sendAppendEntryToPeer(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	rf.mu.Lock()
	peerNextIndex := rf.nextIndex[server]
	prevLogIndex := max(peerNextIndex-1, 0)
	prevLogTerm := 0
	entries := make([]LogEntry, 0)
	entries = append(entries, rf.log...)
	if prevLogIndex != 0 {
		prevLogTerm = rf.log[prevLogIndex-1].Term
		//Handling of these log indexes to manage the actual thing..
		entries = rf.log[peerNextIndex-1:]
		DLogF(dLog, dDebug, rf.me, "Entries exchange: entries:%v peerNextIndex:%d, prevLogIndex:%d, prevLogTerm%d", entries, peerNextIndex, prevLogIndex, prevLogTerm)

	}
	rf.mu.Unlock()
	peerArgs := &AppendEntriesArgs{
		LeaderId:     args.LeaderId,
		Term:         args.Term,
		LeaderCommit: args.LeaderCommit,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}

	ok := rf.sendAppendEntries(server, peerArgs, reply)
	reply.PeerId = server
	if !ok {
		DLogF(dLog, dDebug, rf.me, "Critical AppendEntries failed to peer %d", server)
		return
	}
	rf.mu.Lock()

	if reply.Term != rf.currentTerm {
		if reply.Term > rf.currentTerm {
			// bigger than me means that its a leader as well
			DLogF(dLog, dTrace, rf.me, "Bigger term, big boss leader found maybe myTerm:%d hisTerm:%d", rf.currentTerm, reply.Term)
			rf.setFollowerState(reply.Term, -1)
		} else {
			// only for logging
			DLogF(dLog, dTrace, rf.me, "Term mismatch maybe old logging reply")
		}
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		rf.nextIndex[reply.PeerId] = reply.LastLogIndex + 1
		rf.matchIndex[reply.PeerId] = reply.LastLogIndex
		rf.mu.Unlock()
	} else {
		DLogF(dLog, dDebug, rf.me, "ConflictIndex found %d", reply.ConflictIndex)
		rf.nextIndex[reply.PeerId] = reply.ConflictIndex
		rf.matchIndex[reply.PeerId] = max(0, reply.ConflictIndex-1)
		rf.mu.Unlock()
	}
}

// should only be used when its a leader
func (rf *Raft) monitorCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextCommitIndex := rf.commitIndex + 1
	for !rf.killed() && rf.state == Leader && nextCommitIndex <= rf.GetLastLogIndex() && rf.logIndexCopiedAcrossPeers(nextCommitIndex) {
		DLogF(dCommit, dTrace, rf.me, "Update commit index: {oldIndex:%d, newIndex:%d}", rf.commitIndex, rf.commitIndex+1)
		rf.commitIndex++
		nextCommitIndex++
	}
	go rf.applyLog()
}

func (rf *Raft) logIndexCopiedAcrossPeers(index int) bool {
	updatedPeers := 1.0
	totalPeers := len(rf.peers)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.matchIndex[peer] >= index {
			updatedPeers++
		}
		DLogF(dLog, dTrace, rf.me, "Peer test {peer:%d, totalPeers:%d, updatedPeers:%f, matchIndex:%v}", peer, totalPeers, updatedPeers, rf.matchIndex)
	}
	DLogF(dApplyMsg, dDebug, rf.me, "Peers updated with commitIndex:%d => %v, can apply msg %v", index, updatedPeers, (updatedPeers / float64(totalPeers) / 2))
	return updatedPeers > float64(totalPeers)/2
}

func (rf *Raft) broadcastLogEntries() {
	// calculate log entries to send using nextIndex and matchIndex.
	// use the reply gotten from the peer to update nextIndex, matchIndex & commitIndex/lastApplied
	rf.mu.Lock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	DLogF(dLog, dDebug, rf.me, "Send Logs")
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendAppendEntryToPeer(server, args)
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	for rf.commitIndex > rf.lastApplied {
		DLogF(dCommit, dDebug, rf.me, "ApplyState:{lastApplied=%d, commit=%d}", rf.lastApplied, rf.commitIndex)
		DLogF(dApplyMsg, dDebug, rf.me, "Applying msg %d", rf.lastApplied+1)
		rf.lastApplied++
		rf.applyChannel <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied-1].Command,
			CommandIndex: rf.lastApplied,
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) setLeaderState() {
	rf.state = Leader
	// start heartbeattimer?
	// reset indexes
	rf.initializeNextAndMatch()
}

func (rf *Raft) initializeNextAndMatch() {
	nextIndex := rf.GetLastLogIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = nextIndex
	}
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
	term := 0
	isLeader := false
	term, isLeader = rf.GetState()
	// Your code here (2B).
	if isLeader {
		rf.mu.Lock()
		entry := LogEntry{Command: command, Term: term, Index: rf.GetLastLogIndex() + 1}
		rf.log = append(rf.log, entry)
		DLogF(dApplyMsg, dDebug, rf.me, "Command Appended at Index %v", rf.GetLastLogIndex())

		index = rf.GetLastLogIndex()
		rf.mu.Unlock()
		go rf.broadcastLogEntries()
	}
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
	for !rf.killed() {
		select {
		case <-rf.electionTimer.C:
			go rf.conductElection()
		case <-rf.heartbeatTicker.C:
			rf.mu.Lock()
			if rf.state == Leader {
				go rf.broadcastLogEntries()
			}
			rf.mu.Unlock()
		case <-rf.commitIndexTicker.C:
			go rf.monitorCommitIndex()
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
	rf.commitIndexTicker = time.NewTicker(time.Duration(300) * time.Millisecond)
	rf.setFollowerState(1, -1)
	rf.log = make([]LogEntry, 0)
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyChannel = applyCh
	rf.lastApplied = 0
	rf.commitIndex = 0
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
