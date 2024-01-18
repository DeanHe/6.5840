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

	"6.5840/labrpc"
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// Raft server states.
const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER
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

	state     int
	voteCount int

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Channels between raft peers
	chanApply     chan ApplyMsg
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := (rf.state == STATE_LEADER)
	return term, isleader
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
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
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// reject request with stale term number
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		// turn to follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		// vote for the candidate
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}
}

// check if candidate's log is at least as new as the voter.
func (rf *Raft) isUpToDate(candidateTerm int, candidateLogIndex int) bool {
	return rf.getLastLogTerm() < candidateTerm ||
		(rf.getLastLogTerm() == candidateTerm && rf.getLastLogIndex() <= candidateLogIndex)
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
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.state != STATE_CANDIDATE || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if rf.currentTerm < reply.Term {
		// turn to follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.voteCount = 0
		return ok
	}

	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			// win the election
			rf.chanWinElect <- true
		}
	}

	return ok
}

// broadcast RequestVote RPCs to all peers in parallel.
func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != STATE_CANDIDATE {
		return
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args, &RequestVoteReply{})
		}
	}
}

// AppendEntries RPC arguments structure.
// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PreLogIndex       int
	PreLogTerm        int
	LeaderCommitIndex int
	Entries           []LogEntry
}

// AppendEntries RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

// AppendEntries RPC body.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	if args.Term < rf.currentTerm {
		// reject requests with stale term
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if rf.currentTerm < args.Term {
		// turn to follower and update current term
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.voteCount = 0
	}

	rf.chanHeartbeat <- true
	reply.Term = rf.currentTerm
	if rf.getLastLogIndex() < args.PreLogIndex {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	startLogIndex := rf.log[0].Index
	if startLogIndex <= args.PreLogIndex && rf.log[args.PreLogIndex-startLogIndex].Term != args.PreLogTerm {
		// if entry log[prevLogIndex] term conflicts with new one, there may be conflict entries before.
		// find the last term synced log entry and overwrite from there.
		conflictTerm := rf.log[args.PreLogIndex-startLogIndex].Term
		for i := args.PreLogIndex - 1; i >= startLogIndex; i-- {
			if rf.log[i-startLogIndex].Term != conflictTerm {
				reply.NextTryIndex = i + 1
				break
			}
		}

	} else if startLogIndex-1 <= args.PreLogIndex {
		// otherwise log up to prevLogIndex are safe.
		// merge lcoal log and entries from leader, and apply log if commitIndex changes.
		rf.log = rf.log[:args.PreLogIndex-startLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextTryIndex = args.PreLogIndex + len(args.Entries)
		if rf.commitIndex < args.LeaderCommitIndex {
			// update commitIndex and apply log
			rf.commitIndex = min(args.LeaderCommitIndex, rf.getLastLogIndex())
			go rf.applyLog()
		}

	}
}

// send a AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		return ok
	}

	if rf.currentTerm < reply.Term {
		// turn to follower
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.voteCount = 0
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.getLastLogIndex())
	}

	// find if there are more than half peers received logs to update commitIndex
	startLogIndex := rf.log[0].Index
	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-startLogIndex].Term == rf.currentTerm; N-- {
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}

	return ok
}

// broadcast AppendEntries RPCs to all peers in parallel.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != STATE_LEADER {
		return
	}

	startLogIndex := rf.log[0].Index
	for server := range rf.peers {
		if server != rf.me {
			if startLogIndex < rf.nextIndex[server] {
				args := AppendEntriesArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					PreLogIndex:       rf.nextIndex[server] - 1,
					LeaderCommitIndex: rf.commitIndex,
				}
				if startLogIndex <= args.PreLogIndex {
					args.PreLogTerm = rf.log[args.PreLogIndex-startLogIndex].Term
				}
				if rf.nextIndex[server] <= rf.getLastLogIndex() {
					args.Entries = rf.log[rf.nextIndex[server]-startLogIndex:]
				}
				go rf.sendAppendEntries(server, &args, &AppendEntriesReply{})
			} else {
				//todo
			}
		}
	}
}

// apply log entries with index in range [lastApplied + 1, commitIndex]
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	startLogIndex := rf.log[0].Index
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandIndex: i,
			CommandValid: true,
			Command:      rf.log[i-startLogIndex].Command,
		}
		rf.chanApply <- msg
		rf.lastApplied = rf.commitIndex
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := (rf.state == STATE_LEADER)

	if isLeader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// pause for a random amount of time between 200 and 500
	// milliseconds.
	ms := 200 + (rand.Int63() % 300)

	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		switch rf.state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Duration(ms) * time.Millisecond):
				// turn to candidate when not receive appendEntries from Leader
				rf.mu.Lock()
				rf.state = STATE_CANDIDATE
				rf.mu.Unlock()
			}
		case STATE_CANDIDATE:
			// reset to candidate again
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.mu.Unlock()
			go rf.broadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				// turn to follower when receive appendEntries from Leader
				rf.mu.Lock()
				rf.state = STATE_FOLLOWER
				rf.votedFor = -1
				rf.voteCount = 0
				rf.mu.Unlock()
			case <-rf.chanWinElect:
				// turn to Leader
				rf.mu.Lock()
				rf.state = STATE_LEADER
				rf.persist()
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for nb := range rf.nextIndex {
					rf.nextIndex[nb] = nextIndex
				}
				rf.mu.Unlock()
			case <-time.After(time.Duration(ms) * time.Millisecond):
			}
		case STATE_LEADER:
			go rf.broadcastAppendEntries()
			time.Sleep(60 * time.Millisecond)
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
	rf.state = STATE_FOLLOWER
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool)
	rf.chanWinElect = make(chan bool)
	rf.chanHeartbeat = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// rf.recoverFromSnapshot(persister.ReadSnapshot())
	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
