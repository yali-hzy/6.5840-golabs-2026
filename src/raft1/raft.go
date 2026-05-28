package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command any
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders: (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	state          int // 0: follower, 1: candidate, 2: leader
	needElectionCh chan struct{}
	heartbeatCh    chan struct{}
	electionTimer  int64
	heartbeatTimer int64
	online         []bool
	snapshotMsg    raftapi.ApplyMsg
	applySnapshot  bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
		rf.lastApplied = snapshotIndex
		rf.commitIndex = snapshotIndex
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Printf("%d receive snapshot request with index %d and snapshot size %d\n", rf.me, index, len(snapshot))
	// fmt.Printf("%d current snapshotIndex: %d, snapshotTerm: %d, log len: %d\n", rf.me, rf.snapshotIndex, rf.snapshotTerm, len(rf.log))
	// fmt.Printf("%d log: %v\n", rf.me, rf.log)
	if index <= rf.snapshotIndex {
		return
	}
	rf.snapshotTerm = rf.getLogEntry(index).Term
	rf.log = append([]LogEntry{{
		Term: rf.snapshotTerm,
	}}, rf.log[index-rf.snapshotIndex+1:]...)
	rf.snapshotIndex = index
	rf.snapshot = snapshot
	// fmt.Printf("%d after snapshot, snapshotIndex: %d, snapshotTerm: %d, log len: %d\n", rf.me, rf.snapshotIndex, rf.snapshotTerm, len(rf.log))
	// fmt.Printf("%d log: %v\n", rf.me, rf.log)
	rf.persist()
}

func (rf *Raft) getLastLogInfo() (int, int) {
	return rf.snapshotIndex + len(rf.log) - 1, rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLogLen() int {
	return len(rf.log) + rf.snapshotIndex
}

func (rf *Raft) getLogEntry(index int) LogEntry {
	if index < rf.snapshotIndex {
		panic(fmt.Sprintf("%d getLogEntry: index %d is less than snapshotIndex %d, commitIndex: %d, lastApplied: %d", rf.me, index, rf.snapshotIndex, rf.commitIndex, rf.lastApplied))
	}
	return rf.log[index-rf.snapshotIndex]
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Printf("%d receive InstallSnapshot from %d at term %d, lastIncludedIndex: %d, lastIncludedTerm: %d, snapshot size: %d\n", rf.me, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.electionTimer = 0
	if rf.state == 1 {
		rf.state = 0
	}
	if args.LastIncludedIndex <= rf.snapshotIndex {
		return
	}
	if args.LastIncludedIndex < rf.getLogLen() && rf.getLogEntry(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log = append([]LogEntry{{
			Term: args.LastIncludedTerm,
		}}, rf.log[args.LastIncludedIndex-rf.snapshotIndex+1:]...)
	} else {
		rf.log = []LogEntry{{
			Term: args.LastIncludedTerm,
		}}
	}
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()
	rf.snapshotMsg = raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applySnapshot = true
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// fmt.Printf("%d(%d) send InstallSnapshot to %d at term %d, lastIncludedIndex: %d, lastIncludedTerm: %d, snapshot size: %d\n", rf.me, rf.state, server, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, len(args.Data))
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	// fmt.Printf("%d(%d) receive InstallSnapshot reply from %d at term %d\n", rf.me, rf.state, server, reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		rf.online[server] = false
		return
	}
	rf.online[server] = true
	rf.checkTerm(reply.Term)
	if rf.state != 2 || args.Term != rf.currentTerm {
		return
	}
	rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	rf.sendLogHelper(server)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// fmt.Printf("%d(%d) receive RequestVote from %d at term %d\n", rf.me, rf.state, args.CandidateId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	rf.checkTerm(args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		rf.electionTimer = 0
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		return
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ch chan bool) bool {
	// fmt.Printf("%d(%d) send RequestVote to %d at term %d with lastLogIndex %d and lastLogTerm %d\n", rf.me, rf.state, server, args.Term, args.LastLogIndex, args.LastLogTerm)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// fmt.Printf("%d(%d) receive RequestVote reply from %d at term %d: %v\n", rf.me, rf.state, server, reply.Term, reply.VoteGranted)
	if !ok {
		return false
	}
	rf.mu.Lock()
	rf.checkTerm(reply.Term)
	rf.mu.Unlock()
	ch <- reply.VoteGranted
	return ok
}

func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = 0
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = 2
	rf.heartbeatTimer = 0
	rf.electionTimer = 0
	select {
	case rf.heartbeatCh <- struct{}{}:
	default:
	}
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLogLen()
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) startElection() {
	// fmt.Printf("%d start election at term %d\n", rf.me, rf.currentTerm+1)
	rf.mu.Lock()
	if rf.state != 1 {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	votes := 1
	rf.electionTimer = rand.Int63() % 150
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	rf.persist()
	rf.mu.Unlock()
	ch := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply, ch)
		}
	}
	timeout := false
	for i := 0; i < len(rf.peers)-1; i++ {
		// fmt.Printf("%d wait for vote %d\n", rf.me, i)
		select {
		case ok := <-ch:
			if ok {
				votes++
			}
		case <-rf.needElectionCh:
			timeout = true
		}
		rf.mu.Lock()
		if rf.state != 1 || term != rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if votes > len(rf.peers)/2 || timeout {
			break
		}
		// fmt.Printf("%d receive vote %d, total votes: %d\n", rf.me, i, votes)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if votes > len(rf.peers)/2 && rf.state == 1 && term == rf.currentTerm {
		rf.becomeLeader()
	}
	if rf.state == 1 && timeout {
		select {
		case rf.needElectionCh <- struct{}{}:
		default:
		}
	}
	// fmt.Printf("election result: %d(%d) get %d votes\n", rf.me, rf.state, votes)
}

func (rf *Raft) applyLoop() {
	for {
		rf.applyCond.L.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.applySnapshot {
			rf.applyCond.Wait()
		}
		var msgs []raftapi.ApplyMsg
		if rf.applySnapshot {
			// fmt.Printf("%d apply snapshot at index %d with snapshot size %d\n", rf.me, rf.snapshotIndex, len(rf.snapshot))
			rf.applySnapshot = false
			msgs = []raftapi.ApplyMsg{rf.snapshotMsg}
			rf.lastApplied = rf.snapshotMsg.SnapshotIndex
			rf.commitIndex = max(rf.commitIndex, rf.lastApplied)
		} else {
			msgs = make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				msgs = append(msgs, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.getLogEntry(i).Command,
					CommandIndex: i,
				})
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.applyCond.L.Unlock()
		for _, msg := range msgs {
			// fmt.Printf("raft %d apply %v at index %d\n", rf.me, msg, msg.CommandIndex)
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) updateCommitIndex(index int) {
	rf.commitIndex = index
	rf.applyCond.Signal()
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
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) getAppendEntriesArgs(server int) AppendEntriesArgs {
	if rf.nextIndex[server] <= rf.snapshotIndex {
		panic(fmt.Sprintf("getAppendEntriesArgs: nextIndex %d of server %d is less than or equal to snapshotIndex %d", rf.nextIndex[server], server, rf.snapshotIndex))
	}
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.getLogEntry(prevLogIndex).Term
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      slices.Clone(rf.log[rf.nextIndex[server]-rf.snapshotIndex:]),
		LeaderCommit: rf.commitIndex,
	}
}

func logEntryCmp(e LogEntry, t int) int {
	return e.Term - t
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("%d receive AppendEntries from %d at term %d, prevLogIndex: %d, prevLogTerm: %d, entries: %v, leaderCommit: %d\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.persist()
	rf.checkTerm(args.Term)
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XLen = rf.getLogLen()
	if args.Term < rf.currentTerm {
		return
	}
	rf.electionTimer = 0
	if rf.state == 1 {
		rf.state = 0
	}
	if args.PrevLogIndex >= rf.getLogLen() {
		return
	}
	if args.PrevLogIndex < rf.snapshotIndex {
		reply.XTerm = rf.snapshotTerm
		reply.XIndex = rf.snapshotIndex
		return
	}
	prevLogIndex := args.PrevLogIndex - rf.snapshotIndex
	if rf.log[prevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.log[prevLogIndex].Term
		reply.XIndex, _ = slices.BinarySearchFunc(rf.log, reply.XTerm, logEntryCmp)
		reply.XIndex += rf.snapshotIndex
		return
	}
	for i := 0; i < len(args.Entries); i++ {
		if prevLogIndex+1+i < len(rf.log) {
			if rf.log[prevLogIndex+1+i].Term != args.Entries[i].Term {
				rf.log = rf.log[:prevLogIndex+1+i]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		rf.updateCommitIndex(min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
	}
	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) getInstallSnapshotArgs() InstallSnapshotArgs {
	return InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotIndex,
		LastIncludedTerm:  rf.snapshotTerm,
		Data:              rf.snapshot,
	}
}

func (rf *Raft) sendLogHelper(server int) {
	if rf.nextIndex[server] <= rf.snapshotIndex {
		args := rf.getInstallSnapshotArgs()
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
		go rf.sendInstallSnapshot(server, &args, &reply)
	} else {
		args := rf.getAppendEntriesArgs(server)
		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		go rf.sendAppendEntries(server, &args, &reply)
	}
	rf.mu.Lock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("%d(%d) send append entries to %d at term %d, prevLogIndex: %d, prevLogTerm: %d, entries: %v, leaderCommit: %d\n", rf.me, rf.state, server, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// fmt.Printf("%d(%d) receive append entries reply from %d at term %d: success %v\n", rf.me, rf.state, server, reply.Term, reply.Success)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		rf.online[server] = false
		return
	}
	rf.checkTerm(reply.Term)
	if rf.state != 2 || args.Term != rf.currentTerm {
		return
	}
	if reply.Success {
		rf.matchIndex[server] = max(rf.matchIndex[server], args.PrevLogIndex+len(args.Entries))
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		// fmt.Printf("%d update nextIndex of server %d to %d and matchIndex to %d\n", rf.me, server, rf.nextIndex[server], rf.matchIndex[server])
		matchIndexes := make([]int, len(rf.peers))
		copy(matchIndexes, rf.matchIndex)
		matchIndexes[rf.me] = rf.getLogLen() - 1
		sort.Ints(matchIndexes)
		n := matchIndexes[len(rf.peers)/2]
		if n > rf.commitIndex && rf.getLogEntry(n).Term == rf.currentTerm {
			rf.updateCommitIndex(n)
		}
		if !rf.online[server] {
			rf.sendLogHelper(server)
		}
		rf.online[server] = true
	} else {
		rf.online[server] = true
		if reply.XTerm != 0 {
			_, found := slices.BinarySearchFunc(rf.log, reply.XTerm, logEntryCmp)
			if found {
				rf.nextIndex[server], _ = slices.BinarySearchFunc(rf.log, reply.XTerm+1, logEntryCmp)
				rf.nextIndex[server] += rf.snapshotIndex
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		} else if reply.XLen > 0 {
			rf.nextIndex[server] = reply.XLen
		} else {
			panic("AppendEntries fail with no XTerm and XLen")
		}
		// fmt.Printf("%d update nextIndex of server %d to %d\n", rf.me, server, rf.nextIndex[server])
		// rf.nextIndex[server] = rf.nextIndex[server] - 1
		// if rf.nextIndex[server] < 1 {
		// 	panic(fmt.Sprintf("nextIndex of server %d is less than 1", server))
		// }
		rf.sendLogHelper(server)
	}
}

func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	// fmt.Printf("%d(%d) send heartbeat at term %d\n", rf.me, rf.state, rf.currentTerm)
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	rf.heartbeatTimer = 0
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			rf.mu.Lock()
			if !rf.online[i] {
				if rf.nextIndex[i] <= rf.snapshotIndex {
					args := rf.getInstallSnapshotArgs()
					reply := InstallSnapshotReply{}
					rf.mu.Unlock()
					go rf.sendInstallSnapshot(i, &args, &reply)
				} else {
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  rf.getLogEntry(rf.nextIndex[i] - 1).Term,
						Entries:      []LogEntry{},
						LeaderCommit: rf.commitIndex,
					}
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &args, &reply)
				}
				continue
			}
			rf.sendLogHelper(i)
			rf.mu.Unlock()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// fmt.Printf("raft: %d receive new command %v at term %d log len: %d\n", rf.me, command, rf.currentTerm, rf.getLogLen())
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == 2
	if isLeader {
		index = rf.getLogLen()
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term:    term,
			Command: command,
		})
		rf.persist()
		for i := range rf.peers {
			if i != rf.me {
				// fmt.Printf("%d receive new command, send append entries to %d at term %d, prevLogIndex: %d, prevLogTerm: %d, entries: %v, leaderCommit: %d snapshotIndex: %d\n", rf.me, i, rf.currentTerm, rf.nextIndex[i]-1, rf.getLogEntry(rf.nextIndex[i]-1).Term, rf.log[rf.nextIndex[i]-rf.snapshotIndex:], rf.commitIndex, rf.snapshotIndex)
				rf.sendLogHelper(i)
			}
		}
	}

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// fmt.Printf("%d state: %d, election timer: %d, heartbeat timer: %d\n", rf.me, rf.state, rf.electionTimer, rf.heartbeatTimer)
		if rf.state != 2 && rf.electionTimer > 1000 {
			rf.state = 1
			select {
			case rf.needElectionCh <- struct{}{}:
			default:
			}
		}
		if rf.state == 2 && rf.heartbeatTimer > 100 {
			select {
			case rf.heartbeatCh <- struct{}{}:
			default:
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		if rf.state != 2 {
			rf.electionTimer += ms
		}
		if rf.state == 2 {
			rf.heartbeatTimer += ms
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) loop() {
	for {
		select {
		case <-rf.needElectionCh:
			// fmt.Printf("%d start election\n", rf.me)
			rf.startElection()
			// fmt.Printf("%d end election\n", rf.me)
		case <-rf.heartbeatCh:
			// fmt.Printf("%d send heartbeat\n", rf.me)
			rf.heartbeat()
			// fmt.Printf("%d end heartbeat\n", rf.me)
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.needElectionCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	rf.online = make([]bool, len(peers))
	rf.online[me] = true
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.loop()
	go rf.applyLoop()

	return rf
}
