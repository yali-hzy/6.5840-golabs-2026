package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.


import (
	//	"bytes"
	// "fmt"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)


type LogEntry struct {
	Term int
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

	state int // 0: follower, 1: candidate, 2: leader
	needElectionCh chan struct{}
	heartbeatCh chan struct{}
	electionTimer int64
	heartbeatTimer int64
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

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term 	     int
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
	rf.checkTerm(args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && true { // TODO
		rf.electionTimer = 0
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
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
	// fmt.Printf("%d(%d) send RequestVote to %d at term %d\n", rf.me, rf.state, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// fmt.Printf("%d(%d) receive RequestVote reply from %d at term %d: %v\n", rf.me, rf.state, server, reply.Term, reply.VoteGranted)
	rf.mu.Lock()
	rf.checkTerm(reply.Term)
	rf.mu.Unlock()
	ch <- ok && reply.VoteGranted
	return ok
}


func (rf *Raft) checkTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = 0
		rf.votedFor = -1
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
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}


func (rf *Raft) startElection() {
	// fmt.Printf("%d start election at term %d\n", rf.me, rf.currentTerm + 1)
	rf.mu.Lock()
	rf.electionTimer = rand.Int63() % 150
	rf.votedFor = rf.me
	rf.state = 1
	rf.currentTerm ++
	term := rf.currentTerm
	rf.mu.Unlock()
	votes := 1
	ch := make(chan bool, len(rf.peers) - 1)
	for i := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term: term,
				CandidateId: rf.me,
				LastLogIndex: 0, // TODO
				LastLogTerm: 0, // TODO
			}
			reply := RequestVoteReply{}
			go rf.sendRequestVote(i, &args, &reply, ch)
		}
	}
	for i := 0; i < len(rf.peers) - 1; i++ {
		// fmt.Printf("%d wait for vote %d\n", rf.me, i)
		timeout := false
		select {
		case ok := <-ch:
			if ok {
				votes ++
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
		if votes > len(rf.peers) / 2 || timeout {
			break
		}
		// fmt.Printf("%d receive vote %d, total votes: %d\n", rf.me, i, votes)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if votes > len(rf.peers) / 2 && rf.state == 1 && term == rf.currentTerm {
		rf.becomeLeader()
	}
	if rf.state == 1 {
		select {
		case rf.needElectionCh <- struct{}{}:
		default:
		}
	}
	// fmt.Printf("election result: %d(%d) get %d votes\n", rf.me, rf.state, votes)
}


type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("%d receive heartbeat from %d at arg term %d with term %d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkTerm(args.Term)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.electionTimer = 0
	if rf.state == 1 {
		rf.state = 0
	}
	// TODO
	reply.Term = rf.currentTerm
	reply.Success = true
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Printf("%d send heartbeat to %d at term %d\n", rf.me, server, args.Term)
	rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// fmt.Printf("%d receive heartbeat reply from %d at term %d: %v\n", rf.me, server, reply.Term, reply.Success)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.checkTerm(reply.Term)
}


func (rf *Raft) heartbeat() {
	rf.mu.Lock()
	if rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	rf.heartbeatTimer = 0
	rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me {
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
				LeaderId: rf.me,
				PrevLogIndex: 0, // TODO
				PrevLogTerm: 0, // TODO
				Entries: []LogEntry{},
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
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

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.state == 2
	if isLeader {
		index = len(rf.log)
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{
			Term: term,
			Command: command,
		})
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
		// case <-rf.appendEntriesCh:
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.needElectionCh = make(chan struct{}, 1)
	rf.heartbeatCh = make(chan struct{}, 1)
	go rf.loop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
