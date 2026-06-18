package rsm

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int64
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	cond        *sync.Cond
	id          int64
	resultCh    map[int]chan any
	index2id    map[int]string
	lastApplied int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.cond = sync.NewCond(&rsm.mu)
	rsm.resultCh = make(map[int]chan any)
	rsm.index2id = make(map[int]string)
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	go rsm.reader()
	go rsm.snapshotter()
	return rsm
}

func (rsm *RSM) getId(index int64) string {
	return fmt.Sprintf("%d-%d", rsm.me, index)
}

func (rsm *RSM) snapshotter() {
	for {
		time.Sleep(100 * time.Millisecond)
		rsm.mu.Lock()
		rfStateSize := rsm.rf.PersistBytes()
		if rsm.maxraftstate != -1 && rfStateSize > rsm.maxraftstate / 10 * 9 {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(rsm.lastApplied, snapshot)
		}
		rsm.mu.Unlock()
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid && !msg.SnapshotValid {
			rsm.mu.Lock()
			// fmt.Printf("server %d apply msg: index %d, command %v\n", rsm.me, msg.CommandIndex, msg.Command)
			op := msg.Command.(Op)
			rep := rsm.sm.DoOp(op.Req)
			rsm.lastApplied = msg.CommandIndex
			index := msg.CommandIndex
			rsm.index2id[index] = rsm.getId(op.Id)
			if op.Me == rsm.me && rsm.resultCh[index] != nil {
				rsm.resultCh[index] <- rep
			}
			rsm.cond.Broadcast()
			rsm.mu.Unlock()
		} else if msg.SnapshotValid && !msg.CommandValid {
			rsm.mu.Lock()
			// fmt.Printf("server %d restore snapshot: index %d, term %d\n", rsm.me, msg.SnapshotIndex, msg.SnapshotTerm)
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastApplied = msg.SnapshotIndex
			rsm.cond.Broadcast()
			rsm.mu.Unlock()
		}
	}
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) checkLeader(term int, ch chan struct{}) {
	for {
		time.Sleep(100 * time.Millisecond)
		if currentTerm, isLeader := rsm.rf.GetState(); !isLeader || currentTerm > term {
			ch <- struct{}{}
			rsm.mu.Lock()
			rsm.cond.Broadcast()
			rsm.mu.Unlock()
			return
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.id++
	op := Op{Me: rsm.me, Id: rsm.id, Req: req}
	// fmt.Printf("server %d submit op: id %d, req %v\n", rsm.me, op.Id, req)
	index, term, isLeader := rsm.rf.Start(op)
	rsm.resultCh[index] = make(chan any, 1)
	defer delete(rsm.resultCh, index)
	defer close(rsm.resultCh[index])
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	rsm.index2id[index] = rsm.getId(op.Id)
	rsm.cond.Broadcast()
	leaderChanged := make(chan struct{}, 1)
	go rsm.checkLeader(term, leaderChanged)
	for {
		// fmt.Printf("server %d submit op: index %d, term %d, id %d\n", rsm.me, index, term, op.Id)
		select {
		case res := <-rsm.resultCh[index]:
			// fmt.Printf("server %d submit op: index %d, term %d, id %d, res %v\n", rsm.me, index, term, op.Id, res)
			return rpc.OK, res
		case <-leaderChanged:
			return rpc.ErrWrongLeader, nil
		default:
		}
		if rsm.index2id[index] != rsm.getId(op.Id) {
			// fmt.Printf("server %d submit op: index %d, term %d, id %d, wrong leader\n", rsm.me, index, term, op.Id)
			return rpc.ErrWrongLeader, nil
		}
		rsm.cond.Wait()
	}
	// return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
