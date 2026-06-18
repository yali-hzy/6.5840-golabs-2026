package kvraft

import (
	"bytes"
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu    sync.Mutex
	kvmap map[string]ValueVersion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&req)
	case rpc.PutArgs:
		return kv.doPut(&req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Printf("server %d snapshot: kvmap %v\n", kv.me, kv.kvmap)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvmap)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Printf("server %d restore: data %v\n", kv.me, data)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[string]ValueVersion
	if d.Decode(&kvmap) != nil {
		fmt.Printf("server %d restore: decode error\n", kv.me)
	} else {
		kv.kvmap = kvmap
	}
	// fmt.Printf("server %d restore: kvmap %v\n", kv.me, kv.kvmap)
}

func (kv *KVServer) doGet(args *rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.GetReply{}
	if v, ok := kv.kvmap[args.Key]; ok {
		reply.Value = v.Value
		reply.Version = v.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
	return reply
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
	} else {
		reply.Value = rep.(rpc.GetReply).Value
		reply.Version = rep.(rpc.GetReply).Version
		reply.Err = rep.(rpc.GetReply).Err
	}
}

func (kv *KVServer) doPut(args *rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := rpc.PutReply{}
	key := args.Key
	value := args.Value
	version := args.Version
	// fmt.Printf("before server %d doPut: key %s, value %s, version %d, kvmap %v\n", kv.me, key, value, version, kv.kvmap)
	// defer fmt.Printf("after server %d doPut: key %s, value %s, version %d, kvmap %v\n", kv.me, key, value, version, kv.kvmap)
	if v, ok := kv.kvmap[key]; ok {
		if version != v.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}
		kv.kvmap[key] = ValueVersion{
			Value:   value,
			Version: v.Version + 1,
		}
		reply.Err = rpc.OK
	} else {
		if version != 0 {
			reply.Err = rpc.ErrNoKey
			return reply
		}
		kv.kvmap[key] = ValueVersion{
			Value:   value,
			Version: 1,
		}
		reply.Err = rpc.OK
	}
	return reply
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
	} else {
		reply.Err = rep.(rpc.PutReply).Err
	}
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.kvmap = make(map[string]ValueVersion)
	kv.mu = sync.Mutex{}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.Restore(snapshot)
	}
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
