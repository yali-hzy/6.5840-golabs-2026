package shardgrp

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

const (
	NoShard     = 0
	ValidShard  = 1
	FrozenShard = 2
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu    sync.Mutex
	kvmap map[string]ValueVersion

	shardState   [shardcfg.NShards]int
	maxNum4Shard [shardcfg.NShards]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.doGet(&req)
	case rpc.PutArgs:
		return kv.doPut(&req)
	case shardrpc.FreezeShardArgs:
		return kv.doFreezeShard(&req)
	case shardrpc.InstallShardArgs:
		return kv.doInstallShard(&req)
	case shardrpc.DeleteShardArgs:
		return kv.doDeleteShard(&req)
	default:
		panic(fmt.Sprintf("unexpected request type %T", req))
	}
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
	if kv.shardState[shardcfg.Key2Shard(args.Key)] != ValidShard {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
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
	// Your code here
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
	if kv.shardState[shardcfg.Key2Shard(args.Key)] != ValidShard {
		reply.Err = rpc.ErrWrongGroup
		return reply
	}
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
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
	} else {
		reply.Err = rep.(rpc.PutReply).Err
	}
}

func (kv *KVServer) doFreezeShard(args *shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := shardrpc.FreezeShardReply{}
	if kv.maxNum4Shard[args.Shard] > args.Num || kv.shardState[args.Shard] == NoShard {
		reply.Err = rpc.ErrVersion
		reply.Num = kv.maxNum4Shard[args.Shard]
		return reply
	}
	kv.shardState[args.Shard] = FrozenShard
	kv.maxNum4Shard[args.Shard] = args.Num
	shardValues := make(map[string]ValueVersion)
	for k, v := range kv.kvmap {
		if shardcfg.Key2Shard(k) == args.Shard {
			shardValues[k] = v
		}
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardValues)
	reply.State = w.Bytes()
	reply.Num = kv.maxNum4Shard[args.Shard]
	reply.Err = rpc.OK
	return reply
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
	} else {
		reply.State = rep.(shardrpc.FreezeShardReply).State
		reply.Num = rep.(shardrpc.FreezeShardReply).Num
		reply.Err = rep.(shardrpc.FreezeShardReply).Err
	}
	// fmt.Printf("FreezeShard: server %d FreezeShard: shard %d, state size %d, err %v\n", kv.me, args.Shard, len(reply.State), reply.Err)
}

func (kv *KVServer) doInstallShard(args *shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := shardrpc.InstallShardReply{}
	if kv.maxNum4Shard[args.Shard] >= args.Num {
		reply.Err = rpc.ErrVersion
		return reply
	}
	kv.maxNum4Shard[args.Shard] = args.Num
	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)
	var shardValues map[string]ValueVersion
	err := d.Decode(&shardValues)
	if err != nil {
		log.Fatalf("server %d doInstallShard: decode shard state error: %v, State size: %d", kv.me, err, len(args.State))
		reply.Err = rpc.ErrVersion
		return reply
	}
	for k, v := range shardValues {
		kv.kvmap[k] = v
	}
	kv.shardState[args.Shard] = ValidShard
	kv.maxNum4Shard[args.Shard] = args.Num
	reply.Err = rpc.OK
	return reply
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
	} else {
		reply.Err = rep.(shardrpc.InstallShardReply).Err
	}
}

func (kv *KVServer) doDeleteShard(args *shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply := shardrpc.DeleteShardReply{}
	if kv.shardState[args.Shard] != FrozenShard || kv.maxNum4Shard[args.Shard] > args.Num {
		reply.Err = rpc.ErrVersion
		return reply
	}
	for k := range kv.kvmap {
		if shardcfg.Key2Shard(k) == args.Shard {
			delete(kv.kvmap, k)
		}
	}
	kv.shardState[args.Shard] = NoShard
	kv.maxNum4Shard[args.Shard] = args.Num
	reply.Err = rpc.OK
	return reply
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
	} else {
		reply.Err = rep.(shardrpc.DeleteShardReply).Err
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(ValueVersion{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	kv.kvmap = make(map[string]ValueVersion)
	kv.mu = sync.Mutex{}
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.Restore(snapshot)
	}
	if gid == shardcfg.Gid1 {
		for i := range shardcfg.NShards {
			kv.shardState[i] = ValidShard
			kv.maxNum4Shard[i] = shardcfg.NumFirst
		}
	}
	// fmt.Printf("StartServerShardGrp: gid %d, me %d, snapshot %v, kvmap %v\n", gid, me, snapshot, kv.kvmap)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
