package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type valueVersion struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	kvmap map[string]valueVersion
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
		reply := rpc.GetReply{}
		if v, ok := kv.kvmap[req.Key]; ok {
			reply.Value = v.value
			reply.Version = v.version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		key := req.Key
		value := req.Value
		version := req.Version
		if v, ok := kv.kvmap[key]; ok {
			if version != v.version {
				reply.Err = rpc.ErrVersion
				return reply
			}
			kv.kvmap[key] = valueVersion{
				value:   value,
				version: v.version + 1,
			}
			reply.Err = rpc.OK
		} else {
			if version != 0 {
				reply.Err = rpc.ErrNoKey
				return reply
			}
			kv.kvmap[key] = valueVersion{
				value:   value,
				version: 1,
			}
			reply.Err = rpc.OK
		}
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
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
		reply.Err = rpc.OK
	}
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
	kv.kvmap = make(map[string]valueVersion)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
