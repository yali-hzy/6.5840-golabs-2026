package shardgrp

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	leaderAvailable bool
	mu              sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) tryNextLeader(leader int) int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if ck.leaderAvailable && ck.leader == leader {
		ck.leaderAvailable = false
		return 0
	} else {
		return (leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) foundLeader(leader int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	if !ck.leaderAvailable {
		ck.leaderAvailable = true
		ck.leader = leader
	}
}

func getErrPtr(reply any) *rpc.Err {
	switch reply := reply.(type) {
	case *rpc.GetReply:
		return &reply.Err
	case *rpc.PutReply:
		return &reply.Err
	case *shardrpc.FreezeShardReply:
		return &reply.Err
	case *shardrpc.InstallShardReply:
		return &reply.Err
	case *shardrpc.DeleteShardReply:
		return &reply.Err
	default:
		panic(fmt.Sprintf("unexpected reply type %T", reply))
	}
}

func (ck *Clerk) callLoop(method string, args any, reply any, putLike bool) {
	leader := 0
	first := true
	attempts := 0
	for ; ; time.Sleep(100 * time.Millisecond) {
		var ok bool
		var target int
		ck.mu.Lock()
		if !ck.leaderAvailable {
			target = leader
		} else {
			target = ck.leader
		}
		ck.mu.Unlock()
		// fmt.Printf("callLoop: %v call %s to server %d with args %v\n", ck, method, target, args)
		ok = ck.Call(ck.servers[target], method, args, reply)
		// fmt.Printf("callLoop: %v call %s to server %d got reply %v, ok %v\n", ck, method, target, reply, ok)
		if ok {
			attempts = 0
		}
		if !ok || (*getErrPtr(reply) == rpc.ErrWrongLeader) {
			leader = ck.tryNextLeader(target)
			if !ok {
				attempts++
				if attempts > len(ck.servers) {
					*getErrPtr(reply) = rpc.ErrWrongGroup
					break
				}
			}
			first = false
			continue
		}
		if ok && (*getErrPtr(reply) != rpc.ErrWrongLeader) {
			ck.foundLeader(leader)
			break
		}
	}
	if putLike && *getErrPtr(reply) == rpc.ErrVersion && !first {
		*getErrPtr(reply) = rpc.ErrMaybe
	}
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	ck.callLoop("KVServer.Get", &args, &reply, false)
	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	ck.callLoop("KVServer.Put", &args, &reply, true)
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}
	ck.callLoop("KVServer.FreezeShard", &args, &reply, false)
	// fmt.Printf("Clerk.FreezeShard: shard %d, num %d, state size %d, err %v\n", s, num, len(reply.State), reply.Err)
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}
	ck.callLoop("KVServer.InstallShard", &args, &reply, false)
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}
	ck.callLoop("KVServer.DeleteShard", &args, &reply, false)
	return reply.Err
}
