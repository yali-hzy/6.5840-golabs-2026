package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"fmt"
	"sync"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	mu sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

func (ck *Clerk) getClerkByKey(key string, cacheValid bool) *shardgrp.Clerk {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	cfg := ck.sck.Query()
	shard := shardcfg.Key2Shard(key)
	gid, srvs, ok := cfg.GidServers(shard)
	// fmt.Printf("getClerkByKey: key %s, shard %d, gid %d, servers %v\n", key, shard, gid, srvs)
	if !ok {
		panic(fmt.Sprintf("Get: no group for shard %d\n", shard))
	}
	rck, ok := ck.GetClerk(gid)
	if !ok || !cacheValid {
		rck = shardgrp.MakeClerk(ck.clnt, srvs)
		ck.rcks[gid] = rck
	}
	return rck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	cacheValid := true
	for ; ; time.Sleep(100 * time.Millisecond) {
		rck := ck.getClerkByKey(key, cacheValid)
		value, version, err := rck.Get(key)
		if err != rpc.ErrWrongGroup {
			// fmt.Printf("Get: key %s, value %s, version %d, err %v\n", key, value, version, err)
			return value, version, err
		}
		cacheValid = false
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	cacheValid := true
	for ; ; time.Sleep(100 * time.Millisecond) {
		rck := ck.getClerkByKey(key, cacheValid)
		err := rck.Put(key, value, version)
		if err != rpc.ErrWrongGroup {
			if err == rpc.ErrVersion && !cacheValid {
				// fmt.Printf("Put: maybe version error for key %s, value %s, version %d\n", key, value, version)
				return rpc.ErrMaybe
			}
			// fmt.Printf("Put: key %s, value %s, version %d, err %v\n", key, value, version, err)
			return err
		}
		cacheValid = false
		// fmt.Printf("Put: ErrWrongGroup for key %s, value %s, version %d\n", key, value, version)
	}
}
