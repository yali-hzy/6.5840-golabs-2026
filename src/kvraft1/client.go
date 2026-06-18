package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
	leaderAvailable bool
	mu              sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leaderAvailable = false
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	leader := 0
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
		ok = ck.clnt.Call(ck.servers[target], "KVServer.Get", &args, &reply)
		// fmt.Printf("Get %s got reply %+v\n", key, reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
			ck.mu.Lock()
			if !ck.leaderAvailable {
				ck.leaderAvailable = true
				ck.leader = target
			}
			ck.mu.Unlock()
			break
		}
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.mu.Lock()
			if ck.leaderAvailable && ck.leader == target {
				ck.leaderAvailable = false
				leader = 0
			} else {
				leader = (leader + 1) % len(ck.servers)
			}
			ck.mu.Unlock()
		}
	}
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	first := true
	leader := 0
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
		ok = ck.clnt.Call(ck.servers[target], "KVServer.Put", &args, &reply)
		// fmt.Printf("Put %s=%s (version %d) got reply %+v\n", key, value, version, reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			first = false
			ck.mu.Lock()
			if ck.leaderAvailable && ck.leader == target {
				ck.leaderAvailable = false
				leader = 0
			} else {
				leader = (leader + 1) % len(ck.servers)
			}
			ck.mu.Unlock()
			continue
		}
		if reply.Err != rpc.ErrWrongLeader {
			ck.mu.Lock()
			if !ck.leaderAvailable {
				ck.leaderAvailable = true
				ck.leader = target
			}
			ck.mu.Unlock()
		}
		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
			return reply.Err
		}
		if reply.Err == rpc.ErrVersion {
			if first {
				return rpc.ErrVersion
			}
			return rpc.ErrMaybe
		}
		panic("unexpected error " + string(reply.Err))
	}
}
