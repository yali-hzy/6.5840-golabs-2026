package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	// "fmt"
	"slices"
	"time"

	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	currentCfgStr, _, err := sck.Get("config")
	// fmt.Printf("InitController: get config %s, err %v\n", currentCfgStr, err)
	if err == rpc.ErrNoKey {
		return
	}
	newCfgStr, _, err := sck.Get("new_config")
	// fmt.Printf("InitController: get new_config %s, err %v\n", newCfgStr, err)
	if err == rpc.ErrNoKey {
		return
	}
	currentCfg := shardcfg.FromString(currentCfgStr)
	newCfg := shardcfg.FromString(newCfgStr)
	if newCfg.Num > currentCfg.Num {
		sck.changeConfigTo(newCfg)
	}
}

func (sck *ShardCtrler) changeConfigTo(new *shardcfg.ShardConfig) {
	val, ver, err := sck.Get("config")
	// fmt.Printf("changeConfigTo: get config %s, ver %d, err %v\n", val, ver, err)
	if err != rpc.OK {
		panic("config not found")
	}
	old := shardcfg.FromString(val)
	cachedOldClerks := make(map[tester.Tgid]*shardgrp.Clerk)
	cachedNewClerks := make(map[tester.Tgid]*shardgrp.Clerk)
	for i := range shardcfg.NShards {
		oldGid, oldSrvs, _ := old.GidServers((shardcfg.Tshid)(i))
		newGid, newSrvs, _ := new.GidServers((shardcfg.Tshid)(i))
		if old.Shards[i] != new.Shards[i] || !slices.Equal(oldSrvs, newSrvs) {
			oldClerk, ok := cachedOldClerks[oldGid]
			if !ok {
				oldClerk = shardgrp.MakeClerk(sck.clnt, oldSrvs)
				cachedOldClerks[oldGid] = oldClerk
			}
			newClerk, ok := cachedNewClerks[newGid]
			if !ok {
				newClerk = shardgrp.MakeClerk(sck.clnt, newSrvs)
				cachedNewClerks[newGid] = newClerk
			}
			// fmt.Println()
			var state []byte
			for ; ; time.Sleep(100 * time.Millisecond) {
				state, err = oldClerk.FreezeShard((shardcfg.Tshid)(i), new.Num)
				if err != rpc.ErrWrongGroup {
					break
				}
			}
			// fmt.Printf("changeConfigTo: freeze shard %d from gid %d, state size %d, err %v\n", i, oldGid, len(state), err)
			if err != rpc.OK {
				continue
			}
			for ; ; time.Sleep(100 * time.Millisecond) {
				err = newClerk.InstallShard((shardcfg.Tshid)(i), state, new.Num)
				if err != rpc.ErrWrongGroup {
					break
				}
			}
			for ; ; time.Sleep(100 * time.Millisecond) {
				err = oldClerk.DeleteShard((shardcfg.Tshid)(i), new.Num)
				if err != rpc.ErrWrongGroup {
					break
				}
			}
		}
	}
	err = sck.Put("config", new.String(), ver)
	// fmt.Printf("changeConfigTo: put config %s, ver %d, err %v\n", new.String(), ver, err)
}

func (sck *ShardCtrler) putNewConfig(new *shardcfg.ShardConfig) bool {
	_, newVer, err := sck.Get("new_config")
	if err == rpc.ErrNoKey {
		newVer = 0
	}
	// fmt.Printf("ChangeConfigTo: get new_config ver %d new config %s, err %v\n", newVer, new.String(), err)
	if newVer != rpc.Tversion(new.Num-2) {
		return false
	}
	err = sck.Put("new_config", new.String(), newVer)
	if err == rpc.OK {
		return true
	}
	if err == rpc.ErrVersion {
		return false
	}
	if err == rpc.ErrMaybe {
		cfg, getVer, _ := sck.Get("new_config")
		if getVer == rpc.Tversion(new.Num-1) && cfg == new.String() {
			return true
		}
	}
	return false
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	config := cfg.String()
	sck.Put("config", config, 0)
	// fmt.Printf("InitConfig: put config %s\n", config)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	if !sck.putNewConfig(new) {
		return
	}
	sck.changeConfigTo(new)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	v, _, err := sck.Get("config")
	if err != rpc.OK {
		panic("config not found")
	}
	cfg := shardcfg.FromString(v)
	return cfg
}
