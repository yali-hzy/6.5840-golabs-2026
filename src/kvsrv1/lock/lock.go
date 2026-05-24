package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	name string
	id   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name = lockname
	lk.id = kvtest.RandValue(8)
	lk.ck.Put(lk.name, "unlocked", 0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		v, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			panic("no such lock " + lk.name)
		}
		if err == rpc.OK {
			switch v {
			case "unlocked":
				err = lk.ck.Put(lk.name, lk.id, ver)
				switch err {
				case rpc.OK:
					return
				case rpc.ErrMaybe:
					v, ver, err = lk.ck.Get(lk.name)
					if err == rpc.OK && v == lk.id {
						return
					}
				}
			case lk.id:
				panic("lock " + lk.name + " is already held by this client")
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		v, ver, err := lk.ck.Get(lk.name)
		if err == rpc.ErrNoKey {
			panic("no such lock " + lk.name)
		}
		if err == rpc.OK {
			switch v {
			case lk.id:
				err = lk.ck.Put(lk.name, "unlocked", ver)
				switch err {
				case rpc.OK:
					return
				case rpc.ErrMaybe:
					v, ver, err = lk.ck.Get(lk.name)
					if err == rpc.OK && v != lk.id {
						return
					}
				}
			case "unlocked":
				panic("lock " + lk.name + " is already unlocked")
			default:
				panic("lock " + lk.name + " is held by another client")
			}
		}
	}
}
