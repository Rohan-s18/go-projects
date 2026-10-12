package pbservice

import (
	"coms4113/hw2/src/viewservice"
	"crypto/rand"
	"fmt"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	vs *viewservice.Clerk
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	return ck
}

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	x, _ := rand.Int(rand.Reader, max)
	return x.Int64()
}

func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key}

	for {
		view, ok := ck.vs.Get()
		if ok && view.Primary != "" {
			var reply GetReply
			if call(view.Primary, "PBServer.Get", args, &reply) {
				if reply.Err == OK {
					return reply.Value
				}
				if reply.Err == ErrNoKey {
					return ""
				}
			}
		}
		time.Sleep(viewservice.PingInterval)
	}
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	args := &PutArgs{
		Key:     key,
		Value:   value,
		DoHash:  dohash,
		Request: nrand(),
	}

	for {
		view, ok := ck.vs.Get()
		if ok && view.Primary != "" {
			var reply PutReply
			if call(view.Primary, "PBServer.Put", args, &reply) && reply.Err == OK {
				return reply.PreviousValue
			}
		}
		time.Sleep(viewservice.PingInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
	return ck.PutExt(key, value, true)
}
