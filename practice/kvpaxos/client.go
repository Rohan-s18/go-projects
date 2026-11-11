package kvpaxos

import "crypto/rand"
import "fmt"
import "math/big"
import "net/rpc"
import "time"

type Clerk struct {
	servers []string
	next    int
}

func requestID() int64 {
	max := big.NewInt(int64(1) << 62)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	return n.Int64()
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
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

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, Request: requestID()}
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.next + i) % len(ck.servers)
			var reply GetReply
			if call(ck.servers[server], "KVPaxos.Get", args, &reply) &&
				(reply.Err == OK || reply.Err == ErrNoKey) {
				ck.next = (server + 1) % len(ck.servers)
				return reply.Value
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// set the value for a key.
// keeps trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	args := &PutArgs{Key: key, Value: value, DoHash: dohash, Request: requestID()}
	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.next + i) % len(ck.servers)
			var reply PutReply
			if call(ck.servers[server], "KVPaxos.Put", args, &reply) && reply.Err == OK {
				ck.next = (server + 1) % len(ck.servers)
				return reply.PreviousValue
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
