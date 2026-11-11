package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "coms4113/hw3/src/paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "time"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type    string
	Key     string
	Value   string
	DoHash  bool
	Request int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	data    map[string]string
	results map[int64]PutReply
	nextSeq int
}

func (kv *KVPaxos) wait(seq int, op Op) Op {
	delay := 10 * time.Millisecond
	for {
		decided, value := kv.px.Status(seq)
		if decided {
			return value.(Op)
		}
		kv.px.Start(seq, op)
		time.Sleep(delay)
		if delay < time.Second {
			delay *= 2
		}
	}
}

func (kv *KVPaxos) apply(op Op) {
	if op.Type != "Put" {
		return
	}
	if _, duplicate := kv.results[op.Request]; duplicate {
		return
	}

	previous := kv.data[op.Key]
	value := op.Value
	reply := PutReply{Err: OK}
	if op.DoHash {
		value = strconv.Itoa(int(hash(previous + op.Value)))
		reply.PreviousValue = previous
	}
	kv.data[op.Key] = value
	kv.results[op.Request] = reply
}

func (kv *KVPaxos) run(op Op) {
	for {
		if op.Type == "Put" {
			if _, duplicate := kv.results[op.Request]; duplicate {
				return
			}
		}

		seq := kv.nextSeq
		chosen := kv.wait(seq, op)
		kv.apply(chosen)
		kv.px.Done(seq)
		kv.nextSeq++

		if chosen.Request == op.Request {
			return
		}
	}
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.run(Op{Type: "Get", Key: args.Key, Request: args.Request})
	value, exists := kv.data[args.Key]
	if !exists {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Err = OK
	reply.Value = value
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.run(Op{
		Type:    "Put",
		Key:     args.Key,
		Value:   args.Value,
		DoHash:  args.DoHash,
		Request: args.Request,
	})
	*reply = kv.results[args.Request]
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.data = make(map[string]string)
	kv.results = make(map[int64]PutReply)

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
