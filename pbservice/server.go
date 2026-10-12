package pbservice

import (
	"coms4113/hw2/src/viewservice"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool
	unreliable bool
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	opMu       sync.Mutex
	mu         sync.Mutex
	view       viewservice.View
	data       map[string]string
	results    map[int64]PutReply
	ready      bool
}

func (pb *PBServer) applyPut(args *PutArgs) PutReply {
	if reply, ok := pb.results[args.Request]; ok {
		return reply
	}

	previous := pb.data[args.Key]
	value := args.Value
	if args.DoHash {
		value = strconv.Itoa(int(hash(previous + args.Value)))
	}
	result := PutReply{Err: OK, PreviousValue: previous}
	pb.data[args.Key] = value
	pb.results[args.Request] = result
	return result
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	pb.opMu.Lock()
	defer pb.opMu.Unlock()

	for {
		pb.mu.Lock()
		if !pb.ready || pb.view.Primary != pb.me {
			reply.Err = ErrWrongServer
			pb.mu.Unlock()
			return nil
		}
		view := pb.view
		pb.mu.Unlock()

		if view.Backup != "" {
			forwardArgs := &ForwardPutArgs{
				Args:    *args,
				Viewnum: view.Viewnum,
				Primary: pb.me,
			}
			var forwardReply PutReply
			if !call(view.Backup, "PBServer.ForwardPut", forwardArgs, &forwardReply) || forwardReply.Err != OK {
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		pb.mu.Lock()
		if !pb.ready || pb.view != view || pb.view.Primary != pb.me {
			pb.mu.Unlock()
			reply.Err = ErrWrongServer
			return nil
		}
		*reply = pb.applyPut(args)
		pb.mu.Unlock()
		return nil
	}
}

func (pb *PBServer) ForwardPut(args *ForwardPutArgs, reply *PutReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.ready ||
		pb.view.Viewnum != args.Viewnum ||
		pb.view.Primary != args.Primary ||
		pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	*reply = pb.applyPut(&args.Args)
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.ready || pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	if pb.view.Backup != "" {
		forwardArgs := &ForwardGetArgs{
			Args:    *args,
			Viewnum: pb.view.Viewnum,
			Primary: pb.me,
		}
		var forwardReply GetReply
		if !call(pb.view.Backup, "PBServer.ForwardGet", forwardArgs, &forwardReply) ||
			(forwardReply.Err != OK && forwardReply.Err != ErrNoKey) {
			reply.Err = ErrWrongServer
			return nil
		}
	}

	value, ok := pb.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Err = OK
	reply.Value = value
	return nil
}

func (pb *PBServer) ForwardGet(args *ForwardGetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.ready ||
		pb.view.Viewnum != args.Viewnum ||
		pb.view.Primary != args.Primary ||
		pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}

	value, ok := pb.data[args.Args.Key]
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Err = OK
	reply.Value = value
	return nil
}

func (pb *PBServer) Transfer(args *TransferArgs, reply *TransferReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if args.View.Backup != pb.me || args.View.Viewnum < pb.view.Viewnum {
		reply.Err = ErrWrongServer
		return nil
	}

	data := make(map[string]string, len(args.Data))
	for key, value := range args.Data {
		data[key] = value
	}
	results := make(map[int64]PutReply, len(args.Results))
	for request, result := range args.Results {
		results[request] = result
	}

	pb.data = data
	pb.results = results
	pb.view = args.View
	pb.ready = true
	reply.Err = OK
	return nil
}

func (pb *PBServer) tick() {
	pb.mu.Lock()
	viewnum := pb.view.Viewnum
	pb.mu.Unlock()

	newView, err := pb.vs.Ping(viewnum)
	if err != nil {
		return
	}

	pb.mu.Lock()
	defer pb.mu.Unlock()

	if newView.Viewnum <= pb.view.Viewnum {
		return
	}

	oldView := pb.view
	oldReady := pb.ready

	if newView.Primary == pb.me {
		if !oldReady {
			if oldView.Viewnum == 0 && newView.Viewnum == 1 {
				oldReady = true
			} else {
				pb.view = newView
				pb.ready = false
				return
			}
		}

		if newView.Backup != "" {
			data := make(map[string]string, len(pb.data))
			for key, value := range pb.data {
				data[key] = value
			}
			results := make(map[int64]PutReply, len(pb.results))
			for request, result := range pb.results {
				results[request] = result
			}
			args := &TransferArgs{View: newView, Data: data, Results: results}
			var reply TransferReply
			if !call(newView.Backup, "PBServer.Transfer", args, &reply) || reply.Err != OK {
				return
			}
		}

		pb.view = newView
		pb.ready = true
		return
	}

	pb.view = newView
	pb.ready = false
}

func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	pb.data = make(map[string]string)
	pb.results = make(map[int64]PutReply)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
