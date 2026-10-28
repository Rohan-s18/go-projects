package paxos

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

type instance struct {
	np        int
	na        int
	va        interface{}
	decided   bool
	value     interface{}
	proposing bool
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int
	instances  map[int]*instance
	done       []int
	maxSeq     int
}

type PrepareArgs struct {
	Seq  int
	N    int
	From int
	Done int
}

type PrepareReply struct {
	OK      bool
	Np      int
	Na      int
	Va      interface{}
	Decided bool
	Value   interface{}
	From    int
	Done    int
}

type AcceptArgs struct {
	Seq   int
	N     int
	Value interface{}
	From  int
	Done  int
}

type AcceptReply struct {
	OK      bool
	Np      int
	Decided bool
	Value   interface{}
	From    int
	Done    int
}

type DecideArgs struct {
	Seq   int
	Value interface{}
	From  int
	Done  int
}

type DecideReply struct {
	From int
	Done int
}

func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) minLocked() int {
	minimum := px.done[0]
	for _, value := range px.done[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum + 1
}

func (px *Paxos) forgetLocked() {
	minimum := px.minLocked()
	for seq := range px.instances {
		if seq < minimum {
			delete(px.instances, seq)
		}
	}
}

func (px *Paxos) updateDoneLocked(peer int, value int) {
	if peer >= 0 && peer < len(px.done) && value > px.done[peer] {
		px.done[peer] = value
		px.forgetLocked()
	}
}

func (px *Paxos) getInstanceLocked(seq int) *instance {
	state, ok := px.instances[seq]
	if !ok {
		state = &instance{np: -1, na: -1}
		px.instances[seq] = state
	}
	if seq > px.maxSeq {
		px.maxSeq = seq
	}
	return state
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.updateDoneLocked(args.From, args.Done)
	reply.From = px.me
	reply.Done = px.done[px.me]
	if args.Seq < px.minLocked() {
		return nil
	}

	state := px.getInstanceLocked(args.Seq)
	if state.decided {
		reply.Decided = true
		reply.Value = state.value
		return nil
	}
	if args.N > state.np {
		state.np = args.N
		reply.OK = true
		reply.Na = state.na
		reply.Va = state.va
	}
	reply.Np = state.np
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.updateDoneLocked(args.From, args.Done)
	reply.From = px.me
	reply.Done = px.done[px.me]
	if args.Seq < px.minLocked() {
		return nil
	}

	state := px.getInstanceLocked(args.Seq)
	if state.decided {
		reply.Decided = true
		reply.Value = state.value
		return nil
	}
	if args.N >= state.np {
		state.np = args.N
		state.na = args.N
		state.va = args.Value
		reply.OK = true
	}
	reply.Np = state.np
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	px.updateDoneLocked(args.From, args.Done)
	reply.From = px.me
	reply.Done = px.done[px.me]
	if args.Seq < px.minLocked() {
		return nil
	}

	state := px.getInstanceLocked(args.Seq)
	if !state.decided {
		state.decided = true
		state.value = args.Value
		state.va = args.Value
	}
	return nil
}

func (px *Paxos) localDone() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.done[px.me]
}

func (px *Paxos) recordReply(peer int, done int) {
	px.mu.Lock()
	px.updateDoneLocked(peer, done)
	px.mu.Unlock()
}

func (px *Paxos) sendPrepare(peer int, args *PrepareArgs, reply *PrepareReply) bool {
	if peer == px.me {
		px.Prepare(args, reply)
		return true
	}
	return call(px.peers[peer], "Paxos.Prepare", args, reply)
}

func (px *Paxos) sendAccept(peer int, args *AcceptArgs, reply *AcceptReply) bool {
	if peer == px.me {
		px.Accept(args, reply)
		return true
	}
	return call(px.peers[peer], "Paxos.Accept", args, reply)
}

func (px *Paxos) sendDecide(peer int, args *DecideArgs, reply *DecideReply) bool {
	if peer == px.me {
		px.Decide(args, reply)
		return true
	}
	return call(px.peers[peer], "Paxos.Decide", args, reply)
}

func (px *Paxos) nextProposal(after int) int {
	n := ((after / len(px.peers)) + 1) * len(px.peers)
	n += px.me
	if n <= after {
		n += len(px.peers)
	}
	return n
}

func (px *Paxos) stopped(seq int) (bool, bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if px.dead || seq < px.minLocked() {
		return true, false, nil
	}
	state, ok := px.instances[seq]
	if ok && state.decided {
		return true, true, state.value
	}
	return false, false, nil
}

func (px *Paxos) broadcastDecision(seq int, value interface{}) {
	pending := make(map[int]bool)
	for peer := range px.peers {
		pending[peer] = true
	}
	delay := 10 * time.Millisecond

	for len(pending) > 0 {
		px.mu.Lock()
		stop := px.dead || seq < px.minLocked()
		done := px.done[px.me]
		px.mu.Unlock()
		if stop {
			return
		}

		for peer := range pending {
			args := &DecideArgs{Seq: seq, Value: value, From: px.me, Done: done}
			var reply DecideReply
			if px.sendDecide(peer, args, &reply) {
				px.recordReply(reply.From, reply.Done)
				delete(pending, peer)
			}
		}
		if len(pending) > 0 {
			time.Sleep(delay)
			if delay < 500*time.Millisecond {
				delay *= 2
			}
		}
	}
}

func (px *Paxos) proposer(seq int, proposed interface{}) {
	n := px.me
	value := proposed

	for {
		if stop, decided, decidedValue := px.stopped(seq); stop {
			if decided {
				px.broadcastDecision(seq, decidedValue)
			}
			return
		}

		done := px.localDone()
		prepareOK := 0
		highestAccepted := -1
		maxSeen := n
		chosen := value
		var learned interface{}
		learnedDecision := false

		for peer := range px.peers {
			args := &PrepareArgs{Seq: seq, N: n, From: px.me, Done: done}
			var reply PrepareReply
			if !px.sendPrepare(peer, args, &reply) {
				continue
			}
			px.recordReply(reply.From, reply.Done)
			if reply.Decided {
				learned = reply.Value
				learnedDecision = true
				break
			}
			if reply.OK {
				prepareOK++
				if reply.Na > highestAccepted {
					highestAccepted = reply.Na
					chosen = reply.Va
				}
			} else if reply.Np > maxSeen {
				maxSeen = reply.Np
			}
		}

		if learnedDecision {
			px.broadcastDecision(seq, learned)
			return
		}
		if prepareOK < len(px.peers)/2+1 {
			n = px.nextProposal(maxSeen)
			time.Sleep(time.Duration(10+rand.Intn(20)) * time.Millisecond)
			continue
		}

		acceptOK := 0
		maxSeen = n
		for peer := range px.peers {
			args := &AcceptArgs{Seq: seq, N: n, Value: chosen, From: px.me, Done: done}
			var reply AcceptReply
			if !px.sendAccept(peer, args, &reply) {
				continue
			}
			px.recordReply(reply.From, reply.Done)
			if reply.Decided {
				learned = reply.Value
				learnedDecision = true
				break
			}
			if reply.OK {
				acceptOK++
			} else if reply.Np > maxSeen {
				maxSeen = reply.Np
			}
		}

		if learnedDecision {
			px.broadcastDecision(seq, learned)
			return
		}
		if acceptOK >= len(px.peers)/2+1 {
			px.broadcastDecision(seq, chosen)
			return
		}

		n = px.nextProposal(maxSeen)
		value = chosen
		time.Sleep(time.Duration(10+rand.Intn(20)) * time.Millisecond)
	}
}

func (px *Paxos) Start(seq int, v interface{}) {
	px.mu.Lock()
	if seq < px.minLocked() {
		px.mu.Unlock()
		return
	}
	state := px.getInstanceLocked(seq)
	if state.decided || state.proposing {
		px.mu.Unlock()
		return
	}
	state.proposing = true
	px.mu.Unlock()
	go px.proposer(seq, v)
}

func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	if seq > px.done[px.me] {
		px.done[px.me] = seq
		px.forgetLocked()
	}
	px.mu.Unlock()
}

func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeq
}

func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.forgetLocked()
	return px.minLocked()
}

func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq < px.minLocked() {
		return false, nil
	}
	state, ok := px.instances[seq]
	if !ok || !state.decided {
		return false, nil
	}
	return true, state.value
}

func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.instances = make(map[int]*instance)
	px.done = make([]int, len(peers))
	for i := range px.done {
		px.done[i] = -1
	}
	px.maxSeq = -1

	if rpcs != nil {
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		os.Remove(peers[me])
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
