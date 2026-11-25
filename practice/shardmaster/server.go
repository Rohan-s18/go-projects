package shardmaster

import "coms4113/hw4/src/paxos"
import "encoding/gob"
import "fmt"
import "log"
import "math/rand"
import "net"
import "net/rpc"
import "os"
import "sort"
import "sync"
import "syscall"
import "time"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	nextSeq int
	nextID  int64
}

type Op struct {
	Type    string
	Source  int
	ID      int64
	GID     int64
	Servers []string
	Shard   int
	Num     int
}

type groupLoad struct {
	gid   int64
	count int
}

func cloneConfig(previous Config) Config {
	next := Config{Num: previous.Num + 1, Shards: previous.Shards}
	next.Groups = make(map[int64][]string)
	for gid, servers := range previous.Groups {
		next.Groups[gid] = append([]string(nil), servers...)
	}
	return next
}

func rebalance(config *Config) {
	if len(config.Groups) == 0 {
		for shard := range config.Shards {
			config.Shards[shard] = 0
		}
		return
	}

	counts := make(map[int64]int)
	loads := make([]groupLoad, 0, len(config.Groups))
	for gid := range config.Groups {
		counts[gid] = 0
	}
	for _, gid := range config.Shards {
		if _, exists := config.Groups[gid]; exists {
			counts[gid]++
		}
	}
	for gid := range config.Groups {
		loads = append(loads, groupLoad{gid: gid, count: counts[gid]})
	}

	sort.Slice(loads, func(i int, j int) bool {
		if loads[i].count != loads[j].count {
			return loads[i].count > loads[j].count
		}
		return loads[i].gid < loads[j].gid
	})

	base := NShards / len(loads)
	extra := NShards % len(loads)
	targets := make(map[int64]int)
	for i, load := range loads {
		targets[load.gid] = base
		if i < extra {
			targets[load.gid]++
		}
	}

	pool := make([]int, 0)
	for shard, gid := range config.Shards {
		target, exists := targets[gid]
		if !exists {
			pool = append(pool, shard)
		} else if counts[gid] > target {
			pool = append(pool, shard)
			counts[gid]--
		}
	}

	gids := make([]int64, 0, len(config.Groups))
	for gid := range config.Groups {
		gids = append(gids, gid)
	}
	sort.Slice(gids, func(i int, j int) bool { return gids[i] < gids[j] })

	poolIndex := 0
	for _, gid := range gids {
		for counts[gid] < targets[gid] {
			config.Shards[pool[poolIndex]] = gid
			poolIndex++
			counts[gid]++
		}
	}
}

func (sm *ShardMaster) wait(seq int, op Op) Op {
	delay := 10 * time.Millisecond
	for {
		decided, value := sm.px.Status(seq)
		if decided {
			return value.(Op)
		}
		sm.px.Start(seq, op)
		time.Sleep(delay)
		if delay < time.Second {
			delay *= 2
		}
	}
}

func (sm *ShardMaster) apply(op Op) {
	if op.Type == "Query" {
		return
	}

	config := cloneConfig(sm.configs[len(sm.configs)-1])
	switch op.Type {
	case "Join":
		config.Groups[op.GID] = append([]string(nil), op.Servers...)
		rebalance(&config)
	case "Leave":
		delete(config.Groups, op.GID)
		rebalance(&config)
	case "Move":
		if op.Shard >= 0 && op.Shard < NShards {
			config.Shards[op.Shard] = op.GID
		}
	}
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) run(op Op) {
	for {
		seq := sm.nextSeq
		chosen := sm.wait(seq, op)
		sm.apply(chosen)
		sm.px.Done(seq)
		sm.nextSeq++
		if chosen.Source == op.Source && chosen.ID == op.ID {
			return
		}
	}
}

func (sm *ShardMaster) operation(kind string) Op {
	op := Op{Type: kind, Source: sm.me, ID: sm.nextID}
	sm.nextID++
	return op
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := sm.operation("Join")
	op.GID = args.GID
	op.Servers = append([]string(nil), args.Servers...)
	sm.run(op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := sm.operation("Leave")
	op.GID = args.GID
	sm.run(op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := sm.operation("Move")
	op.Shard = args.Shard
	op.GID = args.GID
	sm.run(op)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := sm.operation("Query")
	op.Num = args.Num
	sm.run(op)

	index := args.Num
	if index < 0 || index >= len(sm.configs) {
		index = len(sm.configs) - 1
	}
	reply.Config = sm.configs[index]
	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
