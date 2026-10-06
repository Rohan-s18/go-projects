package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	currentView  View
	lastPing     map[string]time.Time
	informedView map[string]uint
	restarted    map[string]bool
	acknowledged bool
	serverOrder  []string
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if _, seen := vs.lastPing[args.Me]; !seen {
		vs.serverOrder = append(vs.serverOrder, args.Me)
	}
	vs.lastPing[args.Me] = time.Now()

	if vs.currentView.Viewnum == 0 {
		vs.installView(args.Me, "")
	} else {
		if args.Viewnum == 0 &&
			vs.informedView[args.Me] == vs.currentView.Viewnum &&
			(args.Me == vs.currentView.Primary || args.Me == vs.currentView.Backup) {
			vs.restarted[args.Me] = true
		}

		if args.Me == vs.currentView.Primary &&
			args.Viewnum == vs.currentView.Viewnum &&
			!vs.restarted[args.Me] {
			vs.acknowledged = true
		}
	}

	reply.View = vs.currentView
	vs.informedView[args.Me] = vs.currentView.Viewnum

	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currentView

	return nil
}

func (vs *ViewServer) installView(primary string, backup string) {
	vs.currentView = View{
		Viewnum: vs.currentView.Viewnum + 1,
		Primary: primary,
		Backup:  backup,
	}
	vs.acknowledged = false
	vs.restarted = make(map[string]bool)
}

func (vs *ViewServer) isAlive(server string, now time.Time) bool {
	if server == "" {
		return false
	}

	last, ok := vs.lastPing[server]
	return ok && now.Sub(last) < time.Duration(DeadPings)*PingInterval
}

func (vs *ViewServer) pickBackup(primary string, now time.Time) string {
	for _, server := range vs.serverOrder {
		if server != primary && vs.isAlive(server, now) {
			return server
		}
	}
	return ""
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.currentView.Viewnum == 0 || !vs.acknowledged {
		return
	}

	now := time.Now()
	primary := vs.currentView.Primary
	backup := vs.currentView.Backup

	primaryUnavailable := !vs.isAlive(primary, now) || vs.restarted[primary]
	backupUnavailable := backup != "" &&
		(!vs.isAlive(backup, now) || vs.restarted[backup])

	if primaryUnavailable {
		if backup != "" && !backupUnavailable {
			vs.installView(backup, vs.pickBackup(backup, now))
		}
		return
	}

	if backupUnavailable {
		vs.installView(primary, vs.pickBackup(primary, now))
		return
	}

	if backup == "" {
		if candidate := vs.pickBackup(primary, now); candidate != "" {
			vs.installView(primary, candidate)
		}
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	vs.currentView = View{}
	vs.lastPing = make(map[string]time.Time)
	vs.informedView = make(map[string]uint)
	vs.restarted = make(map[string]bool)
	vs.serverOrder = make([]string, 0)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
