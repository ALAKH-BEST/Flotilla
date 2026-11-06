// Package tcp carries Raft RPCs between processes over TCP, using Go's net/rpc.
package tcp

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

// ErrTimeout is returned when a peer does not answer in time.
var ErrTimeout = errors.New("tcp: request timed out")

// Transport sends Raft RPCs to peers. Connections are made on first use and made again
// after any failure, so a restarted peer is picked up automatically.
type Transport struct {
	addrs map[int]string

	// DialTimeout, CallTimeout and SnapshotTimeout bound each connection attempt and call.
	DialTimeout     time.Duration
	CallTimeout     time.Duration
	SnapshotTimeout time.Duration

	mu      sync.Mutex
	clients map[int]*rpc.Client
	closed  bool
}

var _ raft.Transport = (*Transport)(nil)

// New creates a transport. addrs maps every node ID to its Raft address (host:port).
func New(addrs map[int]string) *Transport {
	known := make(map[int]string, len(addrs))
	for id, a := range addrs {
		known[id] = a
	}
	return &Transport{
		addrs:           known,
		DialTimeout:     300 * time.Millisecond,
		CallTimeout:     time.Second,
		SnapshotTimeout: 10 * time.Second,
		clients:         make(map[int]*rpc.Client),
	}
}

// SetAddr records where a node can be reached, for nodes that join after the transport was
// created. If the node was reachable somewhere else before, the old connection is dropped.
func (t *Transport) SetAddr(id int, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.addrs[id] == addr {
		return
	}
	t.addrs[id] = addr
	if c := t.clients[id]; c != nil {
		c.Close()
		delete(t.clients, id)
	}
}

// Close drops every connection.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
}

func (t *Transport) client(to int) (*rpc.Client, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errors.New("tcp: transport closed")
	}
	if c := t.clients[to]; c != nil {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.addrs[to]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("tcp: no address for node %d", to)
	}

	conn, err := net.DialTimeout("tcp", addr, t.DialTimeout) // not under the lock: it can take a while
	if err != nil {
		return nil, err
	}
	c := rpc.NewClient(conn)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		c.Close()
		return nil, errors.New("tcp: transport closed")
	}
	if existing := t.clients[to]; existing != nil { // another goroutine connected first
		c.Close()
		return existing, nil
	}
	t.clients[to] = c
	return c, nil
}

func (t *Transport) drop(to int, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *Transport) call(to int, method string, args, reply any, timeout time.Duration) error {
	c, err := t.client(to)
	if err != nil {
		return err
	}
	call := c.Go(method, args, reply, make(chan *rpc.Call, 1))
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-call.Done:
		if call.Error != nil {
			t.drop(to, c)
			return call.Error
		}
		return nil
	case <-timer.C:
		t.drop(to, c) // the connection may be wedged; reconnect next time
		return ErrTimeout
	}
}

func (t *Transport) RequestVote(to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := new(raft.RequestVoteReply)
	if err := t.call(to, "Raft.RequestVote", args, reply, t.CallTimeout); err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *Transport) PreVote(to int, args *raft.PreVoteArgs) (*raft.PreVoteReply, error) {
	reply := new(raft.PreVoteReply)
	if err := t.call(to, "Raft.PreVote", args, reply, t.CallTimeout); err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *Transport) AppendEntries(to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := new(raft.AppendEntriesReply)
	if err := t.call(to, "Raft.AppendEntries", args, reply, t.CallTimeout); err != nil {
		return nil, err
	}
	return reply, nil
}

func (t *Transport) InstallSnapshot(to int, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	reply := new(raft.InstallSnapshotReply)
	if err := t.call(to, "Raft.InstallSnapshot", args, reply, t.SnapshotTimeout); err != nil {
		return nil, err
	}
	return reply, nil
}

// service is the receiving side that net/rpc exposes.
type service struct{ h raft.Handler }

func (s *service) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = *s.h.HandleRequestVote(args)
	return nil
}

func (s *service) PreVote(args *raft.PreVoteArgs, reply *raft.PreVoteReply) error {
	*reply = *s.h.HandlePreVote(args)
	return nil
}

func (s *service) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = *s.h.HandleAppendEntries(args)
	return nil
}

func (s *service) InstallSnapshot(args *raft.InstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	*reply = *s.h.HandleInstallSnapshot(args)
	return nil
}

// Serve answers Raft RPCs on l by passing them to h. The returned function stops
// listening and closes every open connection.
func Serve(l net.Listener, h raft.Handler) (stop func()) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &service{h: h}); err != nil {
		panic(err) // the service's methods are fixed, so this cannot fail
	}
	var (
		mu     sync.Mutex
		conns  = make(map[net.Conn]struct{})
		closed bool
	)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				conn.Close()
				return
			}
			conns[conn] = struct{}{}
			mu.Unlock()
			go func() {
				srv.ServeConn(conn)
				mu.Lock()
				delete(conns, conn)
				mu.Unlock()
			}()
		}
	}()
	return func() {
		l.Close()
		mu.Lock()
		closed = true
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
	}
}
