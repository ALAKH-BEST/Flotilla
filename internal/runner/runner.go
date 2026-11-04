// Package runner starts one Flotilla node as a process would: Raft over TCP, durable
// storage on disk, and the HTTP client API.
package runner

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/server"
	"github.com/ALAKH-BEST/flotilla/transport/tcp"
)

// Peer is where a cluster member listens.
type Peer struct {
	Raft string // host:port for Raft RPCs between nodes
	HTTP string // host:port for the client API
}

// ParseCluster reads "0=host:7000/host:8000,1=..." (id=raftAddr/httpAddr, comma separated).
func ParseCluster(spec string) (map[int]Peer, error) {
	cluster := make(map[int]Peer)
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idText, addrs, ok := strings.Cut(part, "=")
		raftAddr, httpAddr, ok2 := strings.Cut(addrs, "/")
		id, err := strconv.Atoi(idText)
		if !ok || !ok2 || err != nil || id < 0 || raftAddr == "" || httpAddr == "" {
			return nil, fmt.Errorf("bad cluster entry %q: want id=raftHost:port/httpHost:port", part)
		}
		if _, dup := cluster[id]; dup {
			return nil, fmt.Errorf("node %d is listed twice", id)
		}
		cluster[id] = Peer{Raft: raftAddr, HTTP: httpAddr}
	}
	if len(cluster) == 0 {
		return nil, fmt.Errorf("the cluster has no nodes")
	}
	return cluster, nil
}

// Config configures a node.
type Config struct {
	ID      int
	Cluster map[int]Peer
	// DataDir holds the node's durable state. Empty keeps state in memory only, which is
	// lost when the process exits.
	DataDir       string
	SnapshotEvery int

	// Listeners are optional. When nil the node listens on its own addresses from Cluster.
	RaftListener net.Listener
	HTTPListener net.Listener
}

// Runner is a running node.
type Runner struct {
	srv       *server.Server
	transport *tcp.Transport
	stopRaft  func()
	httpSrv   *http.Server
	raftAddr  string
	httpAddr  string
}

// Start runs a node until Stop is called.
func Start(cfg Config) (*Runner, error) {
	self, ok := cfg.Cluster[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("node %d is not in the cluster", cfg.ID)
	}
	ids := make([]int, 0, len(cfg.Cluster))
	raftAddrs := make(map[int]string)
	for id, p := range cfg.Cluster {
		ids = append(ids, id)
		raftAddrs[id] = p.Raft
	}
	sort.Ints(ids)

	var storage raft.Storage = raft.NewMemoryStorage()
	if cfg.DataDir != "" {
		fs, err := raft.NewFileStorage(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		storage = fs
	}

	var err error
	raftListener, httpListener := cfg.RaftListener, cfg.HTTPListener
	if raftListener == nil {
		if raftListener, err = net.Listen("tcp", self.Raft); err != nil {
			return nil, fmt.Errorf("listen for raft on %s: %w", self.Raft, err)
		}
	}
	if httpListener == nil {
		if httpListener, err = net.Listen("tcp", self.HTTP); err != nil {
			raftListener.Close()
			return nil, fmt.Errorf("listen for http on %s: %w", self.HTTP, err)
		}
	}

	transport := tcp.New(raftAddrs)
	srv, err := server.New(server.Config{
		Raft: raft.Config{
			ID:        cfg.ID,
			Peers:     ids,
			Transport: transport,
			Storage:   storage,
		},
		SnapshotEvery: cfg.SnapshotEvery,
	})
	if err != nil {
		raftListener.Close()
		httpListener.Close()
		return nil, err
	}

	r := &Runner{
		srv:       srv,
		transport: transport,
		raftAddr:  raftListener.Addr().String(),
		httpAddr:  httpListener.Addr().String(),
	}
	r.stopRaft = tcp.Serve(raftListener, srv)
	r.httpSrv = &http.Server{
		Handler: srv.HTTPHandler(server.HTTPOptions{
			LeaderAddr: func(id int) string { return cfg.Cluster[id].HTTP },
		}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() { _ = r.httpSrv.Serve(httpListener) }()
	srv.Start()
	return r, nil
}

// Server gives access to the node's service, for tests.
func (r *Runner) Server() *server.Server { return r.srv }

// RaftAddr and HTTPAddr are the addresses the node is listening on.
func (r *Runner) RaftAddr() string { return r.raftAddr }
func (r *Runner) HTTPAddr() string { return r.httpAddr }

// Stop shuts the node down.
func (r *Runner) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.httpSrv.Shutdown(ctx)
	r.stopRaft()
	r.srv.Stop()
	r.transport.Close()
}
