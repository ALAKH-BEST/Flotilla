package runner

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

type testNode struct {
	r        *Runner
	raftAddr string
	httpAddr string
	dir      string
}

func listen(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func start(t *testing.T, id int, cluster map[int]Peer, n *testNode, rl, hl net.Listener) {
	t.Helper()
	r, err := Start(Config{ID: id, Cluster: cluster, DataDir: n.dir, SnapshotEvery: 20, RaftListener: rl, HTTPListener: hl})
	if err != nil {
		t.Fatal(err)
	}
	n.r = r
}

func do(method, addr, key, body string) (int, string, error) {
	req, _ := http.NewRequest(method, "http://"+addr+"/kv/"+key, strings.NewReader(body))
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Do(req) // follows the redirect to the leader
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), nil
}

// retry runs an operation against any live node until it succeeds, since elections take a moment.
func retry(t *testing.T, addrs []string, method, key, body string, wantStatus int) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		for _, a := range addrs {
			code, out, err := do(method, a, key, body)
			if err == nil && code == wantStatus {
				return out
			}
			last = fmt.Sprintf("%s: code %d %q err %v", a, code, out, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s %s never returned %d; last: %s", method, key, wantStatus, last)
	return ""
}

func TestClusterOverTCPSurvivesLeaderLossAndRestart(t *testing.T) {
	const n = 3
	nodes := make([]*testNode, n)
	rls, hls := make([]net.Listener, n), make([]net.Listener, n)
	cluster := map[int]Peer{}
	for i := 0; i < n; i++ {
		rls[i], hls[i] = listen(t), listen(t)
		nodes[i] = &testNode{dir: t.TempDir(), raftAddr: rls[i].Addr().String(), httpAddr: hls[i].Addr().String()}
		cluster[i] = Peer{Raft: nodes[i].raftAddr, HTTP: nodes[i].httpAddr}
	}
	for i := 0; i < n; i++ {
		start(t, i, cluster, nodes[i], rls[i], hls[i])
	}
	defer func() {
		for _, nd := range nodes {
			if nd.r != nil {
				nd.r.Stop()
			}
		}
	}()
	all := []string{nodes[0].httpAddr, nodes[1].httpAddr, nodes[2].httpAddr}

	// Write through every node (followers redirect to the leader), enough to force snapshots.
	for i := 0; i < 60; i++ {
		retry(t, []string{all[i%n]}, http.MethodPut, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), http.StatusNoContent)
	}
	if got := retry(t, all, http.MethodGet, "k42", "", http.StatusOK); got != "v42" {
		t.Fatalf("k42 = %q", got)
	}

	// Stop whichever node is leader.
	leader := -1
	for i, nd := range nodes {
		if nd.r.Server().Node().Status().Role.String() == "leader" {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatal("no leader found")
	}
	nodes[leader].r.Stop()
	nodes[leader].r = nil
	var live []string
	for i := range nodes {
		if i != leader {
			live = append(live, nodes[i].httpAddr)
		}
	}

	// The survivors elect a new leader, keep every earlier write and accept new ones.
	retry(t, live, http.MethodPut, "after", "failover", http.StatusNoContent)
	if got := retry(t, live, http.MethodGet, "k7", "", http.StatusOK); got != "v7" {
		t.Fatalf("k7 after failover = %q", got)
	}

	// Restart the stopped node on the same addresses; it must catch up from its own disk plus the leader.
	rl, err := net.Listen("tcp", nodes[leader].raftAddr)
	if err != nil {
		t.Fatal(err)
	}
	hl, err := net.Listen("tcp", nodes[leader].httpAddr)
	if err != nil {
		t.Fatal(err)
	}
	start(t, leader, cluster, nodes[leader], rl, hl)

	deadline := time.Now().Add(10 * time.Second)
	for {
		data := nodes[leader].r.Server().Store().Data()
		if data["after"] == "failover" && data["k0"] == "v0" && data["k59"] == "v59" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("restarted node did not catch up; has %d keys", len(data))
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Delete works and a missing key is a 404.
	retry(t, all, http.MethodDelete, "k1", "", http.StatusNoContent)
	retry(t, all, http.MethodGet, "k1", "", http.StatusNotFound)
}
