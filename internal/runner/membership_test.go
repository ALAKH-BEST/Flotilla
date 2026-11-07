package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// admin calls the membership API on any node, following the redirect to the leader, and retries
// while the cluster settles (a new leader answers 503 until it has committed in its term).
func admin(t *testing.T, addrs []string, method, path, body string, want int) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		for _, a := range addrs {
			u := url.URL{Scheme: "http", Host: a, Path: path}
			req, _ := http.NewRequest(method, u.String(), strings.NewReader(body))
			resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
			if err != nil {
				last = err.Error()
				continue
			}
			out, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == want {
				return
			}
			last = fmt.Sprintf("%s: %d %s", a, resp.StatusCode, strings.TrimSpace(string(out)))
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s %s never returned %d; last: %s", method, path, want, last)
}

func memberIDs(t *testing.T, addr string) []int {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/admin/members")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var ms []struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&ms); err != nil {
		t.Fatal(err)
	}
	ids := make([]int, len(ms))
	for i, m := range ms {
		ids[i] = m.ID
	}
	return ids
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestClusterGrowsAndShrinksOverTCP(t *testing.T) {
	// Three nodes to start with.
	const n = 3
	nodes := make([]*testNode, n)
	rls, hls := make([]listener, n), make([]listener, n)
	cluster := map[int]Peer{}
	for i := 0; i < n; i++ {
		rls[i], hls[i] = listen(t), listen(t)
		nodes[i] = &testNode{dir: t.TempDir(), raftAddr: rls[i].Addr().String(), httpAddr: hls[i].Addr().String()}
		cluster[i] = Peer{Raft: nodes[i].raftAddr, HTTP: nodes[i].httpAddr}
	}
	for i := 0; i < n; i++ {
		start(t, i, cluster, nodes[i], rls[i], hls[i])
	}
	stopAll := func() {
		for _, nd := range nodes {
			if nd != nil && nd.r != nil {
				nd.r.Stop()
			}
		}
	}
	defer stopAll()
	all := []string{nodes[0].httpAddr, nodes[1].httpAddr, nodes[2].httpAddr}
	for i := 0; i < 30; i++ {
		retry(t, all, http.MethodPut, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), http.StatusNoContent)
	}

	// A fourth node starts knowing only its own addresses, and is then added. Its address is
	// recorded in the log, so the other nodes learn how to reach it from there.
	rl3, hl3 := listen(t), listen(t)
	n3 := &testNode{dir: t.TempDir(), raftAddr: rl3.Addr().String(), httpAddr: hl3.Addr().String()}
	own := map[int]Peer{3: {Raft: n3.raftAddr, HTTP: n3.httpAddr}}
	r3, err := Start(Config{ID: 3, Cluster: own, DataDir: n3.dir, SnapshotEvery: 20, Join: true, RaftListener: rl3, HTTPListener: hl3})
	if err != nil {
		t.Fatal(err)
	}
	n3.r = r3
	nodes = append(nodes, n3)
	if ids := r3.Server().Node().Status().Members; len(ids) != 0 {
		t.Fatalf("a node that has not been added has members %v", ids)
	}
	admin(t, all, http.MethodPut, "/admin/members/3", n3.raftAddr+"/"+n3.httpAddr, http.StatusNoContent)
	waitFor(t, "the new node to catch up", func() bool {
		d := r3.Server().Store().Data()
		return d["k0"] == "v0" && d["k29"] == "v29"
	})
	for _, a := range []string{nodes[0].httpAddr, n3.httpAddr} {
		if got := memberIDs(t, a); len(got) != 4 {
			t.Fatalf("node %s sees members %v, want 4", a, got)
		}
	}
	retry(t, all, http.MethodPut, "after-add", "ok", http.StatusNoContent)

	// Remove an original node. Writes keep working through the others, and the removed node can
	// go away without the cluster missing it.
	admin(t, all, http.MethodDelete, "/admin/members/0", "", http.StatusNoContent)
	waitFor(t, "the removed node to shut itself down", func() bool {
		resp, err := http.Get("http://" + nodes[0].httpAddr + "/admin/members")
		if err == nil {
			resp.Body.Close()
		}
		return err != nil
	})
	nodes[0].r.Stop() // already stopped; stopping again is harmless
	nodes[0].r = nil
	rest := []string{nodes[1].httpAddr, nodes[2].httpAddr, n3.httpAddr}
	retry(t, rest, http.MethodPut, "after-remove", "ok", http.StatusNoContent)
	waitFor(t, "the survivors to agree on three members", func() bool {
		for _, a := range rest {
			if got := memberIDs(t, a); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
				return false
			}
		}
		return true
	})

	// The joined node restarts from its own disk, with no address list at all, and still takes part.
	r3.Stop()
	rl, hl := mustRelisten(t, n3.raftAddr), mustRelisten(t, n3.httpAddr)
	r3b, err := Start(Config{ID: 3, Cluster: own, DataDir: n3.dir, SnapshotEvery: 20, Join: true, RaftListener: rl, HTTPListener: hl})
	if err != nil {
		t.Fatal(err)
	}
	n3.r = r3b
	if got := r3b.Server().Node().Status().Members; len(got) != 3 {
		t.Fatalf("after restarting, the node has members %v, want 3", got)
	}
	waitFor(t, "the restarted node to recover its data", func() bool {
		d := r3b.Server().Store().Data()
		return d["k0"] == "v0" && d["after-add"] == "ok" && d["after-remove"] == "ok"
	})
	if got := retry(t, rest, http.MethodGet, "after-remove", "", http.StatusOK); got != "ok" {
		t.Fatalf("after-remove = %q", got)
	}
}
