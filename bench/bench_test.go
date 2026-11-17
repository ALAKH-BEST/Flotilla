// Package bench measures a real three-node cluster: separate processes, real TCP on the loopback
// interface and real disk. It reports write and read throughput and latency, and how long clients
// wait when the leader is killed or hands leadership away.
//
//	FLOTILLA_BENCH=1 go test ./bench -run TestBench -v -timeout 10m
//
// The numbers depend on the machine, and on the disk above all, because every write is saved to
// disk before it is acknowledged. They show how the system behaves, not what it can do anywhere else.
package bench

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

const nodeCount = 3

type node struct {
	id       int
	dir      string
	raftAddr string
	httpAddr string
	cmd      *exec.Cmd
}

type cluster struct {
	t     *testing.T
	bin   string
	nodes []*node
	spec  string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "flotilla")
	if out, err := exec.Command("go", "build", "-o", bin, "../cmd/flotilla").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	c := &cluster{t: t, bin: bin}
	var parts []string
	for i := 0; i < nodeCount; i++ {
		n := &node{id: i, dir: filepath.Join(root, fmt.Sprintf("data%d", i)), raftAddr: freeAddr(t), httpAddr: freeAddr(t)}
		c.nodes = append(c.nodes, n)
		parts = append(parts, fmt.Sprintf("%d=%s/%s", i, n.raftAddr, n.httpAddr))
	}
	c.spec = strings.Join(parts, ",")
	for _, n := range c.nodes {
		c.start(n)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			c.kill(n)
		}
	})
	return c
}

func (c *cluster) start(n *node) {
	args := []string{"server", "-id", strconv.Itoa(n.id), "-cluster", c.spec}
	if os.Getenv("FLOTILLA_BENCH_MEMORY") == "" {
		args = append(args, "-data", n.dir)
	}
	cmd := exec.Command(c.bin, args...)
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	go func() { _ = cmd.Wait() }()
	n.cmd = cmd
}

func (c *cluster) kill(n *node) {
	if n.cmd != nil {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		n.cmd = nil
	}
}

var statusClient = &http.Client{Timeout: 500 * time.Millisecond}

// leader returns the node that currently leads, if one answers.
func (c *cluster) leader() (*node, bool) {
	for _, n := range c.nodes {
		if n.cmd == nil {
			continue
		}
		resp, err := statusClient.Get("http://" + n.httpAddr + "/status")
		if err != nil {
			continue
		}
		var st struct{ Role string }
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err == nil && st.Role == "leader" {
			return n, true
		}
	}
	return nil, false
}

func (c *cluster) waitLeader() *node {
	c.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if l, ok := c.leader(); ok {
			// a write succeeding means the leader has committed in its term
			if code, _ := send(http.MethodPut, l.httpAddr, "warm", "x"); code == http.StatusNoContent {
				return l
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return nil
}

var httpClient = &http.Client{
	Timeout:   5 * time.Second,
	Transport: &http.Transport{MaxIdleConnsPerHost: 512, MaxConnsPerHost: 512},
}

func send(method, addr, key, body string) (int, error) {
	req, err := http.NewRequest(method, "http://"+addr+"/kv/"+key, strings.NewReader(body))
	if err != nil {
		return 0, err
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, nil
}

type result struct {
	ops     int
	errs    int
	elapsed time.Duration
	latency []time.Duration
}

func (r result) pct(p float64) time.Duration {
	if len(r.latency) == 0 {
		return 0
	}
	return r.latency[int(float64(len(r.latency)-1)*p)]
}

func (r result) String() string {
	return fmt.Sprintf("%8.0f ops/s   p50 %-9s p99 %-9s max %-9s errors %d",
		float64(r.ops)/r.elapsed.Seconds(), r.pct(0.50).Round(10*time.Microsecond), r.pct(0.99).Round(10*time.Microsecond),
		r.pct(1).Round(10*time.Microsecond), r.errs)
}

// load runs workers goroutines doing op in a loop for d.
func load(workers int, d time.Duration, op func(worker, i int) (int, error)) result {
	var mu sync.Mutex
	var all []time.Duration
	var errs int64
	var wg sync.WaitGroup
	start := time.Now()
	stop := start.Add(d)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			var lat []time.Duration
			for i := 0; time.Now().Before(stop); i++ {
				t0 := time.Now()
				code, err := op(w, i)
				if err != nil || code/100 != 2 {
					atomic.AddInt64(&errs, 1)
					continue
				}
				lat = append(lat, time.Since(t0))
			}
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return result{ops: len(all), errs: int(errs), elapsed: time.Since(start), latency: all}
}

func spread(ds []time.Duration) string {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	r := func(d time.Duration) time.Duration { return d.Round(time.Millisecond) }
	return fmt.Sprintf("min %v  median %v  max %v  (%d runs)", r(s[0]), r(s[len(s)/2]), r(s[len(s)-1]), len(s))
}

func TestBench(t *testing.T) {
	if os.Getenv("FLOTILLA_BENCH") == "" {
		t.Skip("set FLOTILLA_BENCH=1 to run (starts a real cluster and takes a few minutes)")
	}
	c := newCluster(t)
	leader := c.waitLeader()
	value := strings.Repeat("x", 100)
	dur := 5 * time.Second
	if s, err := strconv.Atoi(os.Getenv("FLOTILLA_BENCH_SECONDS")); err == nil && s > 0 {
		dur = time.Duration(s) * time.Second
	}

	out := &strings.Builder{}
	say := func(f string, a ...any) { fmt.Fprintf(out, f+"\n", a...); fmt.Printf(f+"\n", a...) }
	storage := "state saved to disk"
	if os.Getenv("FLOTILLA_BENCH_MEMORY") != "" {
		storage = "state in memory only (no disk)"
	}
	say("flotilla benchmark: %d nodes, %s/%s, %d CPUs, %s, 100-byte values, %v per run", nodeCount, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), storage, dur)
	say("")
	say("writes (each is replicated to a majority before the answer%s):", map[bool]string{true: "", false: " and saved to disk"}[os.Getenv("FLOTILLA_BENCH_MEMORY") != ""])
	var seq int64
	for _, w := range []int{1, 8, 32, 128} {
		r := load(w, dur, func(_, _ int) (int, error) {
			return send(http.MethodPut, leader.httpAddr, "w"+strconv.FormatInt(atomic.AddInt64(&seq, 1)%5000, 10), value)
		})
		say("  %3d clients  %v", w, r)
	}
	say("")
	// Make sure every key the readers ask for exists, so a miss does not count as an error.
	for i := 0; i < 500; i++ {
		if code, err := send(http.MethodPut, leader.httpAddr, "w"+strconv.Itoa(i), value); err != nil || code != http.StatusNoContent {
			t.Fatalf("seeding key %d: %v %v", i, code, err)
		}
	}
	say("reads (linearizable: the leader confirms with a majority, nothing is written):")
	for _, w := range []int{1, 8, 32, 128} {
		r := load(w, dur, func(_, i int) (int, error) {
			return send(http.MethodGet, leader.httpAddr, "w"+strconv.Itoa(i%500), "")
		})
		say("  %3d clients  %v", w, r)
	}

	say("")
	say("leader killed with SIGKILL: time until a write succeeds through the survivors:")
	var failover []time.Duration
	for i := 0; i < 8; i++ {
		l := c.waitLeader()
		var live []*node
		for _, n := range c.nodes {
			if n != l {
				live = append(live, n)
			}
		}
		t0 := time.Now()
		c.kill(l)
		for k := 0; ; k++ {
			if time.Since(t0) > 30*time.Second {
				t.Fatal("no write succeeded within 30s of killing the leader")
			}
			ctxAttempt := &http.Client{Timeout: 300 * time.Millisecond}
			req, _ := http.NewRequest(http.MethodPut, "http://"+live[k%len(live)].httpAddr+"/kv/failover", strings.NewReader("x"))
			resp, err := ctxAttempt.Do(req)
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode == http.StatusNoContent {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
		}
		failover = append(failover, time.Since(t0))
		c.start(l)
		time.Sleep(2 * time.Second) // let it rejoin before the next round
	}
	say("  %s", spread(failover))

	say("")
	say("leadership hand-over (flotilla transfer): time until the new leader accepts a write:")
	var transfer, call []time.Duration
	for i := 0; i < 8; i++ {
		l := c.waitLeader()
		target := c.nodes[(l.id+1)%nodeCount]
		t0 := time.Now()
		req, _ := http.NewRequest(http.MethodPut, "http://"+l.httpAddr+"/admin/leader/"+strconv.Itoa(target.id), nil)
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil || resp.StatusCode != http.StatusNoContent {
			t.Fatalf("transfer %d: %v %v", i, err, resp)
		}
		resp.Body.Close()
		call = append(call, time.Since(t0))
		for {
			if time.Since(t0) > 10*time.Second {
				t.Fatal("the target never accepted a write")
			}
			if code, err := send(http.MethodPut, target.httpAddr, "transfer", "x"); err == nil && code == http.StatusNoContent {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		transfer = append(transfer, time.Since(t0))
		time.Sleep(300 * time.Millisecond)
	}
	say("  call returns:        %s", spread(call))
	say("  first write served:  %s", spread(transfer))

	if path := os.Getenv("FLOTILLA_BENCH_OUT"); path != "" {
		_ = os.WriteFile(path, []byte(out.String()), 0o644)
	}
}
