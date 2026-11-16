// Package chaos runs real flotilla processes over real TCP and disk, breaks them, and checks that
// what clients saw is still linearizable.
//
// The in-memory tests elsewhere in the repository do the same thing over a simulated network. This
// one adds what a simulation cannot: processes killed with SIGKILL in the middle of a disk write,
// processes frozen with SIGSTOP, and real sockets cut between nodes.
//
//	FLOTILLA_CHAOS=1 go test ./chaos -run TestChaos -v
//
// FLOTILLA_CHAOS_SECONDS sets how long faults run (default 20) and FLOTILLA_CHAOS_SEED repeats a run.
package chaos

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/linearize"
)

const nodeCount = 5

type node struct {
	id       int
	dir      string
	raftAddr string
	httpAddr string
	spec     string // this node's -cluster flag: itself directly, its peers through proxies

	cmd    *exec.Cmd
	paused bool
}

type harness struct {
	t       *testing.T
	bin     string
	logDir  string
	nodes   []*node
	proxies map[[2]int]*proxy // proxies[{from, to}] carries from's connections to node to
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

func newHarness(t *testing.T) *harness {
	t.Helper()
	root := t.TempDir()
	bin := filepath.Join(root, "flotilla")
	if out, err := exec.Command("go", "build", "-o", bin, "../cmd/flotilla").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	h := &harness{t: t, bin: bin, logDir: filepath.Join(root, "logs"), proxies: make(map[[2]int]*proxy)}
	if err := os.MkdirAll(h.logDir, 0o755); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < nodeCount; i++ {
		h.nodes = append(h.nodes, &node{
			id: i, dir: filepath.Join(root, fmt.Sprintf("data%d", i)),
			raftAddr: freeAddr(t), httpAddr: freeAddr(t),
		})
	}
	for _, from := range h.nodes {
		var parts []string
		for _, to := range h.nodes {
			raft := to.raftAddr
			if from != to {
				raft = freeAddr(t)
				p, err := newProxy(raft, to.raftAddr)
				if err != nil {
					t.Fatal(err)
				}
				h.proxies[[2]int{from.id, to.id}] = p
			}
			parts = append(parts, fmt.Sprintf("%d=%s/%s", to.id, raft, to.httpAddr))
		}
		from.spec = strings.Join(parts, ",")
	}
	t.Cleanup(h.shutdown)
	return h
}

func (h *harness) start(n *node) {
	h.t.Helper()
	log, err := os.OpenFile(filepath.Join(h.logDir, fmt.Sprintf("node%d.log", n.id)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		h.t.Fatal(err)
	}
	cmd := exec.Command(h.bin, "server", "-id", strconv.Itoa(n.id), "-cluster", n.spec, "-data", n.dir, "-snapshot-every", "20")
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
	go func() { _ = cmd.Wait(); log.Close() }()
	n.cmd, n.paused = cmd, false
}

// kill is SIGKILL: the process gets no chance to flush or close anything.
func (h *harness) kill(n *node) {
	if n.cmd == nil {
		return
	}
	_ = n.cmd.Process.Signal(syscall.SIGKILL)
	n.cmd = nil
	n.paused = false
}

func (h *harness) pause(n *node)  { _ = n.cmd.Process.Signal(syscall.SIGSTOP); n.paused = true }
func (h *harness) resume(n *node) { _ = n.cmd.Process.Signal(syscall.SIGCONT); n.paused = false }

func (h *harness) shutdown() {
	for _, n := range h.nodes {
		if n.cmd != nil {
			_ = n.cmd.Process.Signal(syscall.SIGCONT)
			h.kill(n)
		}
	}
	for _, p := range h.proxies {
		p.Close()
	}
}

func (h *harness) cut(a, b int, blocked bool) {
	h.proxies[[2]int{a, b}].SetBlocked(blocked)
	h.proxies[[2]int{b, a}].SetBlocked(blocked)
}

// partition cuts every link between the nodes in group and the rest.
func (h *harness) partition(group map[int]bool) {
	for a := 0; a < nodeCount; a++ {
		for b := a + 1; b < nodeCount; b++ {
			if group[a] != group[b] {
				h.cut(a, b, true)
			}
		}
	}
}

func (h *harness) heal() {
	for a := 0; a < nodeCount; a++ {
		for b := a + 1; b < nodeCount; b++ {
			h.cut(a, b, false)
		}
	}
}

func (h *harness) down() int {
	d := 0
	for _, n := range h.nodes {
		if n.cmd == nil || n.paused {
			d++
		}
	}
	return d
}

// nemesis breaks things at random until stop closes, then puts everything back.
func (h *harness) nemesis(rng *rand.Rand, stop <-chan struct{}, log func(string, ...any)) {
	const maxDown = (nodeCount - 1) / 2 // never take down a majority of processes
	pick := func(want func(*node) bool) *node {
		var c []*node
		for _, n := range h.nodes {
			if want(n) {
				c = append(c, n)
			}
		}
		if len(c) == 0 {
			return nil
		}
		return c[rng.Intn(len(c))]
	}
	for {
		select {
		case <-stop:
			return
		case <-time.After(time.Duration(500+rng.Intn(1500)) * time.Millisecond):
		}
		switch rng.Intn(9) {
		case 0, 1:
			if n := pick(func(n *node) bool { return n.cmd != nil && !n.paused }); n != nil && h.down() < maxDown {
				log("kill -9 node %d", n.id)
				h.kill(n)
			}
		case 2:
			if n := pick(func(n *node) bool { return n.cmd == nil }); n != nil {
				log("restart node %d", n.id)
				h.start(n)
			}
		case 3:
			if n := pick(func(n *node) bool { return n.cmd != nil && !n.paused }); n != nil && h.down() < maxDown {
				log("freeze node %d", n.id)
				h.pause(n)
			}
		case 4:
			if n := pick(func(n *node) bool { return n.paused }); n != nil {
				log("unfreeze node %d", n.id)
				h.resume(n)
			}
		case 5:
			h.heal()
			group := map[int]bool{rng.Intn(nodeCount): true}
			if rng.Intn(2) == 0 {
				group[rng.Intn(nodeCount)] = true
			}
			log("partition %v from the rest", keys(group))
			h.partition(group)
		case 6:
			log("heal network")
			h.heal()
		case 7, 8:
			// Cut off whoever leads right now. The rest elect a new leader while the old one may
			// still think it is in charge, which is when a stale answer could slip out.
			if id, ok := h.leader(); ok {
				h.heal()
				log("isolate leader %d", id)
				h.partition(map[int]bool{id: true})
			}
		}
	}
}

// leader asks the running nodes who leads and returns the first answer that names a node.
func (h *harness) leader() (int, bool) {
	cl := &http.Client{Timeout: 300 * time.Millisecond}
	for _, n := range h.nodes {
		if n.cmd == nil || n.paused {
			continue
		}
		resp, err := cl.Get("http://" + n.httpAddr + "/status")
		if err != nil {
			continue
		}
		var st struct {
			ID   int    `json:"id"`
			Role string `json:"role"`
		}
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err == nil && strings.EqualFold(st.Role, "leader") {
			return st.ID, true
		}
	}
	return 0, false
}

func keys(m map[int]bool) []int {
	var out []int
	for k := range m {
		out = append(out, k)
	}
	return out
}

// restoreAll starts every stopped node, unfreezes the frozen ones and heals the network.
func (h *harness) restoreAll() {
	h.heal()
	for _, n := range h.nodes {
		switch {
		case n.cmd == nil:
			h.start(n)
		case n.paused:
			h.resume(n)
		}
	}
}

// client sends operations over HTTP to whichever node answers. A request is retried with the same
// X-Seq until it succeeds, so it runs exactly once however many times it is sent.
type client struct {
	id    int
	seq   int64
	cur   int
	nodes []*node
	http  *http.Client
}

func newClient(id int, nodes []*node) *client {
	return &client{id: id, nodes: nodes, http: &http.Client{Timeout: 2 * time.Second}}
}

func (c *client) do(method, key, body string, deadline time.Time) (string, bool, error) {
	c.seq++
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(method, "http://"+c.nodes[c.cur].httpAddr+"/kv/"+key, strings.NewReader(body))
		req.Header.Set("X-Client-ID", strconv.Itoa(c.id))
		req.Header.Set("X-Seq", strconv.FormatInt(c.seq, 10))
		resp, err := c.http.Do(req)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			switch {
			case resp.StatusCode == http.StatusNotFound && method == http.MethodGet:
				return "", false, nil
			case resp.StatusCode/100 == 2:
				return string(b), true, nil
			}
		}
		c.cur = (c.cur + 1) % len(c.nodes)
		time.Sleep(20 * time.Millisecond)
	}
	return "", false, fmt.Errorf("%s %s: no answer before the deadline", method, key)
}

type recorder struct {
	mu    sync.Mutex
	start time.Time
	ops   []linearize.Op
}

func (r *recorder) now() int64 { return int64(time.Since(r.start)) }
func (r *recorder) add(op linearize.Op) {
	r.mu.Lock()
	r.ops = append(r.ops, op)
	r.mu.Unlock()
}

func TestChaos(t *testing.T) {
	if os.Getenv("FLOTILLA_CHAOS") == "" {
		t.Skip("set FLOTILLA_CHAOS=1 to run (starts real processes and takes about a minute)")
	}
	runFor := 20 * time.Second
	if s, err := strconv.Atoi(os.Getenv("FLOTILLA_CHAOS_SECONDS")); err == nil && s > 0 {
		runFor = time.Duration(s) * time.Second
	}
	seed := time.Now().UnixNano()
	if s, err := strconv.ParseInt(os.Getenv("FLOTILLA_CHAOS_SEED"), 10, 64); err == nil {
		seed = s
	}
	t.Logf("seed %d (FLOTILLA_CHAOS_SEED=%d repeats the fault schedule)", seed, seed)

	h := newHarness(t)
	failed := true
	defer func() {
		if failed {
			t.Logf("node logs: %s", h.logDir)
			// keep the logs: t.TempDir would delete them, so copy them out
			if dst, err := os.MkdirTemp("", "flotilla-chaos-logs-"); err == nil {
				entries, _ := os.ReadDir(h.logDir)
				for _, e := range entries {
					b, _ := os.ReadFile(filepath.Join(h.logDir, e.Name()))
					_ = os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644)
				}
				t.Logf("copied node logs to %s", dst)
			}
		}
	}()
	for _, n := range h.nodes {
		h.start(n)
	}

	rec := &recorder{start: time.Now()}
	keySet := []string{"a", "b", "c", "d", "e", "f"}
	deadline := time.Now().Add(runFor + 90*time.Second)

	// The first write waits for a leader, so the faults start on a working cluster.
	first := newClient(100, h.nodes)
	if _, _, err := first.do(http.MethodPut, "warmup", "1", time.Now().Add(30*time.Second)); err != nil {
		t.Fatalf("cluster never came up: %v", err)
	}

	stop := make(chan struct{})
	var nem sync.WaitGroup
	nem.Add(1)
	go func() {
		defer nem.Done()
		h.nemesis(rand.New(rand.NewSource(seed)), stop, func(f string, a ...any) { t.Logf("nemesis: "+f, a...) })
	}()

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var errs []error
	for id := 1; id <= 5; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl := newClient(id, h.nodes)
			rng := rand.New(rand.NewSource(seed + int64(id)))
			begin := time.Now()
			for i := 0; time.Since(begin) < runFor; i++ {
				time.Sleep(time.Duration(20+rng.Intn(60)) * time.Millisecond)
				key := keySet[rng.Intn(len(keySet))]
				op := linearize.Op{Client: id, Key: key, Call: rec.now()}
				var err error
				switch r := rng.Intn(10); {
				case r < 4:
					op.Kind, op.Value = linearize.Put, fmt.Sprintf("c%d-%d", id, i)
					_, _, err = cl.do(http.MethodPut, key, op.Value, deadline)
				case r < 5:
					op.Kind = linearize.Delete
					_, _, err = cl.do(http.MethodDelete, key, "", deadline)
				default:
					op.Kind = linearize.Get
					op.Value, op.Found, err = cl.do(http.MethodGet, key, "", deadline)
				}
				if err != nil {
					errMu.Lock()
					errs = append(errs, fmt.Errorf("client %d: %w", id, err))
					errMu.Unlock()
					return
				}
				op.Return = rec.now()
				rec.add(op)
			}
		}(id)
	}
	// One reader per node asks that node alone, never following a redirect. A node that has just been
	// frozen, cut off or restarted and still believes it leads would answer from stale state, so what
	// these reads see has to fit the history too. Reads that get no answer from the node are dropped.
	probeEnd := time.Now().Add(runFor)
	for i, n := range h.nodes {
		wg.Add(1)
		go func(id int, n *node) {
			defer wg.Done()
			hc := &http.Client{
				Timeout:       400 * time.Millisecond,
				CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
			}
			rng := rand.New(rand.NewSource(seed + int64(id)))
			for time.Now().Before(probeEnd) {
				time.Sleep(time.Duration(5+rng.Intn(15)) * time.Millisecond)
				key := keySet[rng.Intn(len(keySet))]
				op := linearize.Op{Client: id, Kind: linearize.Get, Key: key, Call: rec.now()}
				resp, err := hc.Get("http://" + n.httpAddr + "/kv/" + key)
				if err != nil {
					continue
				}
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				switch resp.StatusCode {
				case http.StatusOK:
					op.Value, op.Found = string(b), true
				case http.StatusNotFound:
				default:
					continue
				}
				op.Return = rec.now()
				rec.add(op)
			}
		}(300+i, n)
	}
	wg.Wait()
	close(stop)
	nem.Wait()
	for _, err := range errs {
		t.Error(err)
	}

	// With every fault undone, the cluster must serve again, and a final read of each key goes into
	// the history so it also has to agree with everything before it.
	h.restoreAll()
	final := newClient(200, h.nodes)
	for _, key := range keySet {
		op := linearize.Op{Client: 200, Kind: linearize.Get, Key: key, Call: rec.now()}
		v, ok, err := final.do(http.MethodGet, key, "", time.Now().Add(60*time.Second))
		if err != nil {
			t.Fatalf("after healing: %v", err)
		}
		op.Value, op.Found, op.Return = v, ok, rec.now()
		rec.add(op)
	}
	h.waitConverged(t)

	t.Logf("%d operations recorded", len(rec.ops))
	if ok, key := linearize.Check(rec.ops); !ok {
		t.Fatalf("history is not linearizable; operations on key %q:\n%s", key, linearize.Explain(rec.ops, key))
	}
	if t.Failed() {
		return
	}
	failed = false
}

// waitConverged waits until every node has committed the same log index.
func (h *harness) waitConverged(t *testing.T) {
	t.Helper()
	cl := &http.Client{Timeout: time.Second}
	limit := time.Now().Add(30 * time.Second)
	var last string
	for time.Now().Before(limit) {
		seen := map[string]bool{}
		okAll := true
		for _, n := range h.nodes {
			resp, err := cl.Get("http://" + n.httpAddr + "/metrics")
			if err != nil {
				okAll = false
				break
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			for _, line := range strings.Split(string(b), "\n") {
				if strings.HasPrefix(line, "flotilla_commit_index") {
					seen[line[strings.LastIndex(line, " ")+1:]] = true
				}
			}
		}
		if okAll && len(seen) == 1 {
			return
		}
		last = fmt.Sprint(seen)
		time.Sleep(300 * time.Millisecond)
	}
	t.Errorf("nodes did not converge on one commit index within 30s (last seen %s)", last)
}
