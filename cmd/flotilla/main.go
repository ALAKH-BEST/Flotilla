// Command flotilla runs a node of the cluster and talks to it from the terminal.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ALAKH-BEST/flotilla/internal/runner"
)

const usage = `usage:
  flotilla server -id N -cluster SPEC [-data DIR] [-snapshot-every N] [-join]
  flotilla get    [-server ADDRS] KEY
  flotilla put    [-server ADDRS] [-ttl SECONDS] KEY VALUE
  flotilla watch  [-server ADDRS] [-prefix] [-from INDEX] KEY
  flotilla delete [-server ADDRS] KEY
  flotilla status [-server ADDRS]
  flotilla members [-server ADDRS]
  flotilla add    [-server ADDRS] ID RAFT_ADDR/HTTP_ADDR
  flotilla remove [-server ADDRS] ID
  flotilla transfer [-server ADDRS] ID

SPEC is a comma-separated list of id=raftHost:port/httpHost:port, for example
  0=127.0.0.1:7000/127.0.0.1:8000,1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002
ADDRS is a comma-separated list of HTTP addresses (default 127.0.0.1:8000).

To grow a cluster, start the new node with -join (its own entry in -cluster gives its
addresses), then run "flotilla add" with those addresses. "flotilla remove" takes a node out.
"flotilla transfer" hands leadership to a voting node, for example before stopping the leader.
"flotilla put -ttl 30" makes a key expire after 30 seconds (a duration such as 1m30s also works).
"flotilla watch" prints each change to KEY (or, with -prefix, to every key starting with it) as it
happens, and picks up where it left off if the connection to a node drops. -from replays from a
log index first. If that history is no longer kept, it says so and exits.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	var err error
	switch cmd {
	case "server":
		err = runServer(args)
	case "watch":
		err = runWatch(args)
	case "get", "put", "delete", "status", "members", "add", "remove", "transfer":
		err = runClient(cmd, args)
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runServer(args []string) error {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	id := fs.Int("id", -1, "this node's id")
	spec := fs.String("cluster", "", "cluster members: id=raftAddr/httpAddr,...")
	dataDir := fs.String("data", "", "directory for durable state (empty: memory only)")
	snapEvery := fs.Int("snapshot-every", 1000, "take a snapshot every N applied entries")
	join := fs.Bool("join", false, "start outside any cluster, to be added with \"flotilla add\"")
	_ = fs.Parse(args)

	cluster, err := runner.ParseCluster(*spec)
	if err != nil {
		return err
	}
	r, err := runner.Start(runner.Config{ID: *id, Cluster: cluster, DataDir: *dataDir, SnapshotEvery: *snapEvery, Join: *join})
	if err != nil {
		return err
	}
	fmt.Printf("node %d up: raft %s, http %s, data %q\n", *id, r.RaftAddr(), r.HTTPAddr(), *dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down")
	r.Stop()
	return nil
}

func runClient(cmd string, args []string) error {
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	servers := fs.String("server", "127.0.0.1:8000", "comma-separated HTTP addresses")
	ttl := fs.String("ttl", "", "put only: remove the key after this long (seconds, or a duration like 1m30s)")
	_ = fs.Parse(args)
	rest := fs.Args()
	if *ttl != "" && cmd != "put" {
		return fmt.Errorf("-ttl only applies to put")
	}

	var method, path, body string
	want := 1
	switch cmd {
	case "get":
		method = http.MethodGet
	case "put":
		method, want = http.MethodPut, 2
	case "delete":
		method = http.MethodDelete
	case "status":
		method, want, path = http.MethodGet, 0, "/status"
	case "members":
		method, want, path = http.MethodGet, 0, "/admin/members"
	case "add":
		method, want = http.MethodPut, 2
	case "remove":
		method = http.MethodDelete
	case "transfer":
		method = http.MethodPut
	}
	if len(rest) != want {
		return fmt.Errorf("%s takes %d argument(s)\n\n%s", cmd, want, usage)
	}
	switch cmd {
	case "get", "put", "delete":
		path = "/kv/" + rest[0]
		if *ttl != "" {
			path += "?ttl=" + url.QueryEscape(*ttl)
		}
	case "add", "remove", "transfer":
		if _, err := strconv.Atoi(rest[0]); err != nil {
			return fmt.Errorf("the node ID must be a number, got %q", rest[0])
		}
		path = "/admin/members/" + rest[0]
		if cmd == "transfer" {
			path = "/admin/leader/" + rest[0]
		}
	}
	if cmd == "put" || cmd == "add" {
		body = rest[len(rest)-1]
	}

	timeout := 5 * time.Second
	if cmd == "add" {
		timeout = 70 * time.Second // the server waits for the new node to catch up first
	}
	client := &http.Client{Timeout: timeout}
	var lastErr error
	for _, addr := range strings.Split(*servers, ",") {
		req, err := http.NewRequest(method, "http://"+strings.TrimSpace(addr)+path, strings.NewReader(body))
		if err != nil {
			return err
		}
		resp, err := client.Do(req) // follows the leader redirect
		if err != nil {
			lastErr = err
			continue
		}
		out, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		switch {
		case resp.StatusCode == http.StatusNotFound:
			return fmt.Errorf("not found")
		case resp.StatusCode >= 500:
			lastErr = fmt.Errorf("%s: %s", addr, strings.TrimSpace(string(out)))
			continue
		case resp.StatusCode >= 300:
			return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(out)))
		}
		if len(out) > 0 {
			fmt.Println(strings.TrimRight(string(out), "\n"))
		}
		return nil
	}
	return fmt.Errorf("no server answered: %v", lastErr)
}

// runWatch prints changes as they happen. If the connection drops it reconnects, to the next
// address in turn, and resumes after the last event it printed, so nothing is missed or repeated.
func runWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	servers := fs.String("server", "127.0.0.1:8000", "comma-separated HTTP addresses")
	prefix := fs.Bool("prefix", false, "watch every key that starts with the argument")
	from := fs.Int("from", 0, "start at this log index, replaying what a node still holds")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("watch takes 1 argument\n\n%s", usage)
	}
	addrs := strings.Split(*servers, ",")
	param := "key"
	if *prefix {
		param = "prefix"
	}
	last := *from - 1 // the index of the last event printed; -1 means start from now
	if *from == 0 {
		last = -1
	}
	started := *from > 0
	var lastErr error
	for attempt := 0; ; attempt++ {
		addr := strings.TrimSpace(addrs[attempt%len(addrs)])
		q := url.Values{param: {fs.Arg(0)}}
		if started {
			q.Set("from", strconv.Itoa(last+1))
		}
		resp, err := http.Get("http://" + addr + "/watch?" + q.Encode())
		if err != nil {
			lastErr = err
		} else if resp.StatusCode == http.StatusGone {
			var gone struct{ Oldest int }
			_ = json.NewDecoder(resp.Body).Decode(&gone)
			resp.Body.Close()
			return fmt.Errorf("history before index %d is no longer kept: read the keys again and watch from there", gone.Oldest)
		} else if resp.StatusCode != http.StatusOK {
			msg, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusBadRequest {
				return fmt.Errorf("%s", strings.TrimSpace(string(msg)))
			}
			lastErr = fmt.Errorf("%s: %s", addr, strings.TrimSpace(string(msg)))
		} else {
			sc := bufio.NewScanner(resp.Body)
			name := ""
			for sc.Scan() {
				line := sc.Text()
				switch {
				case strings.HasPrefix(line, "event: "):
					name = line[7:]
				case strings.HasPrefix(line, "data: "):
					if name == "ready" {
						var r struct{ From int }
						if json.Unmarshal([]byte(line[6:]), &r) == nil && !started {
							last, started = r.From-1, true
						}
						continue
					}
					if name == "compacted" {
						resp.Body.Close()
						return fmt.Errorf("fell too far behind: %s", line[6:])
					}
					var e struct {
						Index     int
						Type, Key string
						Value     string
						ExpireAt  int64 `json:"expire_at"`
					}
					if json.Unmarshal([]byte(line[6:]), &e) != nil {
						continue
					}
					last, started = e.Index, true
					switch e.Type {
					case "put":
						fmt.Printf("%d put %s = %s\n", e.Index, e.Key, e.Value)
					default:
						fmt.Printf("%d %s %s\n", e.Index, e.Type, e.Key)
					}
				}
			}
			resp.Body.Close()
			lastErr = fmt.Errorf("%s: the stream ended", addr)
		}
		if attempt >= 3*len(addrs) && lastErr != nil && !started {
			return fmt.Errorf("no server answered: %v", lastErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
