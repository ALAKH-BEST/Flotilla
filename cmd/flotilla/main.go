// Command flotilla runs a node of the cluster and talks to it from the terminal.
package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ALAKH-BEST/flotilla/internal/runner"
)

const usage = `usage:
  flotilla server -id N -cluster SPEC [-data DIR] [-snapshot-every N]
  flotilla get    [-server ADDRS] KEY
  flotilla put    [-server ADDRS] KEY VALUE
  flotilla delete [-server ADDRS] KEY
  flotilla status [-server ADDRS]

SPEC is a comma-separated list of id=raftHost:port/httpHost:port, for example
  0=127.0.0.1:7000/127.0.0.1:8000,1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002
ADDRS is a comma-separated list of HTTP addresses (default 127.0.0.1:8000).
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
	case "get", "put", "delete", "status":
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
	_ = fs.Parse(args)

	cluster, err := runner.ParseCluster(*spec)
	if err != nil {
		return err
	}
	r, err := runner.Start(runner.Config{ID: *id, Cluster: cluster, DataDir: *dataDir, SnapshotEvery: *snapEvery})
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
	_ = fs.Parse(args)
	rest := fs.Args()

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
	}
	if len(rest) != want {
		return fmt.Errorf("%s takes %d argument(s)\n\n%s", cmd, want, usage)
	}
	if cmd != "status" {
		path = "/kv/" + rest[0]
	}
	if cmd == "put" {
		body = rest[1]
	}

	client := &http.Client{Timeout: 5 * time.Second}
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
