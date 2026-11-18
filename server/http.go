package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
	"github.com/ALAKH-BEST/flotilla/raft"
)

const maxValueBytes = 1 << 20

// HTTPOptions configures the HTTP API.
type HTTPOptions struct {
	// LeaderAddr returns the HTTP address (host:port) of a node, or "" if unknown. It lets a
	// follower send clients to the leader with a redirect.
	LeaderAddr func(id int) string
}

// HTTPHandler returns the client API:
//
//	GET    /kv/{key}   the value (404 if the key does not exist); the header X-Expires-At holds
//	                   the key's expiry in Unix milliseconds if it has a time to live
//	PUT    /kv/{key}   set the key to the request body; with ?ttl=30 (seconds, or a duration such
//	                   as 1m30s) the key is removed after that long
//	DELETE /kv/{key}   remove the key
//	GET    /watch      a stream of changes (server-sent events); see the comment on watch
//	GET    /status     this node's view of the cluster, as JSON
//	GET    /healthz    200 while this node is running, 503 once the cluster has removed it
//	GET    /metrics    this node's state as Prometheus gauges (plain text)
//
// Membership (changes are followed by a redirect to the leader, like writes):
//
//	GET    /admin/members       the members; learners are marked
//	PUT    /admin/members/{id}  add node id as a learner, then promote it to a voter once it has
//	                            caught up; the body is its address, passed on to every node
//	DELETE /admin/members/{id}  remove node id
//	PUT    /admin/leader/{id}   hand leadership to voting member id (204 once this node has stepped
//	                            down; 504 if id did not take over in time and this node stays leader)
//
// A node that is not the leader answers with a 307 redirect to the leader when it knows
// one, and 503 otherwise. A client that wants a retried write to run exactly once sends
// the headers X-Client-ID and X-Seq (increasing, one request at a time per client).
func (s *Server) HTTPHandler(opts HTTPOptions) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		res, ok := s.serve(w, r, opts, kv.OpGet, r.PathValue("key"), "")
		if !ok {
			return
		}
		if !res.Found {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if res.ExpireAt > 0 {
			w.Header().Set("X-Expires-At", strconv.FormatInt(res.ExpireAt, 10))
		}
		_, _ = io.WriteString(w, res.Value)
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
		if err != nil {
			http.Error(w, "value too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		var expireAt int64
		if v := r.URL.Query().Get("ttl"); v != "" {
			ttl, err := parseTTL(v)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			expireAt = s.now().Add(ttl).UnixMilli()
		}
		if _, ok := s.serveWith(w, r, opts, kv.Command{Op: kv.OpPut, Key: r.PathValue("key"), Value: string(body), ExpireAt: expireAt}); ok {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("GET /watch", s.watch)
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.serve(w, r, opts, kv.OpDelete, r.PathValue("key"), ""); ok {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("GET /admin/members", func(w http.ResponseWriter, r *http.Request) {
		type member struct {
			ID      int    `json:"id"`
			Addr    string `json:"addr,omitempty"`
			Learner bool   `json:"learner,omitempty"`
		}
		var out []member
		for _, m := range s.node.Members() {
			out = append(out, member{m.ID, m.Addr, m.Learner})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("PUT /admin/members/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		addr, err2 := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024))
		if err != nil || err2 != nil || id < 0 {
			http.Error(w, "want PUT /admin/members/{id} with the node's address as the body", http.StatusBadRequest)
			return
		}
		if err := s.AddServer(r.Context(), id, string(addr)); err != nil {
			s.fail(w, r, opts, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /admin/members/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || id < 0 {
			http.Error(w, "want DELETE /admin/members/{id}", http.StatusBadRequest)
			return
		}
		if err := s.RemoveServer(r.Context(), id); err != nil {
			s.fail(w, r, opts, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("PUT /admin/leader/{id}", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil || id < 0 {
			http.Error(w, "want PUT /admin/leader/{id}", http.StatusBadRequest)
			return
		}
		if err := s.TransferLeadership(r.Context(), id); err != nil {
			s.fail(w, r, opts, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		st := s.node.Status()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":            st.ID,
			"role":          st.Role.String(),
			"term":          st.Term,
			"leader":        st.LeaderID,
			"commitIndex":   st.CommitIndex,
			"lastLogIndex":  st.LastLogIndex,
			"snapshotIndex": st.SnapshotIndex,
			"logEntries":    st.LogEntries,
			"members":       st.Members,
			"learners":      st.Learners,
		})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if s.node.Status().Removed {
			http.Error(w, "removed from the cluster", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		st := s.node.Status()
		b2i := func(b bool) int {
			if b {
				return 1
			}
			return 0
		}
		gauges := []struct {
			name, help string
			value      int
		}{
			{"flotilla_is_leader", "1 if this node is the leader.", b2i(st.Role == raft.Leader)},
			{"flotilla_term", "Current term.", st.Term},
			{"flotilla_commit_index", "Highest log index known to be committed.", st.CommitIndex},
			{"flotilla_last_log_index", "Highest log index in this node's log.", st.LastLogIndex},
			{"flotilla_snapshot_index", "Last log index covered by the snapshot (0 if none).", st.SnapshotIndex},
			{"flotilla_log_entries", "Log entries held in memory after the snapshot.", st.LogEntries},
			{"flotilla_voters", "Voting members this node uses.", len(st.Members)},
			{"flotilla_learners", "Non-voting members this node replicates to.", len(st.Learners)},
			{"flotilla_removed", "1 once a committed change has removed this node.", b2i(st.Removed)},
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		for _, g := range gauges {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s{node=\"%d\"} %d\n", g.name, g.help, g.name, g.name, st.ID, g.value)
		}
	})
	return mux
}

// serve runs one command and writes an error response if it fails. It reports whether the caller
// should write the success response.
func (s *Server) serve(w http.ResponseWriter, r *http.Request, opts HTTPOptions, op kv.Op, key, value string) (kv.Result, bool) {
	return s.serveWith(w, r, opts, kv.Command{Op: op, Key: key, Value: value})
}

func (s *Server) serveWith(w http.ResponseWriter, r *http.Request, opts HTTPOptions, cmd kv.Command) (kv.Result, bool) {
	if cmd.Key == "" {
		http.Error(w, "empty key", http.StatusBadRequest)
		return kv.Result{}, false
	}
	if id, seq := r.Header.Get("X-Client-ID"), r.Header.Get("X-Seq"); id != "" || seq != "" {
		var err1, err2 error
		cmd.ClientID, err1 = strconv.ParseInt(id, 10, 64)
		cmd.Seq, err2 = strconv.ParseInt(seq, 10, 64)
		if err1 != nil || err2 != nil || cmd.ClientID <= 0 || cmd.Seq <= 0 {
			http.Error(w, "X-Client-ID and X-Seq must both be positive integers", http.StatusBadRequest)
			return kv.Result{}, false
		}
	}

	res, err := s.Do(r.Context(), cmd)
	if err != nil {
		s.fail(w, r, opts, err)
		return kv.Result{}, false
	}
	return res, true
}

// fail writes the response for an error from Do, AddServer or RemoveServer.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, opts HTTPOptions, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		st := s.node.Status()
		if opts.LeaderAddr != nil && st.LeaderID >= 0 && st.LeaderID != st.ID {
			if addr := opts.LeaderAddr(st.LeaderID); addr != "" {
				http.Redirect(w, r, "http://"+addr+r.URL.RequestURI(), http.StatusTemporaryRedirect)
				return
			}
		}
		http.Error(w, "no leader known, try again shortly", http.StatusServiceUnavailable)
	case errors.Is(err, ErrTimeout), errors.Is(err, context.DeadlineExceeded):
		http.Error(w, "request timed out", http.StatusGatewayTimeout)
	case errors.Is(err, context.Canceled):
		// the client went away; nothing to write
	case errors.Is(err, raft.ErrAlreadyMember), errors.Is(err, raft.ErrNotMember),
		errors.Is(err, raft.ErrLastMember), errors.Is(err, raft.ErrChangeInProgress),
		errors.Is(err, raft.ErrNotLearner), errors.Is(err, raft.ErrBadTransferTarget),
		errors.Is(err, raft.ErrTransferInProgress):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, raft.ErrTransferTimeout):
		http.Error(w, err.Error(), http.StatusGatewayTimeout)
	default: // including raft.ErrNotReady: a new leader that cannot yet accept this, so try again
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}

// maxTTL keeps an expiry well inside what a Unix-millisecond timestamp can hold.
const maxTTL = 10 * 365 * 24 * time.Hour

// parseTTL reads a time to live given as whole seconds ("30") or a duration ("1m30s").
func parseTTL(v string) (time.Duration, error) {
	var d time.Duration
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs > int64(maxTTL/time.Second) {
			return 0, fmt.Errorf("ttl %q is longer than the %v maximum", v, maxTTL)
		}
		d = time.Duration(secs) * time.Second
	} else if d, err = time.ParseDuration(v); err != nil {
		return 0, fmt.Errorf("ttl %q is neither seconds nor a duration such as 1m30s", v)
	}
	if d <= 0 || d > maxTTL {
		return 0, fmt.Errorf("ttl %q must be positive and at most %v", v, maxTTL)
	}
	return d, nil
}

// watch streams changes as server-sent events:
//
//	GET /watch?key=K         changes to the key K
//	GET /watch?prefix=P      changes to every key starting with P (an empty P means every key)
//	    &from=N              start at log index N, replaying what this node still holds; without
//	                         it, only changes from now on. The Last-Event-ID header (which
//	                         EventSource sends when it reconnects) means "after that index".
//
// The stream opens with event: ready and {"from": N}, the first index it will report. Each event has id: the log index, event: put, delete or expire, and data: a JSON object with the
// index, type, key and for puts the value and expiry. Any node serves a watch, the leader or not:
// events come in log order with none missing and none repeated, but a follower may be behind the
// leader. If the node no longer holds the history asked for, or the watcher falls so far behind
// that it was dropped, the answer is 410 Gone (or, mid-stream, an event: compacted) with the
// oldest index available; read the keys again and watch from the index of that read.
func (s *Server) watch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var match func(string) bool
	switch {
	case q.Has("key") && !q.Has("prefix"):
		key := q.Get("key")
		if key == "" {
			http.Error(w, "empty key", http.StatusBadRequest)
			return
		}
		match = func(k string) bool { return k == key }
	case q.Has("prefix") && !q.Has("key"):
		prefix := q.Get("prefix")
		match = func(k string) bool { return strings.HasPrefix(k, prefix) }
	default:
		http.Error(w, "want /watch?key=K or /watch?prefix=P", http.StatusBadRequest)
		return
	}
	from := 0
	if v := q.Get("from"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			http.Error(w, "from must be a log index of 1 or more", http.StatusBadRequest)
			return
		}
		from = n
	} else if id := r.Header.Get("Last-Event-ID"); id != "" {
		n, err := strconv.Atoi(id)
		if err != nil || n < 0 {
			http.Error(w, "Last-Event-ID must be a log index", http.StatusBadRequest)
			return
		}
		from = n + 1
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming is not supported here", http.StatusInternalServerError)
		return
	}
	watcher, err := s.store.Watch(from, match)
	var compacted *kv.CompactedError
	if errors.As(err, &compacted) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusGone)
		_ = json.NewEncoder(w).Encode(map[string]int{"oldest": compacted.Oldest})
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer watcher.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	go func() {
		select {
		case <-s.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	// Say where the stream starts, so a client that asked for "from now" knows which index to
	// resume from if the connection drops before the first change.
	fmt.Fprintf(w, "event: ready\ndata: {\"from\":%d}\n\n", watcher.Start())
	flusher.Flush()
	for {
		wait, stop := context.WithTimeout(ctx, 15*time.Second)
		events, err := watcher.Next(wait)
		stop()
		switch {
		case errors.As(err, &compacted):
			fmt.Fprintf(w, "event: compacted\ndata: {\"oldest\":%d}\n\n", compacted.Oldest)
			flusher.Flush()
			return
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
			_, _ = io.WriteString(w, ": keep-alive\n\n") // also notices a client that went away
			flusher.Flush()
			continue
		case err != nil:
			return
		}
		for _, e := range events {
			data, _ := json.Marshal(e)
			if _, err := fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", e.Index, e.Type, data); err != nil {
				return
			}
		}
		flusher.Flush()
	}
}
