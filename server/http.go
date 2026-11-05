package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

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
//	GET    /kv/{key}   the value (404 if the key does not exist)
//	PUT    /kv/{key}   set the key to the request body
//	DELETE /kv/{key}   remove the key
//	GET    /status     this node's view of the cluster, as JSON
//
// Membership (changes are followed by a redirect to the leader, like writes):
//
//	GET    /admin/members       the voting members
//	PUT    /admin/members/{id}  add node id; the body is its address, passed on to every node
//	DELETE /admin/members/{id}  remove node id
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
		_, _ = io.WriteString(w, res.Value)
	})
	mux.HandleFunc("PUT /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxValueBytes))
		if err != nil {
			http.Error(w, "value too large or unreadable", http.StatusRequestEntityTooLarge)
			return
		}
		if _, ok := s.serve(w, r, opts, kv.OpPut, r.PathValue("key"), string(body)); ok {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("DELETE /kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := s.serve(w, r, opts, kv.OpDelete, r.PathValue("key"), ""); ok {
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("GET /admin/members", func(w http.ResponseWriter, r *http.Request) {
		type member struct {
			ID   int    `json:"id"`
			Addr string `json:"addr,omitempty"`
		}
		var out []member
		for _, m := range s.node.Members() {
			out = append(out, member{m.ID, m.Addr})
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
		})
	})
	return mux
}

// serve runs one command and writes an error response if it fails. It reports whether the caller
// should write the success response.
func (s *Server) serve(w http.ResponseWriter, r *http.Request, opts HTTPOptions, op kv.Op, key, value string) (kv.Result, bool) {
	if key == "" {
		http.Error(w, "empty key", http.StatusBadRequest)
		return kv.Result{}, false
	}
	cmd := kv.Command{Op: op, Key: key, Value: value}
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
		errors.Is(err, raft.ErrLastMember), errors.Is(err, raft.ErrChangeInProgress):
		http.Error(w, err.Error(), http.StatusConflict)
	default: // including raft.ErrNotReady: a new leader that cannot yet accept this, so try again
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	}
}
