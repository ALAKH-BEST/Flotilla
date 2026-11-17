package server_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/server"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	body, _ := io.ReadAll(rec.Result().Body)
	return rec.Code, string(body)
}

func TestHealthAndMetrics(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	if err := c.clerk(1).Put(ctx(t, 20*time.Second), "k", "v"); err != nil {
		t.Fatal(err)
	}
	leaders := 0
	for i := 0; i < 3; i++ {
		h := c.servers[i].HTTPHandler(server.HTTPOptions{})
		if code, body := get(t, h, "/healthz"); code != 200 || body != "ok\n" {
			t.Fatalf("node %d /healthz = %d %q", i, code, body)
		}
		code, body := get(t, h, "/metrics")
		if code != 200 {
			t.Fatalf("node %d /metrics = %d", i, code)
		}
		for _, want := range []string{
			`flotilla_voters{node="` + string(rune('0'+i)) + `"} 3`,
			`flotilla_removed{node="` + string(rune('0'+i)) + `"} 0`,
			"# TYPE flotilla_term gauge",
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("node %d /metrics missing %q:\n%s", i, want, body)
			}
		}
		if strings.Contains(body, `flotilla_is_leader{node="`+string(rune('0'+i))+`"} 1`) {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d nodes report leader, want 1", leaders)
	}
}

// Once the cluster removes a node it reports itself unhealthy, so an orchestrator replaces it.
func TestHealthzFailsAfterRemoval(t *testing.T) {
	c := newKVCluster(t, 4, 0)
	if err := c.change(10*time.Second, func(ctx context.Context, s *server.Server) error {
		return s.RemoveServer(ctx, 3)
	}); err != nil {
		t.Fatal(err)
	}
	h := c.servers[3].HTTPHandler(server.HTTPOptions{})
	deadline := time.Now().Add(10 * time.Second)
	for {
		code, _ := get(t, h, "/healthz")
		if code == http.StatusServiceUnavailable {
			if _, body := get(t, h, "/metrics"); !strings.Contains(body, `flotilla_removed{node="3"} 1`) {
				t.Fatalf("removed node's metrics do not say so:\n%s", body)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("removed node still reports /healthz = %d", code)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func put(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("PUT", path, nil))
	return rec.Code
}

// PUT /admin/leader/{id} moves leadership, and the cluster keeps serving writes afterwards. A
// follower does not do it (it has no leader address to redirect to here, so it says 503), a node
// that cannot lead is refused with 409, and a bad ID with 400.
func TestAdminLeaderHandsOverLeadership(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	dl := ctx(t, 30*time.Second)
	if err := c.clerk(1).Put(dl, "k", "v"); err != nil {
		t.Fatal(err)
	}
	leader := c.leader(t)
	target := (leader + 1) % 3
	lh := c.servers[leader].HTTPHandler(server.HTTPOptions{})

	if code := put(t, c.servers[target].HTTPHandler(server.HTTPOptions{}), "/admin/leader/"+string(rune('0'+leader))); code != http.StatusServiceUnavailable {
		t.Errorf("from a follower: %d, want 503", code)
	}
	if code := put(t, lh, "/admin/leader/9"); code != http.StatusConflict {
		t.Errorf("to a stranger: %d, want 409", code)
	}
	if code := put(t, lh, "/admin/leader/x"); code != http.StatusBadRequest {
		t.Errorf("to a bad ID: %d, want 400", code)
	}
	if code := put(t, lh, "/admin/leader/"+string(rune('0'+target))); code != http.StatusNoContent {
		t.Fatalf("hand-over: %d, want 204", code)
	}
	if got := c.leader(t); got != target {
		t.Fatalf("node %d leads, want %d", got, target)
	}
	if err := c.clerk(2).Put(dl, "k2", "v2"); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := c.clerk(3).Get(dl, "k"); err != nil || !ok || v != "v" {
		t.Fatalf("read after hand-over: %q %v %v", v, ok, err)
	}
}
