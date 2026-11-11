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
