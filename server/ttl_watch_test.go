package server_test

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
	"github.com/ALAKH-BEST/flotilla/server"
)

func (c *kvCluster) handler(i int) http.Handler {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i].HTTPHandler(server.HTTPOptions{})
}

func request(t *testing.T, h http.Handler, method, path, body string) (int, string, http.Header) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	resp := rec.Result()
	b, _ := io.ReadAll(resp.Body)
	return rec.Code, string(b), resp.Header
}

// waitEverywhere waits until every running replica holds key.
func (c *kvCluster) waitEverywhere(t *testing.T, key string) {
	t.Helper()
	limit := time.Now().Add(5 * time.Second)
	for time.Now().Before(limit) {
		all := true
		c.mu.Lock()
		for _, srv := range c.servers {
			if srv != nil {
				if _, ok := srv.Store().Get(key); !ok {
					all = false
				}
			}
		}
		c.mu.Unlock()
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("key %q did not reach every replica", key)
}

// waitGone waits until no running replica holds key.
func (c *kvCluster) waitGone(t *testing.T, key string, within time.Duration) {
	t.Helper()
	limit := time.Now().Add(within)
	for time.Now().Before(limit) {
		present := false
		c.mu.Lock()
		for _, srv := range c.servers {
			if srv != nil {
				if _, ok := srv.Store().Get(key); ok {
					present = true
				}
			}
		}
		c.mu.Unlock()
		if !present {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("key %q is still on a replica after %v", key, within)
}

func TestTTLKeyIsRemovedOnEveryReplica(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	l := c.leader(t)
	h := c.handler(l)
	if code, _, _ := request(t, h, "PUT", "/kv/session?ttl=1", "abc"); code != http.StatusNoContent {
		t.Fatalf("PUT with ttl: %d", code)
	}
	if code, _, _ := request(t, h, "PUT", "/kv/forever", "x"); code != http.StatusNoContent {
		t.Fatalf("PUT without ttl: %d", code)
	}
	code, body, hdr := request(t, h, "GET", "/kv/session", "")
	if code != http.StatusOK || body != "abc" {
		t.Fatalf("GET before it expires: %d %q", code, body)
	}
	at, err := strconv.ParseInt(hdr.Get("X-Expires-At"), 10, 64)
	if err != nil || at < time.Now().UnixMilli() || at > time.Now().Add(2*time.Second).UnixMilli() {
		t.Fatalf("X-Expires-At = %q, want about a second from now", hdr.Get("X-Expires-At"))
	}
	if _, _, hdr := request(t, h, "GET", "/kv/forever", ""); hdr.Get("X-Expires-At") != "" {
		t.Fatal("a key with no ttl reports an expiry")
	}

	c.waitGone(t, "session", 6*time.Second)
	if code, _, _ := request(t, c.handler(c.leader(t)), "GET", "/kv/session", ""); code != http.StatusNotFound {
		t.Fatalf("GET after it expired: %d, want 404", code)
	}
	if v, ok := c.servers[l].Store().Get("forever"); !ok || v != "x" {
		t.Fatal("a key with no ttl was removed")
	}
}

// Writing a key again renews its time to live: the earlier expiry, even if already proposed, is ignored.
func TestRewritingAKeyRenewsItsTTL(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	h := c.handler(c.leader(t))
	request(t, h, "PUT", "/kv/k?ttl=1", "one")
	time.Sleep(600 * time.Millisecond)
	request(t, h, "PUT", "/kv/k?ttl=3", "two")
	time.Sleep(1500 * time.Millisecond) // past the first expiry
	if code, body, _ := request(t, c.handler(c.leader(t)), "GET", "/kv/k", ""); code != http.StatusOK || body != "two" {
		t.Fatalf("after the first expiry: %d %q, want the renewed key", code, body)
	}
	c.waitGone(t, "k", 6*time.Second)

	request(t, c.handler(c.leader(t)), "PUT", "/kv/j?ttl=1", "x")
	request(t, c.handler(c.leader(t)), "PUT", "/kv/j", "y") // now with no ttl at all
	time.Sleep(1500 * time.Millisecond)
	if v, ok := c.servers[c.leader(t)].Store().Get("j"); !ok || v != "y" {
		t.Fatal("a key rewritten without a ttl was removed")
	}
}

// The leader dies before the key expires; the next leader removes it.
func TestTTLKeyExpiresAfterALeaderChange(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	l := c.leader(t)
	request(t, c.handler(l), "PUT", "/kv/k?ttl=2", "v")
	c.waitEverywhere(t, "k") // every replica has it, so "gone" below means removed, not never arrived
	c.crash(l)
	c.waitGone(t, "k", 10*time.Second)
}

func TestBadTTLIsRejected(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	h := c.handler(c.leader(t))
	for _, ttl := range []string{"0", "-5", "abc", "99999999999999", "0s", "-1m"} {
		if code, _, _ := request(t, h, "PUT", "/kv/k?ttl="+ttl, "v"); code != http.StatusBadRequest {
			t.Errorf("ttl=%s: %d, want 400", ttl, code)
		}
	}
	if code, _, _ := request(t, h, "PUT", "/kv/k?ttl=1m30s", "v"); code != http.StatusNoContent {
		t.Errorf("ttl=1m30s: %d, want 204", code)
	}
}

// sseEvent is one event read off a watch stream.
type sseEvent struct {
	id, name string
	data     kv.Event
}

// openWatch connects to a watch and sends its events on the returned channel, which is closed when
// the stream ends. It returns the response too, to check headers.
func openWatch(t *testing.T, base, query string, header map[string]string) (<-chan sseEvent, *http.Response) {
	t.Helper()
	req, _ := http.NewRequest("GET", base+"/watch?"+query, nil)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	ch := make(chan sseEvent, 64)
	go func() {
		defer close(ch)
		sc := bufio.NewScanner(resp.Body)
		var cur sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if cur.name != "" {
					ch <- cur
				}
				cur = sseEvent{}
			case strings.HasPrefix(line, "id: "):
				cur.id = line[4:]
			case strings.HasPrefix(line, "event: "):
				cur.name = line[7:]
			case strings.HasPrefix(line, "data: "):
				_ = json.Unmarshal([]byte(line[6:]), &cur.data)
				var r struct{ From int }
				if json.Unmarshal([]byte(line[6:]), &r) == nil && cur.name == "ready" {
					cur.data.Index = r.From // for a ready event, the index the stream starts at
				}
			}
		}
	}()
	return ch, resp
}

// recv returns the next change, skipping the ready event that opens every stream.
func recv(t *testing.T, ch <-chan sseEvent) sseEvent {
	t.Helper()
	for {
		e := recvAny(t, ch)
		if e.name != "ready" {
			return e
		}
	}
}

func recvAny(t *testing.T, ch <-chan sseEvent) sseEvent {
	t.Helper()
	select {
	case e, ok := <-ch:
		if !ok {
			t.Fatal("the watch stream ended")
		}
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("no event within 5 seconds")
	}
	return sseEvent{}
}

func TestWatchStreamsChangesToAKeyInOrder(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	l := c.leader(t)
	ts := httptest.NewServer(c.handler(l))
	t.Cleanup(ts.Close) // runs after the stream is closed, which Close waits for
	ch, resp := openWatch(t, ts.URL, "key=a", nil)
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	h := c.handler(l)
	request(t, h, "PUT", "/kv/a", "1")
	request(t, h, "PUT", "/kv/b", "ignored")
	request(t, h, "PUT", "/kv/a", "2")
	request(t, h, "DELETE", "/kv/a", "")

	var got []sseEvent
	for i := 0; i < 3; i++ {
		got = append(got, recv(t, ch))
	}
	want := []struct{ name, key, value string }{{"put", "a", "1"}, {"put", "a", "2"}, {"delete", "a", ""}}
	last := 0
	for i, w := range want {
		e := got[i]
		if e.name != w.name || e.data.Key != w.key || e.data.Value != w.value || e.data.Type != w.name {
			t.Fatalf("event %d = %+v, want %+v", i, e, w)
		}
		if e.data.Index <= last || e.id != strconv.Itoa(e.data.Index) {
			t.Fatalf("event %d has id %q and index %d after %d", i, e.id, e.data.Index, last)
		}
		last = e.data.Index
	}
}

func TestWatchPrefixSeesExpiry(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	l := c.leader(t)
	ts := httptest.NewServer(c.handler(l))
	t.Cleanup(ts.Close) // runs after the stream is closed, which Close waits for
	ch, _ := openWatch(t, ts.URL, "prefix=cfg:", nil)
	h := c.handler(l)
	request(t, h, "PUT", "/kv/other", "x")
	request(t, h, "PUT", "/kv/cfg:lease?ttl=1", "v")
	if e := recv(t, ch); e.name != "put" || e.data.Key != "cfg:lease" || e.data.ExpireAt == 0 {
		t.Fatalf("first event = %+v, want the put with its expiry", e)
	}
	if e := recv(t, ch); e.name != "expire" || e.data.Key != "cfg:lease" {
		t.Fatalf("second event = %+v, want the expire", e)
	}
}

// A watcher that reconnects, even to a different node, replays from where it left off and sees the
// same events in the same order.
func TestWatchResumesFromAnIndexOnAnotherNode(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	l := c.leader(t)
	h := c.handler(l)
	for i := 1; i <= 5; i++ {
		request(t, h, "PUT", "/kv/k", strconv.Itoa(i))
	}
	lts := httptest.NewServer(h)
	t.Cleanup(lts.Close) // runs after the stream is closed, which Close waits for
	lch, _ := openWatch(t, lts.URL, "key=k&from=1", nil)
	var seen []kv.Event
	for i := 0; i < 5; i++ {
		seen = append(seen, recv(t, lch).data)
	}

	follower := (l + 1) % 3
	c.waitConverged()
	fts := httptest.NewServer(c.handler(follower))
	t.Cleanup(fts.Close)    // runs after the stream is closed, which Close waits for
	resume := seen[2].Index // the third event was the last one this client saw
	fch, _ := openWatch(t, fts.URL, "key=k", map[string]string{"Last-Event-ID": strconv.Itoa(resume)})
	for i := 3; i < 5; i++ {
		got := recv(t, fch).data
		if got != seen[i] {
			t.Fatalf("after resuming on a follower, event %d = %+v, want %+v", i, got, seen[i])
		}
	}
	request(t, h, "PUT", "/kv/k", "6") // and it keeps following
	if e := recv(t, fch); e.data.Value != "6" {
		t.Fatalf("live event on the follower = %+v", e)
	}
}

func TestWatchFromADroppedIndexIsGone(t *testing.T) {
	c := newKVClusterOpts(t, 3, 3, 0, 3)
	l := c.leader(t)
	h := c.handler(l)
	for i := 0; i < 10; i++ {
		request(t, h, "PUT", "/kv/k", strconv.Itoa(i))
	}
	code, body, _ := request(t, h, "GET", "/watch?key=k&from=1", "")
	if code != http.StatusGone {
		t.Fatalf("watching from a dropped index: %d, want 410", code)
	}
	var out struct{ Oldest int }
	if err := json.Unmarshal([]byte(body), &out); err != nil || out.Oldest <= 1 {
		t.Fatalf("body %q: want the oldest index still held", body)
	}
	// From that index it works.
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close) // runs after the stream is closed, which Close waits for
	ch, _ := openWatch(t, ts.URL, "key=k&from="+strconv.Itoa(out.Oldest), nil)
	if e := recv(t, ch); e.data.Index != out.Oldest {
		t.Fatalf("first event index %d, want %d", e.data.Index, out.Oldest)
	}
}

func TestWatchRefusesBadRequests(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	h := c.handler(c.leader(t))
	for _, q := range []string{"", "key=a&prefix=b", "key=", "key=a&from=0", "key=a&from=x", "key=a&from=-3"} {
		if code, _, _ := request(t, h, "GET", "/watch?"+q, ""); code != http.StatusBadRequest {
			t.Errorf("/watch?%s: %d, want 400", q, code)
		}
	}
}

// The stream says where it starts, so a client that watched "from now" knows what to resume from.
func TestWatchSaysWhereItStarts(t *testing.T) {
	c := newKVCluster(t, 3, 0)
	l := c.leader(t)
	h := c.handler(l)
	request(t, h, "PUT", "/kv/a", "1")
	applied := c.servers[l].Store().LastApplied()
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)

	ch, _ := openWatch(t, ts.URL, "key=a", nil)
	if e := recvAny(t, ch); e.name != "ready" || e.data.Index != applied+1 {
		t.Fatalf("first event = %+v, want ready from %d", e, applied+1)
	}
	ch2, _ := openWatch(t, ts.URL, "key=a&from=1", nil)
	if e := recvAny(t, ch2); e.name != "ready" || e.data.Index != 1 {
		t.Fatalf("first event with from=1 = %+v, want ready from 1", e)
	}
}
