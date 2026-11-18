package kv

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
)

func expiredKeys(s *Store, now int64) []string {
	var out []string
	for _, e := range s.Expired(now, 100) {
		out = append(out, e.Key)
	}
	return out
}

// A key with a time to live is listed as expired once its time has passed, and is removed only
// when the OpExpire command for exactly that expiry is applied.
func TestExpireRemovesTheKeyOnlyWhenTheCommandIsApplied(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1", ExpireAt: 1000})
	apply(s, 2, Command{Op: OpPut, Key: "b", Value: "2"})
	if got := expiredKeys(s, 999); got != nil {
		t.Fatalf("expired before its time: %v", got)
	}
	if got := expiredKeys(s, 1000); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("expired at its time: %v, want [a]", got)
	}
	if v, ok := s.Get("a"); !ok || v != "1" {
		t.Fatal("the key vanished without a committed expire command: a read must not depend on a clock")
	}
	apply(s, 3, Command{Op: OpExpire, Key: "a", ExpireAt: 1000})
	if _, ok := s.Get("a"); ok {
		t.Fatal("the key survived its expire command")
	}
	if got := expiredKeys(s, 1<<60); got != nil {
		t.Fatalf("still listed as expired after removal: %v", got)
	}
	if v, _ := s.Get("b"); v != "2" {
		t.Fatal("a key with no time to live was touched")
	}
}

// An expire command that was proposed for an old expiry must not remove a key that was written again.
func TestStaleExpireLeavesARewrittenKey(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1", ExpireAt: 1000})
	apply(s, 2, Command{Op: OpPut, Key: "a", Value: "2", ExpireAt: 5000}) // renewed
	apply(s, 3, Command{Op: OpExpire, Key: "a", ExpireAt: 1000})          // the leader's earlier proposal
	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Fatalf("a = %q, %v; the renewed key was removed by a stale expire", v, ok)
	}
	apply(s, 4, Command{Op: OpPut, Key: "a", Value: "3"}) // written again with no time to live
	apply(s, 5, Command{Op: OpExpire, Key: "a", ExpireAt: 5000})
	if v, ok := s.Get("a"); !ok || v != "3" {
		t.Fatalf("a = %q, %v; a key rewritten without a ttl was removed", v, ok)
	}
	if got := expiredKeys(s, 1<<60); got != nil {
		t.Fatalf("a key with no ttl is listed as expiring: %v", got)
	}
}

func TestDeleteClearsTheExpiryAndGetReportsIt(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1", ExpireAt: 1000})
	if r := s.GetEntry("a"); !r.Found || r.ExpireAt != 1000 {
		t.Fatalf("GetEntry = %+v, want found with expiry 1000", r)
	}
	if r := applyClient(s, 2, Command{Op: OpGet, Key: "a"}); r.ExpireAt != 1000 {
		t.Fatalf("a get through the log reports expiry %d, want 1000", r.ExpireAt)
	}
	apply(s, 3, Command{Op: OpDelete, Key: "a"})
	if got := expiredKeys(s, 1<<60); got != nil {
		t.Fatalf("a deleted key is still listed as expiring: %v", got)
	}
}

func TestExpiriesSurviveASnapshotAndOldSnapshotsStillLoad(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1", ExpireAt: 1000})
	apply(s, 2, Command{Op: OpPut, Key: "b", Value: "2"})
	r := NewStore()
	r.Apply(raft.ApplyMsg{SnapshotValid: true, SnapshotIndex: 2, Snapshot: s.Snapshot()})
	if got := expiredKeys(r, 1000); !reflect.DeepEqual(got, []string{"a"}) {
		t.Fatalf("after restoring: %v, want [a] expired", got)
	}

	old := NewStore() // a snapshot from before time to live existed has no "expires"
	old.Apply(raft.ApplyMsg{SnapshotValid: true, SnapshotIndex: 5, Snapshot: []byte(`{"data":{"x":"1"},"sessions":{}}`)})
	if v, ok := old.Get("x"); !ok || v != "1" {
		t.Fatal("an old-format snapshot did not load")
	}
	apply(old, 6, Command{Op: OpPut, Key: "y", Value: "2", ExpireAt: 7}) // and the map is usable
}

func TestExpiredIsOrderedAndLimited(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "c", Value: "x", ExpireAt: 30})
	apply(s, 2, Command{Op: OpPut, Key: "a", Value: "x", ExpireAt: 10})
	apply(s, 3, Command{Op: OpPut, Key: "b", Value: "x", ExpireAt: 20})
	got := s.Expired(100, 2)
	if len(got) != 2 || got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("Expired = %v, want the two oldest, a then b", got)
	}
}

func next(t *testing.T, w *Watcher) []Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ev, err := w.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return ev
}

func types(ev []Event) []string {
	var out []string
	for _, e := range ev {
		out = append(out, e.Type+":"+e.Key)
	}
	return out
}

// Changes come out in log order with the index of the command that made them. A delete of a key
// that is not there changes nothing, and a retried request does not happen twice.
func TestEventsFollowTheLog(t *testing.T) {
	s := NewStore()
	w, err := s.Watch(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1", ExpireAt: 500})
	apply(s, 2, Command{Op: OpDelete, Key: "zzz"}) // nothing there
	s.Apply(raft.ApplyMsg{Index: 3, Term: 1, NoOp: true})
	applyClient(s, 4, Command{Op: OpPut, Key: "b", Value: "2", ClientID: 9, Seq: 1})
	applyClient(s, 5, Command{Op: OpPut, Key: "b", Value: "2", ClientID: 9, Seq: 1}) // a retry
	apply(s, 6, Command{Op: OpExpire, Key: "a", ExpireAt: 500})
	apply(s, 7, Command{Op: OpDelete, Key: "b"})

	var got []Event
	for len(got) < 4 {
		got = append(got, next(t, w)...)
	}
	want := []Event{
		{Index: 1, Type: "put", Key: "a", Value: "1", ExpireAt: 500},
		{Index: 4, Type: "put", Key: "b", Value: "2"},
		{Index: 6, Type: "expire", Key: "a"},
		{Index: 7, Type: "delete", Key: "b"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events:\n got %+v\nwant %+v", got, want)
	}
}

func TestWatchFromNowSeesOnlyNewChangesAndCanFilter(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "old", Value: "x"})
	w, err := s.Watch(0, func(k string) bool { return len(k) >= 3 && k[:3] == "cfg" })
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	apply(s, 2, Command{Op: OpPut, Key: "other", Value: "x"})
	apply(s, 3, Command{Op: OpPut, Key: "cfg/a", Value: "1"})
	if got := types(next(t, w)); !reflect.DeepEqual(got, []string{"put:cfg/a"}) {
		t.Fatalf("events: %v, want only put:cfg/a", got)
	}
}

func TestNextWaitsForAChangeAndStopsWithItsContext(t *testing.T) {
	s := NewStore()
	w, _ := s.Watch(0, nil)
	defer w.Close()
	done := make(chan []Event, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ev, _ := w.Next(ctx)
		done <- ev
	}()
	select {
	case <-done:
		t.Fatal("Next returned with nothing to report")
	case <-time.After(100 * time.Millisecond):
	}
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1"})
	select {
	case ev := <-done:
		if len(ev) != 1 || ev[0].Key != "a" {
			t.Fatalf("Next = %v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Next did not wake up for a change")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := w.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Next with nothing new = %v, want the context's deadline error", err)
	}
}

// Only so many recent events are kept. Asking for older ones is refused with the index to restart
// from, and a watcher that falls behind is told the same instead of silently skipping changes.
func TestWatchRefusesEventsThatWereDropped(t *testing.T) {
	s := NewStore()
	s.SetMaxEvents(3)
	slow, err := s.Watch(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	for i := 1; i <= 10; i++ {
		apply(s, i, Command{Op: OpPut, Key: "k", Value: "v"})
	}
	var ce *CompactedError
	if _, err := s.Watch(5, nil); !errors.As(err, &ce) || ce.Oldest != 8 {
		t.Fatalf("Watch(5) = %v, want a CompactedError with Oldest 8", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := slow.Next(ctx); !errors.As(err, &ce) || ce.Oldest != 8 {
		t.Fatalf("a watcher that fell behind got %v, want a CompactedError with Oldest 8", err)
	}
	w, err := s.Watch(8, nil) // exactly the oldest kept
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if ev := next(t, w); len(ev) != 3 || ev[0].Index != 8 {
		t.Fatalf("events from the oldest kept: %+v", ev)
	}
}

// Restoring a snapshot jumps the state forward without events, so everything before it is gone.
func TestSnapshotClearsTheHistory(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1"})
	w, _ := s.Watch(1, nil)
	defer w.Close()
	next(t, w)
	other := NewStore()
	apply(other, 1, Command{Op: OpPut, Key: "b", Value: "2"})
	s.Apply(raft.ApplyMsg{SnapshotValid: true, SnapshotIndex: 50, Snapshot: other.Snapshot()})

	var ce *CompactedError
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := w.Next(ctx); !errors.As(err, &ce) || ce.Oldest != 51 {
		t.Fatalf("a watcher across a snapshot got %v, want a CompactedError with Oldest 51", err)
	}
	if _, err := s.Watch(10, nil); !errors.As(err, &ce) {
		t.Fatalf("Watch(10) = %v, want a CompactedError", err)
	}
	if w2, err := s.Watch(51, nil); err != nil {
		t.Fatalf("Watch(51) = %v", err)
	} else {
		w2.Close()
	}
}

// Two replicas that apply the same log produce the same events, which is what lets a watcher
// resume from an index on a different node.
func TestReplicasProduceTheSameEvents(t *testing.T) {
	cmds := []Command{
		{Op: OpPut, Key: "a", Value: "1", ExpireAt: 9},
		{Op: OpPut, Key: "b", Value: "2"},
		{Op: OpExpire, Key: "a", ExpireAt: 9},
		{Op: OpDelete, Key: "b"},
	}
	var all [2][]Event
	for r := range all {
		s := NewStore()
		w, _ := s.Watch(1, nil)
		for i, c := range cmds {
			apply(s, i+1, c)
		}
		for len(all[r]) < 4 {
			all[r] = append(all[r], next(t, w)...)
		}
		w.Close()
	}
	if !reflect.DeepEqual(all[0], all[1]) {
		t.Fatalf("replicas differ:\n%+v\n%+v", all[0], all[1])
	}
}
