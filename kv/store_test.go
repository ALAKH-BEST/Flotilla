package kv

import (
	"testing"

	"github.com/ALAKH-BEST/flotilla/raft"
)

func apply(s *Store, index int, c Command) {
	s.Apply(raft.ApplyMsg{Index: index, Term: 1, Command: Encode(c)})
}

func TestPutGetDelete(t *testing.T) {
	s := NewStore()
	apply(s, 1, Command{Op: OpPut, Key: "a", Value: "1"})
	apply(s, 2, Command{Op: OpPut, Key: "a", Value: "2"})
	if v, ok := s.Get("a"); !ok || v != "2" {
		t.Fatalf("Get(a) = %q, %v; want 2, true", v, ok)
	}
	apply(s, 3, Command{Op: OpDelete, Key: "a"})
	if _, ok := s.Get("a"); ok {
		t.Fatal("a still present after delete")
	}
	if s.LastApplied() != 3 {
		t.Fatalf("LastApplied = %d; want 3", s.LastApplied())
	}
}

func TestNoOpAndBadCommandsAreSkipped(t *testing.T) {
	s := NewStore()
	s.Apply(raft.ApplyMsg{Index: 1, Term: 1, NoOp: true})
	s.Apply(raft.ApplyMsg{Index: 2, Term: 1, Command: []byte("not json")})
	if s.LastApplied() != 2 {
		t.Fatalf("LastApplied = %d; want 2", s.LastApplied())
	}
	if _, ok := s.Get(""); ok {
		t.Fatal("a skipped entry changed the data")
	}
}

func applyClient(s *Store, index int, c Command) Result {
	return s.Apply(raft.ApplyMsg{Index: index, Term: 1, Command: Encode(c)})
}

func TestGetReturnsValueAtItsPosition(t *testing.T) {
	s := NewStore()
	applyClient(s, 1, Command{Op: OpPut, Key: "a", Value: "1"})
	if r := applyClient(s, 2, Command{Op: OpGet, Key: "a"}); !r.Found || r.Value != "1" {
		t.Fatalf("get = %+v, want 1", r)
	}
	if r := applyClient(s, 3, Command{Op: OpGet, Key: "missing"}); r.Found {
		t.Fatalf("get of a missing key = %+v, want not found", r)
	}
}

func TestRetriedRequestsRunOnce(t *testing.T) {
	s := NewStore()
	applyClient(s, 1, Command{Op: OpPut, Key: "a", Value: "first", ClientID: 7, Seq: 1})
	applyClient(s, 2, Command{Op: OpPut, Key: "a", Value: "second", ClientID: 7, Seq: 2})
	// A delayed copy of the first request arrives after the second ran: it must not undo it.
	applyClient(s, 3, Command{Op: OpPut, Key: "a", Value: "first", ClientID: 7, Seq: 1})
	if v, _ := s.Get("a"); v != "second" {
		t.Fatalf("a = %q, want second", v)
	}
	// A retry of the latest get returns the first answer instead of reading again.
	r1 := applyClient(s, 4, Command{Op: OpGet, Key: "a", ClientID: 7, Seq: 3})
	applyClient(s, 5, Command{Op: OpPut, Key: "a", Value: "third", ClientID: 8, Seq: 1})
	r2 := applyClient(s, 6, Command{Op: OpGet, Key: "a", ClientID: 7, Seq: 3})
	if r1 != r2 || r1.Value != "second" {
		t.Fatalf("retried get = %+v then %+v, want the same answer (second)", r1, r2)
	}
}

func TestSnapshotKeepsSessions(t *testing.T) {
	s := NewStore()
	applyClient(s, 1, Command{Op: OpPut, Key: "a", Value: "1", ClientID: 7, Seq: 1})
	snap := s.Snapshot()

	restored := NewStore()
	restored.Apply(raft.ApplyMsg{SnapshotValid: true, Snapshot: snap, SnapshotIndex: 1, SnapshotTerm: 1})
	// The restored store still knows request 1 ran, so a retry does not run it again.
	applyClient(restored, 2, Command{Op: OpPut, Key: "a", Value: "2", ClientID: 9, Seq: 1})
	applyClient(restored, 3, Command{Op: OpPut, Key: "a", Value: "1", ClientID: 7, Seq: 1})
	if v, _ := restored.Get("a"); v != "2" {
		t.Fatalf("a = %q, want 2: the retried request ran again after the restore", v)
	}
}
