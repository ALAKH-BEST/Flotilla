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
