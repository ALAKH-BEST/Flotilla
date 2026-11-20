package raft

import "testing"

// Two saves can be in flight at once: the syncer's, captured earlier, and a persist made under
// the lock, captured later. Whichever reaches storage second, the newer state must be what stays.
func TestAnOlderCaptureNeverOverwritesANewerOne(t *testing.T) {
	storage := NewMemoryStorage()
	n, err := New(Config{ID: 0, Peers: []int{0}, Transport: noPeers{}, Storage: storage, ApplyCh: make(chan ApplyMsg, 1)})
	if err != nil {
		t.Fatal(err)
	}
	older := State{CurrentTerm: 1, VotedFor: -1, Log: []Entry{{}, {Term: 1, Command: []byte("a")}}}
	newer := State{CurrentTerm: 1, VotedFor: -1, Log: []Entry{{}, {Term: 1, Command: []byte("a")}, {Term: 1, Command: []byte("b")}}}

	if err := n.save(2, newer); err != nil { // the later capture reaches storage first
		t.Fatal(err)
	}
	if err := n.save(1, older); err != nil { // the earlier one arrives late
		t.Fatal(err)
	}
	got, ok, err := storage.Load()
	if err != nil || !ok {
		t.Fatalf("load: %v %v", ok, err)
	}
	if len(got.Log) != 3 {
		t.Fatalf("storage holds a log of %d entries, want the newer state's 3: an older capture overwrote it", len(got.Log))
	}
}
