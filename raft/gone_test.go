package raft

import (
	"fmt"
	"testing"
)

type noPeers struct{ Transport }

// A snapshot taken after many removals remembers all of them by default. (The cluster tests above
// cover the behaviour end to end with a handful; this goes well past the 16 an earlier version kept.)
func TestGoneAfterKeepsFarMoreThanSixteenByDefault(t *testing.T) {
	n, err := New(Config{ID: 0, Peers: []int{0}, Transport: noPeers{}, Storage: NewMemoryStorage(), ApplyCh: make(chan ApplyMsg, 1)})
	if err != nil {
		t.Fatal(err)
	}
	const removals = 100
	var all []Member
	for i := 0; i <= removals; i++ {
		all = append(all, Member{ID: i, Addr: fmt.Sprintf("n%d:1", i)})
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotMembers = all
	n.log = []Entry{{}}
	for k := 1; k <= removals; k++ { // entry k drops server k-1 from the membership
		n.log = append(n.log, Entry{Term: 1, Members: all[k:]})
	}
	if got := n.goneAfter(removals); len(got) != removals {
		t.Fatalf("a snapshot after %d removals remembers %d, want all of them", removals, len(got))
	}

	n.cfg.MaxGone = 10
	got := n.goneAfter(removals)
	if len(got) != 10 || got[len(got)-1].ID != removals-1 {
		t.Fatalf("with MaxGone 10 a snapshot remembers %v, want the 10 most recent removals", got)
	}
}
