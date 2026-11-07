package raft

import (
	"errors"
	"sort"
	"time"
)

var (
	// ErrChangeInProgress means an earlier membership change has not committed yet. Changes
	// go one server at a time, which is what keeps any two majorities overlapping.
	ErrChangeInProgress = errors.New("raft: a membership change is already in progress")
	// ErrAlreadyMember and ErrNotMember report a change that would do nothing.
	ErrAlreadyMember = errors.New("raft: that server is already a member")
	ErrNotMember     = errors.New("raft: that server is not a member")
	// ErrNotLearner means the server is already a voter.
	ErrNotLearner = errors.New("raft: that server is not a learner")
	// ErrLastMember means the change would leave the cluster with no voters.
	ErrLastMember = errors.New("raft: cannot remove the last member")
)

func sortedMembers(ms []Member) []Member {
	out := append([]Member(nil), ms...)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func sameMembers(a, b []Member) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// membersAt returns the membership in effect at a log index: the newest membership entry at or
// before it, or the snapshot's membership if there is none (the lock must be held).
func (n *Node) membersAt(index int) []Member {
	for i := index; i > n.snapshotIndex; i-- {
		if m := n.log[i-n.snapshotIndex].Members; len(m) > 0 {
			return m
		}
	}
	return n.snapshotMembers
}

// refreshMembers recomputes the membership from the log. It must run whenever the log's tail
// changes, because a node uses the newest membership entry as soon as it has it, and a
// truncated entry stops counting (the lock must be held).
func (n *Node) refreshMembers() {
	next := n.membersAt(n.lastLogIndex())
	if sameMembers(next, n.members) {
		return
	}
	old := n.members
	n.members = next
	had := make(map[int]bool, len(old))
	for _, m := range old {
		had[m.ID] = true
	}
	for _, m := range next {
		if !had[m.ID] { // a server joining: start replicating to it from the end of the log
			n.nextIndex[m.ID] = n.lastLogIndex() + 1
			n.matchIndex[m.ID] = 0
			n.ackSeq[m.ID] = 0
		}
	}
	if n.cfg.OnMembership != nil {
		n.cfg.OnMembership(append([]Member(nil), next...))
	}
}

func (n *Node) isMember(id int) bool {
	for _, m := range n.members {
		if m.ID == id {
			return true
		}
	}
	return false
}

func (n *Node) isVoter(id int) bool {
	for _, m := range n.members {
		if m.ID == id && !m.Learner {
			return true
		}
	}
	return false
}

func (n *Node) voters() []Member {
	var out []Member
	for _, m := range n.members {
		if !m.Learner {
			out = append(out, m)
		}
	}
	return out
}

func (n *Node) voterIDs() []int {
	ids := []int{}
	for _, m := range n.voters() {
		ids = append(ids, m.ID)
	}
	return ids
}

func (n *Node) learnerIDs() []int {
	ids := []int{}
	for _, m := range n.members {
		if m.Learner {
			ids = append(ids, m.ID)
		}
	}
	return ids
}

// isMajority reports whether count nodes are a majority of the voters.
func (n *Node) isMajority(count int) bool { return count*2 > len(n.voters()) }

// Members returns the voting members this node currently uses.
func (n *Node) Members() []Member {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]Member(nil), n.members...)
}

// AddLearner starts adding a server as a non-voting member. It returns the index and term of the
// membership entry, which takes effect on this leader at once and is done when that index
// commits. The voters do not change, so this cannot affect availability. Start the new node
// (with Config.Join) first, wait until CatchUp reports it has the log, then PromoteLearner.
func (n *Node) AddLearner(m Member) (index, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.canChange(); err != nil {
		return 0, 0, err
	}
	if n.isMember(m.ID) {
		return 0, 0, ErrAlreadyMember
	}
	m.Learner = true
	return n.proposeMembers(sortedMembers(append(append([]Member(nil), n.members...), m)))
}

// PromoteLearner makes a learner a voter, with the same guarantees as AddLearner. This is the
// one-server change to the voters.
func (n *Node) PromoteLearner(id int) (index, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.canChange(); err != nil {
		return 0, 0, err
	}
	if !n.isMember(id) {
		return 0, 0, ErrNotMember
	}
	if n.isVoter(id) {
		return 0, 0, ErrNotLearner
	}
	next := append([]Member(nil), n.members...)
	for i := range next {
		if next[i].ID == id {
			next[i].Learner = false
		}
	}
	return n.proposeMembers(next)
}

// RemoveServer starts removing a voter or a learner, with the same guarantees as AddLearner.
// A leader can remove itself: it keeps leading until the change commits, then steps down.
func (n *Node) RemoveServer(id int) (index, term int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if err := n.canChange(); err != nil {
		return 0, 0, err
	}
	if !n.isMember(id) {
		return 0, 0, ErrNotMember
	}
	if n.isVoter(id) && len(n.voters()) == 1 {
		return 0, 0, ErrLastMember
	}
	var rest []Member
	for _, m := range n.members {
		if m.ID != id {
			rest = append(rest, m)
		}
	}
	return n.proposeMembers(rest)
}

// CatchUp reports how far a member's log trails this leader's commit index, and the index it has
// replicated up to. It is how a caller decides a learner is ready to be promoted.
func (n *Node) CatchUp(id int) (match, commit int, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return 0, 0, ErrStopped
	}
	if n.role != Leader {
		return 0, 0, ErrNotLeader
	}
	if !n.isMember(id) {
		return 0, 0, ErrNotMember
	}
	return n.matchIndex[id], n.commitIndex, nil
}

// canChange checks the conditions for starting a membership change (the lock must be held).
func (n *Node) canChange() error {
	if n.stopped {
		return ErrStopped
	}
	if n.role != Leader {
		return ErrNotLeader
	}
	// Until the leader commits an entry of its own term it cannot tell which earlier entries,
	// including membership entries, are committed.
	if n.termAt(n.commitIndex) != n.currentTerm {
		return ErrNotReady
	}
	for i := n.commitIndex + 1; i <= n.lastLogIndex(); i++ {
		if len(n.log[i-n.snapshotIndex].Members) > 0 {
			return ErrChangeInProgress
		}
	}
	return nil
}

func (n *Node) proposeMembers(members []Member) (index, term int, err error) {
	n.log = append(n.log, Entry{Term: n.currentTerm, Members: members})
	n.persist()
	index = n.lastLogIndex()
	n.matchIndex[n.cfg.ID] = index
	old := n.members
	n.refreshMembers() // the new membership applies from the moment it is in the log
	for _, m := range old {
		if !hasMember(members, m.ID) && m.ID != n.cfg.ID {
			n.departing[m.ID] = &departure{index: index}
		}
	}
	for _, m := range members {
		delete(n.departing, m.ID)
	}
	n.broadcastAppendEntries()
	n.advanceCommit()
	return index, n.currentTerm, nil
}

// departure tracks a removed server the leader is still trying to tell about its removal.
type departure struct {
	index    int       // the log index of the entry that removed it
	nextTry  time.Time // departing servers are tried far less often than members
	inflight bool
}

// departRetry is how many heartbeat intervals pass between attempts to reach a removed server.
const departRetry = 10

func hasMember(ms []Member, id int) bool {
	for _, m := range ms {
		if m.ID == id {
			return true
		}
	}
	return false
}

// noteRemoval records that this node has been removed if the committed membership went from
// including it to excluding it. Only committed membership counts: an appended entry can still
// be undone (the lock must be held).
func (n *Node) noteRemoval(before, after []Member) {
	if n.removed || !hasMember(before, n.cfg.ID) || hasMember(after, n.cfg.ID) {
		return
	}
	n.removed = true
	if n.cfg.OnRemoved != nil {
		go n.cfg.OnRemoved()
	}
}

// noteCommit runs after the commit index moved up from old.
func (n *Node) noteCommit(old int) {
	if n.commitIndex > old {
		n.noteRemoval(n.membersAt(old), n.membersAt(n.commitIndex))
	}
}

// rebuildDeparting works out, from the log, which removed servers a new leader should still tell:
// every server that a membership entry in the log removed and a later one did not bring back.
// Removals compacted into a snapshot are not remembered.
func (n *Node) rebuildDeparting() {
	n.departing = make(map[int]*departure)
	prev := n.snapshotMembers
	for i := n.snapshotIndex + 1; i <= n.lastLogIndex(); i++ {
		cur := n.log[i-n.snapshotIndex].Members
		if len(cur) == 0 {
			continue
		}
		for _, m := range prev {
			if !hasMember(cur, m.ID) {
				n.departing[m.ID] = &departure{index: i}
				n.nextIndex[m.ID] = n.lastLogIndex() + 1
				n.matchIndex[m.ID] = 0
			}
		}
		for _, m := range cur {
			delete(n.departing, m.ID)
		}
		prev = cur
	}
}

// notifyDeparting offers the log to removed servers that have not confirmed their removal yet.
func (n *Node) notifyDeparting() {
	now := time.Now()
	for id, d := range n.departing {
		if d.inflight || now.Before(d.nextTry) {
			continue
		}
		d.inflight = true
		d.nextTry = now.Add(departRetry * n.cfg.HeartbeatInterval)
		go func(id int) {
			n.replicateTo(id)
			n.mu.Lock()
			if d := n.departing[id]; d != nil {
				d.inflight = false
			}
			n.mu.Unlock()
		}(id)
	}
}
