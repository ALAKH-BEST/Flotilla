package raft

import "fmt"

// Entry is one slot of the replicated log.
type Entry struct {
	Term    int
	Command []byte
	// NoOp marks the entry a new leader appends to commit entries from earlier terms.
	// It occupies a log index but is not a client command.
	NoOp bool
	// Members, when not empty, makes this a membership entry: the cluster's voting members
	// from this entry on. A node uses the newest such entry in its log as soon as it is
	// appended, committed or not.
	Members []Member
}

// Member is a voting member of the cluster. Addr is opaque to Raft; the transport and the
// application use it to find the node, and it travels with the membership.
type Member struct {
	ID   int
	Addr string
}

// ApplyMsg is delivered, in order, to the state machine. It is either a committed
// log entry or, when SnapshotValid is set, a snapshot that replaces the state
// machine's whole state (after a restart, or when this node fell too far behind).
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte
	NoOp    bool

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// On failure, the follower tells the leader where the logs diverge so it can
	// back up a whole term at a time instead of one entry per round trip.
	// ConflictTerm is 0 when the follower's log is simply too short.
	ConflictTerm  int
	ConflictIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// Data is the whole snapshot in one message. Large snapshots would need chunking.
	Data []byte
	// Members is the cluster membership as of LastIncludedIndex.
	Members []Member
}

type InstallSnapshotReply struct {
	Term int
}

// Role is the part of the protocol a node is currently playing.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", int(r))
	}
}

// Status is a point-in-time view of a node, for tests and tooling.
type Status struct {
	ID           int
	Term         int
	Role         Role
	LeaderID     int
	CommitIndex  int
	LastLogIndex int
	// SnapshotIndex is the last index covered by the node's snapshot (0 if none), and
	// LogEntries is how many entries it still holds in memory after that point.
	SnapshotIndex int
	LogEntries    int
	// Members are the IDs of the voting members this node currently uses.
	Members []int
}
