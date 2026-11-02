package raft

import "fmt"

// Entry is one slot of the replicated log.
type Entry struct {
	Term    int
	Command []byte
	// NoOp marks the entry a new leader appends to commit entries from earlier terms.
	// It occupies a log index but is not a client command.
	NoOp bool
}

// ApplyMsg is delivered, in log order, once an entry is committed.
type ApplyMsg struct {
	Index   int
	Term    int
	Command []byte
	NoOp    bool
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
}
