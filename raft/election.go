package raft

import "time"

// startElection begins a new term as a candidate (the lock must be held).
func (n *Node) startElection() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.leaderID = -1
	n.persist()
	n.resetElectionDeadline()

	term := n.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	voters := append([]Member(nil), n.members...) // the membership this election is held under
	granted := 1                                  // our own vote
	if granted*2 > len(voters) {
		n.becomeLeader()
		return
	}
	for _, m := range voters {
		peer := m.ID
		if peer == n.cfg.ID {
			continue
		}
		go func(peer int) {
			reply, err := n.cfg.Transport.RequestVote(peer, &args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped || n.currentTerm != term || n.role != Candidate {
				return // the election this vote belongs to is over
			}
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.resetElectionDeadline()
				return
			}
			if reply.VoteGranted {
				granted++
				if granted*2 > len(voters) {
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeLeader takes over after winning an election (the lock must be held).
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	// A leader cannot count replicas to commit entries from earlier terms (Figure 8
	// of the paper), so it appends an entry of its own term; committing that entry
	// commits everything before it.
	n.log = append(n.log, Entry{Term: n.currentTerm, NoOp: true})
	n.persist()
	last := n.lastLogIndex()
	for _, m := range n.members {
		n.nextIndex[m.ID] = last // the no-op is the first entry to send
		n.matchIndex[m.ID] = 0
		n.ackSeq[m.ID] = 0
	}
	n.matchIndex[n.cfg.ID] = last
	n.nextHeartbeat = time.Now().Add(n.cfg.HeartbeatInterval)
	n.broadcastAppendEntries()
	n.advanceCommit()
}

// HandleRequestVote handles a vote request from a candidate.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || args.Term < n.currentTerm {
		return &RequestVoteReply{Term: n.currentTerm}
	}
	// A server that is not in our membership cannot lead us. Ignoring it entirely, without even
	// adopting its term, stops a removed server that never heard of its removal from
	// disrupting the cluster with ever higher terms.
	if len(n.members) > 0 && !n.isMember(args.CandidateID) {
		return &RequestVoteReply{Term: n.currentTerm}
	}
	if args.Term > n.currentTerm {
		n.becomeFollower(args.Term)
	}
	// Grant the vote only if the candidate's log is at least as up to date as ours.
	upToDate := args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		n.persist()
		n.resetElectionDeadline()
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: true}
	}
	return &RequestVoteReply{Term: n.currentTerm}
}
