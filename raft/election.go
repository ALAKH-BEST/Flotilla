package raft

import "time"

// startPreVote asks a majority of the voters whether they would vote for this node, without
// changing the term or any vote, and starts a real election only if they would. A node that was
// cut off from a healthy cluster is turned down, so it does not run up its term and force
// an election when it comes back (the lock must be held).
func (n *Node) startPreVote() {
	n.resetElectionDeadline() // if no majority says yes, try again after a fresh timeout
	epoch := n.epoch
	args := PreVoteArgs{
		Term:         n.currentTerm + 1,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastLogIndex(),
		LastLogTerm:  n.lastLogTerm(),
	}
	voters := n.voters()
	granted := 1 // ourselves
	if granted*2 > len(voters) {
		n.startElection()
		return
	}
	for _, m := range voters {
		peer := m.ID
		if peer == n.cfg.ID {
			continue
		}
		go func(peer int) {
			reply, err := n.cfg.Transport.PreVote(peer, &args)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			// Anything that reset the election timer since (a leader's heartbeat, a vote we
			// granted, a real election) makes this round stale.
			if n.stopped || n.epoch != epoch || n.role == Leader {
				return
			}
			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				n.resetElectionDeadline()
				return
			}
			if reply.VoteGranted {
				granted++
				if granted*2 > len(voters) {
					n.startElection()
				}
			}
		}(peer)
	}
}

// HandlePreVote answers whether this node would vote for the candidate. It changes nothing.
// It says no while it is still hearing from a leader, which is what keeps a node that merely lost
// its own connection from unseating a leader everyone else can reach.
func (n *Node) HandlePreVote(args *PreVoteArgs) *PreVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()
	no := &PreVoteReply{Term: n.currentTerm}
	if n.stopped || args.Term <= n.currentTerm {
		return no
	}
	if len(n.members) > 0 && !n.isVoter(args.CandidateID) {
		return no
	}
	if n.role == Leader || time.Since(n.lastLeaderContact) < n.cfg.ElectionTimeoutMin {
		return no
	}
	upToDate := args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())
	if !upToDate {
		return no
	}
	return &PreVoteReply{Term: n.currentTerm, VoteGranted: true}
}

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
	voters := n.voters() // the voters this election is held under
	granted := 1         // our own vote
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
		n.lastContact[m.ID] = time.Now() // a new leader gets a full timeout to hear from a majority
	}
	n.matchIndex[n.cfg.ID] = last
	n.snapOffset = make(map[int]snapProgress)
	n.rebuildDeparting()
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
	// A server that is not a voter in our membership cannot lead us. Ignoring it entirely, without even
	// adopting its term, stops a removed server that never heard of its removal from
	// disrupting the cluster with ever higher terms.
	if len(n.members) > 0 && !n.isVoter(args.CandidateID) {
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
