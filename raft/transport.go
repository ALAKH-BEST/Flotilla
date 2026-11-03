package raft

// Transport sends RPCs to other nodes. An error means the call did not complete
// (the request or the reply was lost, or the peer is unreachable); Raft handles
// that by trying again later.
type Transport interface {
	RequestVote(to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	InstallSnapshot(to int, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// Handler is the receiving side of the RPCs. *Node implements it.
type Handler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
	HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply
}
