# Design notes

How Flotilla works, and how it is tested. Section numbers refer to the [Raft paper](https://raft.github.io/raft.pdf).

## Layers

`raft.Node` is the algorithm. It talks to the outside through two small interfaces, `raft.Transport` (send RPCs) and `raft.Storage` (save and load state), so the same code runs against the in-memory network in tests (`simnet`) and against TCP and files in a real cluster. `server.Server` puts a key-value state machine (`kv`) on a node and adds the client API. `internal/runner` wires one node as a process.

## Elections

- A new leader appends a no-op entry of its own term. Committing it commits earlier entries safely (section 5.4.2).
- **Pre-vote.** Before a node raises its term and campaigns, it asks the voters whether they would vote for it, with a `PreVote` RPC that changes nothing on either side. A voter says no while it still hears from a leader, and also when the candidate's log is behind or its prospective term is not ahead. Only if a majority says yes does the node start a real election. A node that lost its own connection therefore cannot run up its term, and its return, or the return of a leader that stepped down, does not force a new election. A pre-vote round that a heartbeat or another vote overtook is dropped.
- **Check-quorum.** A leader records when each voter last answered it. If a majority of voters (itself included) has not answered within an election timeout, it steps down. Without this, a leader cut off from the cluster stays leader in name: it cannot commit or confirm a read, and every client that reaches it waits out a timeout. With it, such a node answers "not leader" and clients move on at once. A newly elected leader gets a full timeout to hear back first.

## Replication and storage

- A follower reports where its log diverges, so the leader backs up a term at a time instead of one entry.
- Followers truncate only at a real conflict, so delayed or duplicated messages cannot remove correct entries.
- If storage fails, the node panics instead of risking a forgotten vote or entry.
- `FileStorage` writes the whole state to a temporary file, syncs it, renames it over the old one and syncs the directory, so a crash leaves either the old state or the new one. Every save rewrites the whole state, which is acceptable because snapshots keep the log short.

## Snapshots

- The state machine decides when to compact. A snapshot, its index and term, the membership at that index, and the log after it are saved in one write, so they cannot disagree after a crash.
- A follower that is behind the leader's snapshot receives it in an `InstallSnapshot` RPC, keeps any entries after it that match, and passes it to the state machine before later entries. The snapshot travels in one message; very large ones would need chunking.

## Client API

- **Writes** go through the log. A client numbers its requests and retries with the same number; the store remembers each client's latest request and its result, so a retry after a timeout or leader change does not run twice.
- **Reads** use the read index (section 6.4). The leader notes its commit index, confirms with one round of heartbeats that a majority still accepts it as leader, waits for its state machine to reach that index, then answers from local state. That writes nothing to the log or the disk. Right after an election, before the new leader has committed an entry of its own term, its commit index may be behind, so reads fall back to the log until it has. `Config.LogReads` forces reads through the log.

## Membership changes

- One server is added or removed at a time (section 6, single-server changes). Any majority of the old voters overlaps any majority of the new ones, so two leaders cannot be elected in the same term across a change.
- The membership is an entry in the log, and a node uses the newest one it has as soon as it is appended, committed or not. If that entry is truncated, the node goes back to the previous membership. Snapshots and `InstallSnapshot` carry the membership, so compaction cannot lose it.
- A leader starts a change only after it has committed an entry of its own term, and only when no earlier change is uncommitted.
- A leader can remove itself: it stays leader, not counting itself, until the change commits, then steps down.
- A node ignores a vote request from a server that is not a voter in its membership, without adopting its term. A removed server that never heard of its removal therefore cannot disrupt the cluster with higher terms. The leader stops replicating to a removed server as soon as the entry is appended, so that server may never learn it was removed; stop it.
- A new node starts with no members (`-join`), so it cannot elect itself leader of a cluster of one.
- A new node is added as a **learner**: it receives the log and snapshots but does not vote, campaign, or count towards commits or read confirmation. Only promoting it changes the voters, one server at a time. `Server.AddServer` waits until the learner trails the commit index by a few entries before promoting it; this protects availability, not safety. If it fails part way the node stays a learner and `flotilla add` can be run again.
- Every node learns a new node's address from the log, so a node restarted from its data directory needs no address list.

## Testing

`go test -race ./...` runs everything. The tests run real nodes over `simnet`, which can drop, delay, isolate and partition, and check:

- a single leader is elected and stays stable; a failed leader is replaced, and the old one steps down when it returns
- no entry commits without a majority; entries from a partitioned minority leader are discarded when the partition heals
- followers catch up after being cut off; committed entries survive every node restarting
- progress under 15% message loss and random delay, with concurrent clients
- snapshots keep the log short while every replica ends with the same data, a follower behind the compacted log catches up from the snapshot, and nodes restore from it after a restart
- the cluster grows and shrinks, a leader removes itself, removed nodes still running do not disturb it, a second change is refused while one is uncommitted, and an uncommitted change is undone when its leader is replaced
- the membership survives compaction and restarts, including a node that joins after the log was compacted past its entry
- a learner does not count towards a majority and never campaigns; adding a server waits for it to catch up
- a cut-off follower does not raise its term and a returning node does not force an election; a leader cut off from a majority steps down and a healthy one does not
- reads add nothing to the log, a follower refuses them, a new leader refuses a read index until it has committed in its term, and a read waits for the state machine to catch up

Every applied entry is also compared across replicas, and the test fails if two nodes ever disagree at the same index.

**Linearizability.** The server tests run clients against five servers while a nemesis isolates nodes, splits the cluster and crashes and restarts nodes. Every operation is recorded with its start and end times, and the checker in `linearize/` (a search in the style of Wing and Gong, with memoization) verifies that the whole history is linearizable. A second run adds and removes servers at random, the leader included, among the same faults.

**Do the tests bite?** For each guarantee above I broke the code on purpose (skipping a check, counting the wrong set of nodes, dropping a step) and confirmed a test fails. Several tests were added or tightened because a mutation survived the first time. The random runs are not what catches most of these: they inject one fault at a time and rarely build the situations that unsafe rules need, so the targeted tests carry that load.

## Not done

- Chunked snapshot transfer.
- Removing a node that is unreachable when its removal is replicated: it keeps running until stopped by hand.
