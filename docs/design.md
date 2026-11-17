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
- `FileStorage` keeps the state in one append-only file of checksummed records. The first record is a checkpoint, the whole state. Each later record is a delta: the new term and vote, how many log entries to keep, and the entries to add after them. A save that only appends writes just the new entries and syncs once, however long the log is; a save that changes nothing writes nothing. A snapshot, or anything that cannot be written as a delta, replaces the file with a fresh checkpoint (written to a temporary file, synced, renamed over the old one, directory synced), which is also what keeps the file from growing without bound.
- A crash while a delta is being appended leaves a torn record at the end of the file. Loading ignores it and removes it, so later appends follow valid data. A bad record with more data after it is damage, not a torn write, and loading fails rather than silently forgetting acknowledged writes. A directory in the earlier single-file format is read as the starting checkpoint and replaced on the first save.

## Snapshots

- The state machine decides when to compact. A snapshot, its index and term, the membership at that index, and the log after it are saved in one write, so they cannot disagree after a crash.
- A follower that is behind the leader's snapshot receives it in `InstallSnapshot` RPCs, keeps any entries after it that match, and passes it to the state machine before later entries.
- The snapshot is sent in chunks (`SnapshotChunkSize`, 64 KiB by default). The follower buffers them and uses nothing until the last one arrives. Each reply says how many bytes the follower holds, so a lost, duplicated or out-of-order chunk is repaired by resuming from that offset, and an interrupted transfer continues instead of starting over. A transfer belongs to one leader, term and snapshot; chunks of anything else restart it. The leader runs one transfer per follower at a time, so heartbeats do not start competing ones.

## Client API

- **Writes** go through the log. A client numbers its requests and retries with the same number; the store remembers each client's latest request and its result, so a retry after a timeout or leader change does not run twice.
- **Reads** use the read index (section 6.4). The leader notes its commit index, confirms with one round of heartbeats that a majority still accepts it as leader, waits for its state machine to reach that index, then answers from local state. That writes nothing to the log or the disk. Right after an election, before the new leader has committed an entry of its own term, its commit index may be behind, so reads fall back to the log until it has. `Config.LogReads` forces reads through the log.

## Membership changes

- One server is added or removed at a time (section 6, single-server changes). Any majority of the old voters overlaps any majority of the new ones, so two leaders cannot be elected in the same term across a change.
- The membership is an entry in the log, and a node uses the newest one it has as soon as it is appended, committed or not. If that entry is truncated, the node goes back to the previous membership. Snapshots and `InstallSnapshot` carry the membership, so compaction cannot lose it.
- A leader starts a change only after it has committed an entry of its own term, and only when no earlier change is uncommitted.
- A leader can remove itself: it stays leader, not counting itself, until the change commits, then steps down.
- A node ignores a vote request from a server that is not a voter in its membership, without adopting its term. A removed server that never heard of its removal therefore cannot disrupt the cluster with higher terms. 
- A removed server finds out. The leader keeps offering the log to each server it removed, now and then (every ten heartbeats), until one confirms it holds the removal and knows it is committed. That works for a server that was cut off at the time and comes back later, including after the others compacted past the removal, because the snapshot carries the membership. A node counts itself removed only when a *committed* membership change drops it from the committed membership, never because of an entry that could still be undone. It then calls `Config.OnRemoved`, and the server process stops itself a second later. A new leader works out whom to keep telling from the membership entries still in its log and from the snapshot, which records the most recent 16 removed servers with their addresses (`SnapshotGone`; it travels with `InstallSnapshot` and is saved with the snapshot). A server added back is dropped from that list. The leader hands the addresses to `Config.OnDeparted`, because a removed server is in no membership and the transport may never have heard of it.
- A new node starts with no members (`-join`), so it cannot elect itself leader of a cluster of one.
- A new node is added as a **learner**: it receives the log and snapshots but does not vote, campaign, or count towards commits or read confirmation. Only promoting it changes the voters, one server at a time. `Server.AddServer` waits until the learner trails the commit index by a few entries before promoting it; this protects availability, not safety. If it fails part way the node stays a learner and `flotilla add` can be run again.
- Every node learns a new node's address from the log, so a node restarted from its data directory needs no address list.

## Leadership transfer

`Node.TransferLeadership` (section 3.10 of Ongaro's thesis) moves leadership to a chosen voter without waiting for an election timeout, so a leader can be drained before it is stopped.

- The leader stops accepting new commands (`Propose` reports not leader, so clients move on), keeps replicating, and waits until the target's log matches its own. A target that is behind therefore never loses the election for that reason.
- It then sends `TimeoutNow`. The target starts a real election at once, skipping pre-vote, since the leader has chosen the moment and pre-vote would refuse a node that still hears a leader. It acts only on a `TimeoutNow` from the leader of its current term, so a stale or forged message cannot start an election. The message is resent every minimum election timeout in case it was lost.
- When the target wins, its higher term makes the old leader step down, which ends the transfer. If nothing happens within `Config.TransferTimeout` (default 2 seconds), or the caller's context ends, the transfer is cancelled and the node carries on as leader.
- Reads are still served during a transfer: the leader is still the leader until a newer term appears.
- Only another voting member can be a target (not a learner, not the leader itself), and one transfer runs at a time.

## Testing

`go test -race ./...` runs everything. The tests run real nodes over `simnet`, which can drop, delay, isolate and partition, and check:

- a single leader is elected and stays stable; a failed leader is replaced, and the old one steps down when it returns
- no entry commits without a majority; entries from a partitioned minority leader are discarded when the partition heals
- followers catch up after being cut off; committed entries survive every node restarting
- progress under 15% message loss and random delay, with concurrent clients
- snapshots keep the log short while every replica ends with the same data, a follower behind the compacted log catches up from the snapshot, and nodes restore from it after a restart
- the cluster grows and shrinks, a leader removes itself, removed nodes still running do not disturb it, a second change is refused while one is uncommitted, and an uncommitted change is undone when its leader is replaced
- a snapshot is split into chunks and still arrives whole over a lossy, delayed network; the receiver handles duplicated, missing, stale and mixed chunks, and installs nothing before the last one
- a removed server learns of it whether it was cut off, reachable, the leader, behind a compacted log, or the leader changed meanwhile; an uncommitted removal and a server being added are never mistaken for one; the leader stops contacting a server that knows
- the membership survives compaction and restarts, including a node that joins after the log was compacted past its entry
- a learner does not count towards a majority and never campaigns; adding a server waits for it to catch up
- a cut-off follower does not raise its term and a returning node does not force an election; a leader cut off from a majority steps down and a healthy one does not
- a leadership transfer makes exactly the chosen node leader without waiting out a timeout, catches a lagging target up first, is cancelled when the target never takes over (the leader refuses commands meanwhile and then resumes), refuses a follower, a stranger and a learner, and a timeout-now from anyone but the current leader is ignored
- the file storage appends instead of rewriting, round-trips a random history of appends, tail rewrites, term changes and snapshots, recovers the right state when the file is cut at every possible byte, reports damage in the middle of the file, and reads the old single-file format
- reads add nothing to the log, a follower refuses them, a new leader refuses a read index until it has committed in its term, and a read waits for the state machine to catch up

Every applied entry is also compared across replicas, and the test fails if two nodes ever disagree at the same index.

**Linearizability.** The server tests run clients against five servers while a nemesis isolates nodes, splits the cluster and crashes and restarts nodes. Every operation is recorded with its start and end times, and the checker in `linearize/` (a search in the style of Wing and Gong, with memoization) verifies that the whole history is linearizable. A second run adds and removes servers at random, the leader included, among the same faults.

**Real processes.** The `chaos/` test starts five `flotilla` processes with real TCP and disk. Each node reaches its peers through a small TCP proxy, so the test can cut any link between two live processes. A nemesis then kills nodes with SIGKILL and restarts them from disk, hands leadership to another node while clients write, freezes and unfreezes them with SIGSTOP and SIGCONT, splits the network, and cuts off whoever leads at that moment. Five clients send writes and reads over HTTP, retrying with the same `X-Seq` so each runs once. One more reader per node asks that node alone and never follows a redirect, so a node that wrongly thinks it still leads has nothing to hide behind. After everything is restored the test reads every key once more, waits for every node to report the same commit index, and checks the whole history with `linearize/`.

```sh
FLOTILLA_CHAOS=1 go test ./chaos -run TestChaos -v
```

It is skipped without `FLOTILLA_CHAOS`, since it takes about 30 seconds. `FLOTILLA_CHAOS_SECONDS` sets the length and `FLOTILLA_CHAOS_SEED` repeats a fault schedule (the schedule repeats; the timing of the real processes does not). On a failure the node logs are copied to a temporary directory whose path is printed.

**Do the tests bite?** For each guarantee above I broke the code on purpose (skipping a check, counting the wrong set of nodes, dropping a step) and confirmed a test fails. Several tests were added or tightened because a mutation survived the first time. The random runs are not what catches most of these: they inject one fault at a time and rarely build the situations that unsafe rules need, so the targeted tests carry that load. The real-process test is the same: with reads made to skip the check that a majority still follows the leader, it fails on some seeds and not others (3 of 5 in my runs), and only after it was changed to isolate the current leader and to read from each node directly. Run several seeds before trusting a pass.

## Not done

- Only the 16 most recent removals are remembered across compactions, so a server that stays away through more removals than that is never told (harmless, since it cannot campaign, but stop it by hand). The leader also keeps trying such a server every ten heartbeats for as long as it is listed.
