# Flotilla

A [Raft](https://raft.github.io/raft.pdf) consensus implementation in Go, with a replicated key-value store on top and a test harness that breaks the network on purpose.

Raft keeps a cluster of nodes agreeing on one ordered log even when nodes crash and messages are lost, delayed or cut off. Flotilla builds it from the paper and checks it under those failures.

## Status

Working: leader election, log replication, persistence across restarts, snapshots with log compaction, a key-value state machine, and a client API with exactly-once writes and linearizable reads, checked against recorded histories. It runs as real processes over TCP with an HTTP API and on-disk state, and servers can be added to and removed from a running cluster.

## Layout

```
raft/       the algorithm: election, replication, snapshots, persistence hooks, Node API
simnet/     in-memory network with drops, delays, isolation and partitions
kv/         key-value state machine: puts, gets, deletes, exactly-once client sessions, snapshots
server/     service on top of a Raft node, plus a client (Clerk) that finds the leader and retries
linearize/  checker that decides whether a recorded history of operations is linearizable
transport/tcp/   Raft RPCs over TCP (net/rpc with gob), with timeouts and reconnects
internal/runner/ starts one node: TCP transport, on-disk storage, HTTP API
cmd/flotilla/    the command-line server and client
```

```mermaid
flowchart LR
    C[client command] --> L[leader Node]
    L -- AppendEntries --> F1[follower Node]
    L -- AppendEntries --> F2[follower Node]
    L -.- S[(Storage)]
    F1 -.- S1[(Storage)]
    L --> A[committed entries] --> KV[kv.Store]
```

Networking and storage sit behind two small interfaces (`raft.Transport`, `raft.Storage`), so the same `Node` runs against the simulated network in tests and can run against a real one later.

## Use

```go
applyCh := make(chan raft.ApplyMsg, 256)
node, _ := raft.New(raft.Config{
    ID:        0,
    Peers:     []int{0, 1, 2},
    Transport: transport,                 // sends RPCs to the other nodes
    Storage:   raft.NewMemoryStorage(),
    ApplyCh:   applyCh,                   // committed entries arrive here, in order
})
node.Start()

index, term, isLeader := node.Propose([]byte("command"))

// The state machine decides when to compact: tell the node it has applied everything
// up to index and hand over a snapshot. The log before index is dropped.
err := node.Snapshot(index, stateMachine.Snapshot())
```

A snapshot reaches the state machine on `ApplyCh` as an `ApplyMsg` with `SnapshotValid` set, after a restart or when the node fell too far behind. It replaces the state machine's whole state.

### Client API

```go
clerk := server.NewClerk(1, servers) // a unique non-zero client ID
clerk.Put(ctx, "x", "1")
value, found, err := clerk.Get(ctx, "x")
```

Writes go through the replicated log, so a request that returns took effect at one point between its start and its end. A client retries with the same sequence number, and the store remembers each client's latest request, so a retry after a timeout or leader change does not run twice.

Reads use Raft's read index, so they write nothing to the log or the disk. The leader notes its commit index, confirms with one round of heartbeats that a majority still accepts it as leader, waits for its state machine to reach that index, then answers from local state. Right after an election, before the new leader has committed an entry of its own term, reads fall back to the log. `Config.LogReads` forces the old behaviour of sending reads through the log.

## Run a cluster

Build it, then start three nodes, each in its own terminal (`-cluster` lists `id=raftAddr/httpAddr` for every node):

```
go build -o flotilla ./cmd/flotilla
C=0=127.0.0.1:7000/127.0.0.1:8000,1=127.0.0.1:7001/127.0.0.1:8001,2=127.0.0.1:7002/127.0.0.1:8002
./flotilla server -id 0 -cluster $C -data data0
./flotilla server -id 1 -cluster $C -data data1
./flotilla server -id 2 -cluster $C -data data2
```

Talk to any node; followers redirect writes to the leader, and a list of servers lets the client fall back when one is down:

```
./flotilla put -server 127.0.0.1:8000,127.0.0.1:8001 city Paris
./flotilla get -server 127.0.0.1:8001 city
./flotilla status -server 127.0.0.1:8002
```

Stop the leader with Ctrl-C and the others elect a new one and keep serving. Start it again with the same `-data` directory and it rejoins and catches up. Without `-data`, state is kept in memory only.

### Change the cluster

Start the new node with `-join` (its own entry in `-cluster` gives its addresses), then add it. The leader first adds it as a non-voting learner and sends it the history, and promotes it to a voter once it has caught up, so a slow or broken new node never affects the cluster's majority:

```
./flotilla server -id 3 -cluster 3=127.0.0.1:7003/127.0.0.1:8003 -data data3 -join
./flotilla add -server 127.0.0.1:8000 3 127.0.0.1:7003/127.0.0.1:8003
./flotilla members -server 127.0.0.1:8000
./flotilla remove -server 127.0.0.1:8000 0
```

`members` marks learners. Changes go one server at a time and take effect once committed. Every node learns the new node's address from the log, so a node restarted from its data directory needs no address list. A removed node should be stopped.

The HTTP API is `GET|PUT|DELETE /kv/{key}`, `GET /status`, and for membership `GET /admin/members`, `PUT /admin/members/{id}` (the body is the node's address; it returns once the node is a voter) and `DELETE /admin/members/{id}`.

## Tests

```sh
go test -race ./...
```

The cluster tests run real nodes over `simnet` and check these cases:

- a single leader is elected and stays stable
- a failed leader is replaced, and the old one steps down when it returns
- no entry commits without a majority
- entries from a partitioned minority leader are discarded when the partition heals
- followers catch up after being cut off
- committed entries survive every node restarting
- progress under 15% message loss and random delay, with concurrent clients
- snapshots keep the log short while every replica ends with the same data
- a follower that fell behind the leader's compacted log catches up from the snapshot
- nodes restore their state machine from the snapshot after a restart
- snapshots together with message loss and delay
- the cluster grows from three to five nodes and shrinks again, and removed nodes still running do not disturb it
- a leader removes itself and the rest carry on
- a second change is refused while one is uncommitted, and the last member cannot be removed
- a membership entry that never committed is undone when its leader is replaced
- a leader cut off from a majority steps down, a healthy one does not, and a new leader gets time to hear back on a slow network
- a learner does not count towards a majority and never campaigns, and a learner can be promoted or removed
- adding a server waits for the new node to catch up before promoting it
- the membership survives log compaction, restarts, and a node that joins after the log was compacted past it
- with five nodes, two are not a majority

The server tests run five servers with clients hammering them while a nemesis isolates nodes, splits the cluster 2 / 3 and crashes and restarts nodes. Every operation is recorded and the checker in `linearize/` verifies the whole history is linearizable. Targeted tests check that a leader cut off from the majority refuses reads instead of answering from stale data, that reads add nothing to the log, that a new leader refuses a read index until it has committed in its term, that a read waits for the state machine to catch up, and that a duplicated request runs once. A second run of the same kind adds and removes servers at random (the leader included) among the isolations and crashes. I checked that the targeted tests fail when each guarantee is deliberately broken. The random runs are not what catches those breaks, since they inject one fault at a time and rarely build the situations that unsafe membership rules need.

Every applied entry is also compared across replicas, and the test fails if two nodes ever disagree at the same index.

## Design notes

- A new leader appends a no-op entry of its own term. Committing it commits earlier entries safely (paper section 5.4.2).
- A follower reports where its log diverges, so the leader backs up a term at a time.
- Followers truncate only at a real conflict, so delayed or duplicated messages cannot remove correct entries.
- If storage fails, the node panics instead of risking a forgotten vote or entry.
- A snapshot, its index and term, and the log after it are saved in one write, so they cannot disagree after a crash.
- A follower that is behind the leader's snapshot receives it in an `InstallSnapshot` RPC, keeps any entries after it that match, and passes it to the state machine before later entries. The snapshot travels in one message; very large ones would need chunking.

### Check-quorum

A leader records when each voter last answered it. If a majority of voters (itself included) has not answered within an election timeout, it steps down. Without this, a leader cut off from the cluster stays leader in name: it cannot commit or confirm a read, and every client that reaches it waits out a timeout. With it, such a node answers "not leader" and clients move on at once. A newly elected leader gets a full timeout to hear back first.

### Membership changes

- One server is added or removed at a time (paper section 6, single-server changes). Any majority of the old membership overlaps any majority of the new one, so two leaders cannot be elected in the same term across a change.
- The membership is an entry in the log, and a node uses the newest one it has as soon as it is appended, committed or not. If that entry is truncated, the node goes back to the previous membership. Snapshots and `InstallSnapshot` carry the membership, so compaction cannot lose it.
- A leader starts a change only after it has committed an entry of its own term, and only when no earlier change is uncommitted.
- A leader can remove itself: it stays leader, not counting itself, until the change commits, then steps down.
- A node ignores a vote request from a server that is not in its membership, without adopting its term. A removed server that never heard of its removal therefore cannot disrupt the cluster with higher terms. The leader stops replicating to a removed server as soon as the entry is appended, so that server may never learn it was removed; stop it.
- A new node starts with no members (`-join`), so it cannot elect itself leader of a cluster of one.
- A new node is added as a learner: it receives the log and snapshots but does not vote, campaign, or count towards commits or read confirmation. Only promoting it changes the voters, one server at a time. `AddServer` on the server waits until the learner trails the commit index by a few entries before promoting it; this only protects availability, not safety. If it fails part way the node stays a learner and `add` can be run again.

## Not done

- Chunked snapshot transfer, and removing a node that is unreachable when its removal is replicated.
- Pre-vote. A node that was cut off keeps raising its term while it campaigns, and forces an extra election when it comes back.
