# Flotilla

A [Raft](https://raft.github.io/raft.pdf) consensus implementation in Go, with a replicated key-value store on top and a test harness that breaks the network on purpose.

Raft keeps a cluster of nodes agreeing on one ordered log even when nodes crash and messages are lost, delayed or cut off. Flotilla builds it from the paper and checks it under those failures.

## Status

Working: leader election, log replication, persistence across restarts, snapshots with log compaction, and a key-value state machine. Not done yet: cluster membership changes, a real network transport and a linearizable client API (see the roadmap).

## Layout

```
raft/     the algorithm: election, replication, persistence hooks, Node API
simnet/   in-memory network with drops, delays, isolation and partitions
kv/       key-value state machine that applies committed entries and snapshots its data
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

Every applied entry is also compared across replicas, and the test fails if two nodes ever disagree at the same index.

## Design notes

- A new leader appends a no-op entry of its own term. Committing it commits earlier entries safely (paper section 5.4.2).
- A follower reports where its log diverges, so the leader backs up a term at a time.
- Followers truncate only at a real conflict, so delayed or duplicated messages cannot remove correct entries.
- If storage fails, the node panics instead of risking a forgotten vote or entry.
- A snapshot, its index and term, and the log after it are saved in one write, so they cannot disagree after a crash.
- A follower that is behind the leader's snapshot receives it in an `InstallSnapshot` RPC, keeps any entries after it that match, and passes it to the state machine before later entries. The snapshot travels in one message; very large ones would need chunking.

## Roadmap

- A linearizable client API (request deduplication, reads through the leader)
- Linearizability checking of recorded histories
- A TCP transport and a runnable server
- Cluster membership changes
