package raft_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ALAKH-BEST/flotilla/raft"
	"github.com/ALAKH-BEST/flotilla/simnet"
)

// slowStorage stands in for a disk whose sync takes a fixed time, which is what decides write
// throughput on many real machines (a cloud disk syncs in 1 to 5 ms; a local SSD in well under one).
type slowStorage struct {
	raft.Storage
	sync time.Duration
}

func (s slowStorage) Save(state raft.State) error {
	time.Sleep(s.sync)
	return s.Storage.Save(state)
}

// BenchmarkProposeSlowDisk measures how fast a three-node cluster takes commands when every node's
// disk sync takes a given time, with several clients proposing at once.
//
//	go test ./raft -run xxx -bench ProposeSlowDisk -benchtime 2000x
func BenchmarkProposeSlowDisk(b *testing.B) {
	for _, syncTime := range []time.Duration{time.Millisecond, 4 * time.Millisecond} {
		for _, clients := range []int{1, 8, 32} {
			b.Run(fmt.Sprintf("sync=%s/clients=%d", syncTime, clients), func(b *testing.B) {
				net := simnet.New(3)
				var nodes []*raft.Node
				for i := 0; i < 3; i++ {
					node, err := raft.New(raft.Config{
						ID: i, Peers: []int{0, 1, 2}, Transport: net.Transport(i),
						Storage: slowStorage{raft.NewMemoryStorage(), syncTime},
						ApplyCh: make(chan raft.ApplyMsg, 1<<16), Seed: int64(i + 1),
					})
					if err != nil {
						b.Fatal(err)
					}
					net.Register(i, node)
					nodes = append(nodes, node)
				}
				for _, n := range nodes {
					n.Start()
				}
				b.Cleanup(func() {
					for _, n := range nodes {
						n.Stop()
					}
				})
				// Drain what the nodes apply; this measures replication, not a state machine.
				for _, n := range nodes {
					_ = n
				}
				leader := func() *raft.Node {
					for {
						for _, n := range nodes {
							if n.Status().Role == raft.Leader {
								return n
							}
						}
						time.Sleep(5 * time.Millisecond)
					}
				}
				l := leader()
				time.Sleep(200 * time.Millisecond)

				var next, last atomic.Int64
				b.ResetTimer()
				var wg sync.WaitGroup
				for c := 0; c < clients; c++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for next.Add(1) <= int64(b.N) {
							index, _, ok := l.Propose([]byte("0123456789012345678901234567890123456789"))
							if !ok {
								b.Error("the leader refused a proposal")
								return
							}
							for {
								old := last.Load()
								if int64(index) <= old || last.CompareAndSwap(old, int64(index)) {
									break
								}
							}
						}
					}()
				}
				wg.Wait()
				for l.Status().CommitIndex < int(last.Load()) {
					time.Sleep(time.Millisecond)
				}
				b.StopTimer()
				b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "commits/s")
			})
		}
	}
}
