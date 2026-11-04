package server

import (
	"context"
	"time"

	"github.com/ALAKH-BEST/flotilla/kv"
)

// Doer is anything that can run a command: a Server, or a client stub for one.
type Doer interface {
	Do(ctx context.Context, cmd kv.Command) (kv.Result, error)
}

// Clerk is a client. It finds the leader, retries failed requests with the same
// sequence number so they run exactly once, and keeps going until the request succeeds
// or the context ends. A Clerk is for one goroutine at a time and needs a unique ID.
type Clerk struct {
	servers []Doer
	id      int64
	seq     int64
	leader  int
	attempt time.Duration
}

// NewClerk creates a client with the given non-zero unique ID.
func NewClerk(id int64, servers []Doer) *Clerk {
	return &Clerk{servers: servers, id: id, attempt: 2 * time.Second}
}

func (c *Clerk) do(ctx context.Context, op kv.Op, key, value string) (kv.Result, error) {
	c.seq++
	cmd := kv.Command{Op: op, Key: key, Value: value, ClientID: c.id, Seq: c.seq}
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, c.attempt)
		res, err := c.servers[c.leader].Do(attemptCtx, cmd)
		cancel()
		if err == nil {
			return res, nil
		}
		if ctx.Err() != nil {
			return kv.Result{}, ctx.Err()
		}
		c.leader = (c.leader + 1) % len(c.servers)
		time.Sleep(10 * time.Millisecond)
	}
}

// Get returns the value of key and whether it exists.
func (c *Clerk) Get(ctx context.Context, key string) (string, bool, error) {
	res, err := c.do(ctx, kv.OpGet, key, "")
	return res.Value, res.Found, err
}

// Put sets key to value.
func (c *Clerk) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, kv.OpPut, key, value)
	return err
}

// Delete removes key.
func (c *Clerk) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, kv.OpDelete, key, "")
	return err
}
