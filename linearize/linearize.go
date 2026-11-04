// Package linearize checks whether a recorded history of key-value operations is
// linearizable: whether the operations can be put in a single order that respects
// real time (an operation that finished before another began comes first) and that
// a plain key-value map could have produced.
//
// It uses the Wing and Gong search with memoization. Keys are independent, so each is
// checked on its own, which keeps the search small.
package linearize

import (
	"fmt"
	"sort"
	"strings"
)

// Kind is the type of an operation.
type Kind int

const (
	Get Kind = iota
	Put
	Delete
)

// Op is one completed client operation. Call and Return are timestamps (any
// consistent clock) taken just before the request was sent and just after the
// answer arrived. For a Put, Value is the value written. For a Get, Value and
// Found are what the client saw.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	Value  string
	Found  bool
	Call   int64
	Return int64
}

func (o Op) String() string {
	switch o.Kind {
	case Put:
		return fmt.Sprintf("client %d put(%s=%s) [%d,%d]", o.Client, o.Key, o.Value, o.Call, o.Return)
	case Delete:
		return fmt.Sprintf("client %d delete(%s) [%d,%d]", o.Client, o.Key, o.Call, o.Return)
	default:
		if !o.Found {
			return fmt.Sprintf("client %d get(%s)=<none> [%d,%d]", o.Client, o.Key, o.Call, o.Return)
		}
		return fmt.Sprintf("client %d get(%s)=%s [%d,%d]", o.Client, o.Key, o.Value, o.Call, o.Return)
	}
}

// Check reports whether the history is linearizable. If it is not, badKey names a key
// whose operations cannot be ordered.
func Check(history []Op) (ok bool, badKey string) {
	byKey := make(map[string][]Op)
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if !checkKey(byKey[k]) {
			return false, k
		}
	}
	return true, ""
}

type state struct {
	value   string
	present bool
}

// step applies op to the register state, reporting whether the op's result is consistent with it.
func step(s state, op Op) (state, bool) {
	switch op.Kind {
	case Put:
		return state{value: op.Value, present: true}, true
	case Delete:
		return state{}, true
	default:
		if op.Found != s.present || (op.Found && op.Value != s.value) {
			return s, false
		}
		return s, true
	}
}

type search struct {
	ops    []Op
	done   []bool
	failed map[string]struct{} // (set of linearized ops, state) pairs that led nowhere
}

func checkKey(ops []Op) bool {
	s := &search{ops: ops, done: make([]bool, len(ops)), failed: make(map[string]struct{})}
	return s.run(state{}, len(ops))
}

func (s *search) run(cur state, remaining int) bool {
	if remaining == 0 {
		return true
	}
	memo := s.memoKey(cur)
	if _, seen := s.failed[memo]; seen {
		return false
	}

	// An operation can go first among those left only if no other one finished before it began.
	earliestReturn := int64(1<<63 - 1)
	for i, op := range s.ops {
		if !s.done[i] && op.Return < earliestReturn {
			earliestReturn = op.Return
		}
	}
	for i, op := range s.ops {
		if s.done[i] || op.Call > earliestReturn {
			continue
		}
		next, valid := step(cur, op)
		if !valid {
			continue
		}
		s.done[i] = true
		if s.run(next, remaining-1) {
			return true
		}
		s.done[i] = false
	}
	s.failed[memo] = struct{}{}
	return false
}

func (s *search) memoKey(cur state) string {
	var b strings.Builder
	for _, d := range s.done {
		if d {
			b.WriteByte('1')
		} else {
			b.WriteByte('0')
		}
	}
	fmt.Fprintf(&b, "|%t|%s", cur.present, cur.value)
	return b.String()
}

// Explain lists a key's operations in call order, for a failure message.
func Explain(history []Op, key string) string {
	var ops []Op
	for _, op := range history {
		if op.Key == key {
			ops = append(ops, op)
		}
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
	lines := make([]string, len(ops))
	for i, op := range ops {
		lines[i] = op.String()
	}
	return strings.Join(lines, "\n")
}
