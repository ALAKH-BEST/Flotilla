package linearize

import "testing"

func put(c int, k, v string, call, ret int64) Op {
	return Op{Client: c, Kind: Put, Key: k, Value: v, Call: call, Return: ret}
}
func del(c int, k string, call, ret int64) Op {
	return Op{Client: c, Kind: Delete, Key: k, Call: call, Return: ret}
}
func get(c int, k, v string, found bool, call, ret int64) Op {
	return Op{Client: c, Kind: Get, Key: k, Value: v, Found: found, Call: call, Return: ret}
}

func TestSequentialHistoryIsLinearizable(t *testing.T) {
	h := []Op{
		get(1, "x", "", false, 1, 2),
		put(1, "x", "1", 3, 4),
		get(1, "x", "1", true, 5, 6),
		del(1, "x", 7, 8),
		get(1, "x", "", false, 9, 10),
	}
	if ok, k := Check(h); !ok {
		t.Fatalf("rejected a valid history (key %s)", k)
	}
}

func TestStaleReadIsRejected(t *testing.T) {
	h := []Op{
		put(1, "x", "1", 1, 2),
		put(1, "x", "2", 3, 4),
		get(2, "x", "1", true, 5, 6), // starts after put 2 finished but sees the old value
	}
	if ok, _ := Check(h); ok {
		t.Fatal("accepted a stale read")
	}
}

func TestReadOfNeverWrittenValueIsRejected(t *testing.T) {
	h := []Op{
		put(1, "x", "1", 1, 2),
		get(2, "x", "7", true, 3, 4),
	}
	if ok, _ := Check(h); ok {
		t.Fatal("accepted a value nobody wrote")
	}
}

func TestOverlappingOperationsMayOrderEitherWay(t *testing.T) {
	// The get overlaps the put, so seeing either the old or the new value is fine.
	for _, seen := range []struct {
		v     string
		found bool
	}{{"", false}, {"1", true}} {
		h := []Op{
			put(1, "x", "1", 1, 10),
			get(2, "x", seen.v, seen.found, 2, 9),
		}
		if ok, _ := Check(h); !ok {
			t.Fatalf("rejected a get that overlaps the put and saw %+v", seen)
		}
	}
}

func TestReadsCannotGoBackInTime(t *testing.T) {
	// Two sequential reads during one long write: new then old is impossible.
	h := []Op{
		put(1, "x", "1", 1, 20),
		get(2, "x", "1", true, 2, 3),
		get(2, "x", "", false, 4, 5),
	}
	if ok, _ := Check(h); ok {
		t.Fatal("accepted a read that went back to an older value")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Op{
		put(1, "x", "1", 1, 2),
		put(1, "y", "2", 3, 4),
		get(2, "x", "1", true, 5, 6),
		get(2, "y", "2", true, 7, 8),
	}
	if ok, _ := Check(h); !ok {
		t.Fatal("rejected a valid two-key history")
	}
	h = append(h, get(3, "y", "1", true, 9, 10)) // y was never 1
	if ok, key := Check(h); ok || key != "y" {
		t.Fatalf("Check = %v, %q; want false, y", ok, key)
	}
}
