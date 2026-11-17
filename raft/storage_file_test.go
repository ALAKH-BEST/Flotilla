package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := fs.Load(); ok || err != nil {
		t.Fatalf("fresh storage Load = ok %v, err %v; want not ok, no error", ok, err)
	}

	want := State{
		CurrentTerm:   4,
		VotedFor:      2,
		Log:           []Entry{{Term: 2}, {Term: 3, Command: []byte("a")}, {Term: 4, NoOp: true}},
		SnapshotIndex: 10,
		SnapshotTerm:  2,
		Snapshot:      []byte("snapshot bytes"),
	}
	if err := fs.Save(want); err != nil {
		t.Fatal(err)
	}

	// A new FileStorage on the same directory is what a restarted process sees.
	again, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := again.Load()
	if err != nil || !ok {
		t.Fatalf("Load after reopen = ok %v, err %v", ok, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state changed across a reopen:\n got %+v\nwant %+v", got, want)
	}
}

func TestFileStorageKeepsLastGoodStateAfterInterruptedSave(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	good := State{CurrentTerm: 1, VotedFor: -1, Log: []Entry{{}}}
	if err := fs.Save(good); err != nil {
		t.Fatal(err)
	}
	// A crash in the middle of a later save leaves a half-written temporary file.
	if err := os.WriteFile(filepath.Join(dir, walFileName+".tmp"), []byte("half writ"), 0o644); err != nil {
		t.Fatal(err)
	}
	reopened, _ := NewFileStorage(dir)
	got, ok, err := reopened.Load()
	if err != nil || !ok || got.CurrentTerm != 1 {
		t.Fatalf("Load = %+v, ok %v, err %v; want the last good state", got, ok, err)
	}
}

func TestFileStorageReportsCorruption(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	if err := os.WriteFile(filepath.Join(dir, walFileName), []byte("not a state file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fs.Load(); err == nil {
		t.Fatal("a corrupt state file loaded without an error")
	}
}

// entries builds n entries in the given term, each with a command of size bytes.
func entries(term, n, size, from int) []Entry {
	out := make([]Entry, n)
	for i := range out {
		out[i] = Entry{Term: term}
		if size > 0 { // an empty command is stored and read back as nil
			out[i].Command = bytes.Repeat([]byte{byte('a' + (from+i)%26)}, size)
		}
	}
	return out
}

func logOf(parts ...[]Entry) []Entry {
	log := []Entry{{}}
	for _, p := range parts {
		log = append(log, p...)
	}
	return log
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

func mustLoad(t *testing.T, dir string) State {
	t.Helper()
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, ok, err := fs.Load()
	if err != nil || !ok {
		t.Fatalf("Load = ok %v, err %v", ok, err)
	}
	return st
}

// An ordinary write appends a small record, however long the log already is, and a save that
// changes nothing writes nothing.
func TestFileStorageAppendsInsteadOfRewriting(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	path := filepath.Join(dir, walFileName)
	st := State{CurrentTerm: 1, VotedFor: -1, Log: logOf(entries(1, 500, 100, 0))}
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	base := fileSize(t, path)

	st.Log = append(st.Log, entries(1, 1, 100, 500)...)
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	grew := fileSize(t, path) - base
	if grew <= 0 || grew > 200 {
		t.Fatalf("appending one 100-byte entry to a log of 500 grew the file by %d bytes; want a small record", grew)
	}
	after := fileSize(t, path)
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	if fileSize(t, path) != after {
		t.Fatal("saving the same state again wrote something")
	}
	if got := mustLoad(t, dir); !reflect.DeepEqual(got, st) {
		t.Fatalf("reloaded state differs:\n got %+v\nwant %+v", got, st)
	}
}

// Whatever mix of appends, rewrites of the log's tail, term and vote changes and snapshots a node
// does, reopening the directory gives back exactly the last state saved.
func TestFileStorageRoundTripsRandomHistory(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	st := State{CurrentTerm: 1, VotedFor: -1, Log: logOf()}
	for step := 0; step < 400; step++ {
		switch r := rng.Intn(10); {
		case r < 5: // append
			st.Log = append(st.Log, entries(st.CurrentTerm, 1+rng.Intn(3), rng.Intn(20), step)...)
		case r < 7: // a new leader overwrites the tail
			if len(st.Log) > 1 {
				cut := 1 + rng.Intn(len(st.Log)-1)
				st.Log = append(st.Log[:cut:cut], entries(st.CurrentTerm, rng.Intn(3), 5, step)...)
			}
		case r < 8:
			st.CurrentTerm++
			st.VotedFor = rng.Intn(3) - 1
		case r < 9: // membership entry
			st.Log = append(st.Log, Entry{Term: st.CurrentTerm, Members: []Member{{ID: 0, Addr: "a:1"}, {ID: 1, Addr: "b:2", Learner: true}}})
		default: // a snapshot replaces the front of the log
			if len(st.Log) > 3 {
				at := 1 + rng.Intn(len(st.Log)-2)
				st.SnapshotIndex += at
				st.SnapshotTerm = st.Log[at].Term
				st.Snapshot = []byte(fmt.Sprintf("snap%d", step))
				st.Log = append([]Entry{{Term: st.SnapshotTerm}}, st.Log[at+1:]...)
			}
		}
		if err := fs.Save(st); err != nil {
			t.Fatal(err)
		}
		if step%7 == 0 { // a restart: a new FileStorage on the directory, which keeps going from there
			got := mustLoad(t, dir)
			if !reflect.DeepEqual(got, st) {
				t.Fatalf("step %d: reloaded state differs:\n got %+v\nwant %+v", step, got, st)
			}
			fs, _ = NewFileStorage(dir)
			if _, _, err := fs.Load(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if got := mustLoad(t, dir); !reflect.DeepEqual(got, st) {
		t.Fatalf("final state differs:\n got %+v\nwant %+v", got, st)
	}
}

// A crash can cut the file anywhere inside the record being appended. Cut it at every possible
// byte: what loads is always the state before that record or the state after it, never anything
// in between, and the stump is removed so later saves follow valid data.
func TestFileStorageSurvivesACutAtEveryByte(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	path := filepath.Join(dir, walFileName)
	st := State{CurrentTerm: 1, VotedFor: -1, Log: logOf(entries(1, 3, 10, 0))}
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	type point struct {
		size  int64
		state State
	}
	points := []point{{fileSize(t, path), cloneState(st)}}
	steps := []func(*State){
		func(s *State) { s.Log = append(s.Log, entries(1, 2, 30, 3)...) },
		func(s *State) { s.CurrentTerm, s.VotedFor = 2, 1 },
		func(s *State) { s.Log = append(s.Log[:3:3], entries(2, 2, 12, 9)...) }, // overwrite the tail
		func(s *State) { s.Log = append(s.Log, Entry{Term: 2, NoOp: true}) },
	}
	for _, step := range steps {
		step(&st)
		if err := fs.Save(st); err != nil {
			t.Fatal(err)
		}
		points = append(points, point{fileSize(t, path), cloneState(st)})
	}
	full, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	for cut := points[0].size; cut <= int64(len(full)); cut++ {
		want := points[0]
		for _, p := range points {
			if p.size <= cut {
				want = p
			}
		}
		d := t.TempDir()
		if err := os.WriteFile(filepath.Join(d, walFileName), full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		fs2, _ := NewFileStorage(d)
		got, ok, err := fs2.Load()
		if err != nil || !ok {
			t.Fatalf("cut at %d: Load = ok %v, err %v", cut, ok, err)
		}
		if !reflect.DeepEqual(got, want.state) {
			t.Fatalf("cut at %d: loaded the wrong state:\n got %+v\nwant %+v", cut, got, want.state)
		}
		if size := fileSize(t, filepath.Join(d, walFileName)); size != want.size {
			t.Fatalf("cut at %d: the file is %d bytes after loading, want the torn tail removed (%d)", cut, size, want.size)
		}
		next := cloneState(got)
		next.Log = append(next.Log, entries(9, 1, 4, 0)...)
		if err := fs2.Save(next); err != nil {
			t.Fatal(err)
		}
		if again := mustLoad(t, d); !reflect.DeepEqual(again, next) {
			t.Fatalf("cut at %d: a save after recovery did not survive a reopen", cut)
		}
	}
}

func cloneState(s State) State {
	s.Log = append([]Entry(nil), s.Log...)
	return s
}

// Damage inside the file, with more data after it, is reported and not skipped over: skipping
// would silently forget writes that were acknowledged.
func TestFileStorageReportsDamageInTheMiddle(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	path := filepath.Join(dir, walFileName)
	st := State{CurrentTerm: 1, VotedFor: -1, Log: logOf(entries(1, 3, 10, 0))}
	fs.Save(st)
	first := fileSize(t, path)
	st.Log = append(st.Log, entries(1, 2, 10, 3)...)
	fs.Save(st)
	second := fileSize(t, path)
	st.Log = append(st.Log, entries(1, 2, 10, 5)...)
	fs.Save(st)

	data, _ := os.ReadFile(path)
	bad := append([]byte(nil), data...)
	bad[first+headerSize+3] ^= 0xff // inside the second record, which the third follows
	d := t.TempDir()
	os.WriteFile(filepath.Join(d, walFileName), bad, 0o644)
	fs2, _ := NewFileStorage(d)
	if _, _, err := fs2.Load(); err == nil {
		t.Fatal("damage in the middle of the file loaded without an error")
	}

	// The same damage in the last record is indistinguishable from a torn write: it is dropped.
	bad = append([]byte(nil), data...)
	bad[second+headerSize+3] ^= 0xff
	d2 := t.TempDir()
	os.WriteFile(filepath.Join(d2, walFileName), bad, 0o644)
	fs3, _ := NewFileStorage(d2)
	got, ok, err := fs3.Load()
	if err != nil || !ok || len(got.Log) != 6 {
		t.Fatalf("a damaged last record: Load = log of %d, ok %v, err %v; want the state before it (6 entries)", len(got.Log), ok, err)
	}
}

// A snapshot replaces the file with a fresh checkpoint, which is what keeps it from growing for ever.
func TestFileStorageSnapshotShrinksTheFile(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	path := filepath.Join(dir, walFileName)
	st := State{CurrentTerm: 1, VotedFor: -1, Log: logOf(entries(1, 50, 100, 0))}
	fs.Save(st)
	for i := 0; i < 50; i++ {
		st.Log = append(st.Log, entries(1, 1, 100, i)...)
		fs.Save(st)
	}
	before := fileSize(t, path)
	st.SnapshotIndex, st.SnapshotTerm, st.Snapshot = 95, 1, []byte("state")
	st.Log = append([]Entry{{Term: 1}}, st.Log[96:]...)
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	if after := fileSize(t, path); after >= before/2 {
		t.Fatalf("the file is %d bytes after a snapshot that dropped most of the log, was %d", after, before)
	}
	if got := mustLoad(t, dir); !reflect.DeepEqual(got, st) {
		t.Fatalf("state after the snapshot differs:\n got %+v\nwant %+v", got, st)
	}
}

// A directory written by the single-file format of earlier versions still loads, and the first
// save moves it to the new file.
func TestFileStorageReadsTheOldFormat(t *testing.T) {
	dir := t.TempDir()
	old := State{CurrentTerm: 3, VotedFor: 1, Log: logOf(entries(3, 4, 8, 0)), SnapshotMembers: []Member{{ID: 0, Addr: "x"}}}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(old); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stateFileName), buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	fs, _ := NewFileStorage(dir)
	got, ok, err := fs.Load()
	if err != nil || !ok || !reflect.DeepEqual(got, old) {
		t.Fatalf("Load = %+v, ok %v, err %v; want the old state", got, ok, err)
	}
	old.Log = append(old.Log, entries(3, 1, 8, 4)...)
	if err := fs.Save(old); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, stateFileName)); !os.IsNotExist(err) {
		t.Fatal("the old file is still there after the first save")
	}
	if got := mustLoad(t, dir); !reflect.DeepEqual(got, old) {
		t.Fatalf("state after migrating differs:\n got %+v\nwant %+v", got, old)
	}
}

// Moving the snapshot point has to be saved even when the log that remains looks the same as the
// front of the old one (the same term and command over and over), which a log comparison alone
// would miss.
func TestFileStorageSavesAMovedSnapshotPoint(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewFileStorage(dir)
	same := make([]Entry, 8)
	for i := range same {
		same[i] = Entry{Term: 1, Command: []byte("x")}
	}
	st := State{CurrentTerm: 1, VotedFor: -1, Snapshot: []byte("s"), SnapshotTerm: 1, Log: append([]Entry{{Term: 1}}, same...)}
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	st.SnapshotIndex += 3
	st.Log = append([]Entry{{Term: 1}}, same[:5]...)
	if err := fs.Save(st); err != nil {
		t.Fatal(err)
	}
	if got := mustLoad(t, dir); got.SnapshotIndex != 3 || len(got.Log) != 6 {
		t.Fatalf("reloaded snapshot index %d and a log of %d; want 3 and 6", got.SnapshotIndex, len(got.Log))
	}
}
