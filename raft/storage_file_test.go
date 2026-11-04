package raft

import (
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
	if err := os.WriteFile(filepath.Join(dir, stateFileName+".tmp"), []byte("half writ"), 0o644); err != nil {
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
	if err := os.WriteFile(filepath.Join(dir, stateFileName), []byte("not a state file"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := fs.Load(); err == nil {
		t.Fatal("a corrupt state file loaded without an error")
	}
}
