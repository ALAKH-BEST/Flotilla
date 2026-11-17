package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage keeps State in an append-only file.
//
// The file is a series of records. The first is a checkpoint: the whole state. Each later record
// is a delta against the one before: the new term and vote, how many log entries to keep, and the
// entries to add after them. A save that only appends to the log therefore writes only the new
// entries and syncs once, however long the log is. A save that changes the snapshot, or that
// cannot be described as a delta, writes a new checkpoint to a temporary file, syncs it, renames
// it over the old file and syncs the directory, so a crash leaves either the old file or the new one.
//
// Every record carries a length and a checksum. A crash while a delta is being appended leaves a
// torn record at the end of the file; Load ignores it and removes it. A bad record that is
// followed by more data is damage, not a torn write, and is reported as an error.
//
// A directory written by an older version (a single raft-state.gob) is read as the starting
// checkpoint and replaced by the new file on the first save.
type FileStorage struct {
	mu     sync.Mutex
	dir    string
	path   string
	legacy string

	f         *os.File // the open file, for appending
	haveFile  bool     // the file on disk holds a checkpoint that cur agrees with
	cur       *State   // what the file holds
	fileBytes int64
}

const (
	walFileName   = "raft-wal.log"
	stateFileName = "raft-state.gob" // the single-file format of earlier versions

	recCheckpoint byte = 1
	recDelta      byte = 2

	headerSize = 8 // 4 bytes of length, 4 of checksum
	// A file this long is replaced by a fresh checkpoint even if saves keep being appends.
	maxFileBytes = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// NewFileStorage keeps state in dir, creating it if needed.
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data dir: %w", err)
	}
	path := filepath.Join(dir, walFileName)
	_ = os.Remove(path + ".tmp") // left over from a crash during a checkpoint; the real file is intact
	legacy := filepath.Join(dir, stateFileName)
	_ = os.Remove(legacy + ".tmp")
	return &FileStorage{dir: dir, path: path, legacy: legacy}, nil
}

// Save makes state durable. It does not return until the new record is on disk.
func (f *FileStorage) Save(state State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	state.Log = append([]Entry(nil), state.Log...) // the caller may keep changing its slice

	if f.cur == nil || !f.haveFile || f.fileBytes > maxFileBytes {
		return f.checkpoint(state)
	}
	keep, ok := deltaBase(f.cur, &state)
	if !ok {
		return f.checkpoint(state)
	}
	if keep == len(f.cur.Log) && len(state.Log) == keep &&
		state.CurrentTerm == f.cur.CurrentTerm && state.VotedFor == f.cur.VotedFor {
		return nil // nothing changed
	}
	rec := encodeDelta(state.CurrentTerm, state.VotedFor, keep, state.Log[keep:])
	if err := f.appendRecord(rec); err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	f.cur = &state
	return nil
}

// deltaBase reports how many leading log entries the saved and the new state share, and whether
// the new state can be written as a delta at all: it can unless the snapshot changed or the logs
// have nothing in common (not even the entry the snapshot stands for).
func deltaBase(old, cur *State) (keep int, ok bool) {
	if old.SnapshotIndex != cur.SnapshotIndex || old.SnapshotTerm != cur.SnapshotTerm ||
		!sameBytes(old.Snapshot, cur.Snapshot) ||
		!sameMembers(old.SnapshotMembers, cur.SnapshotMembers) || !sameMembers(old.SnapshotGone, cur.SnapshotGone) {
		return 0, false
	}
	n := min(len(old.Log), len(cur.Log))
	for keep < n && sameEntry(&old.Log[keep], &cur.Log[keep]) {
		keep++
	}
	return keep, keep > 0
}

// sameBytes compares snapshots. A snapshot is never modified once set and a node passes the
// same slice every time, so the same backing array means the same bytes and the copy is skipped.
func sameBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 || &a[0] == &b[0] {
		return true
	}
	return bytes.Equal(a, b)
}

func sameEntry(a, b *Entry) bool {
	return a.Term == b.Term && a.NoOp == b.NoOp && bytes.Equal(a.Command, b.Command) && sameMembers(a.Members, b.Members)
}

// appendRecord writes one record at the end of the file and syncs it.
func (f *FileStorage) appendRecord(payload []byte) error {
	if f.f == nil {
		file, err := os.OpenFile(f.path, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		f.f = file
	}
	rec := frame(payload)
	if _, err := f.f.Write(rec); err != nil {
		return err
	}
	if err := f.f.Sync(); err != nil {
		return err
	}
	f.fileBytes += int64(len(rec))
	return nil
}

func frame(payload []byte) []byte {
	rec := make([]byte, headerSize, headerSize+len(payload))
	binary.LittleEndian.PutUint32(rec[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(rec[4:], crc32.Checksum(payload, crcTable))
	return append(rec, payload...)
}

// checkpoint replaces the file with one holding just state.
func (f *FileStorage) checkpoint(state State) error {
	var buf bytes.Buffer
	buf.WriteByte(recCheckpoint)
	if err := gob.NewEncoder(&buf).Encode(state); err != nil {
		return fmt.Errorf("raft: encode state: %w", err)
	}
	rec := frame(buf.Bytes())

	tmp := f.path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	if _, err := file.Write(rec); err != nil {
		file.Close()
		return fmt.Errorf("raft: save state: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("raft: save state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	if f.f != nil {
		f.f.Close()
		f.f = nil
	}
	if err := os.Rename(tmp, f.path); err != nil {
		return fmt.Errorf("raft: save state: %w", err)
	}
	// Flush the directory entry so the rename itself survives a crash.
	if dir, err := os.Open(f.dir); err == nil {
		_ = dir.Sync()
		dir.Close()
	}
	_ = os.Remove(f.legacy) // superseded; if this is missed the new file still wins on the next load
	f.cur, f.haveFile, f.fileBytes = &state, true, int64(len(rec))
	return nil
}

// encodeDelta describes a change: the term and vote, how many of the old log's entries to keep,
// and the entries that follow them.
func encodeDelta(term, votedFor, keep int, entries []Entry) []byte {
	b := []byte{recDelta}
	b = binary.AppendVarint(b, int64(term))
	b = binary.AppendVarint(b, int64(votedFor))
	b = binary.AppendUvarint(b, uint64(keep))
	b = binary.AppendUvarint(b, uint64(len(entries)))
	for i := range entries {
		e := &entries[i]
		b = binary.AppendUvarint(b, uint64(e.Term))
		if e.NoOp {
			b = append(b, 1)
		} else {
			b = append(b, 0)
		}
		b = binary.AppendUvarint(b, uint64(len(e.Command)))
		b = append(b, e.Command...)
		b = binary.AppendUvarint(b, uint64(len(e.Members)))
		for _, m := range e.Members {
			b = binary.AppendVarint(b, int64(m.ID))
			b = binary.AppendUvarint(b, uint64(len(m.Addr)))
			b = append(b, m.Addr...)
			if m.Learner {
				b = append(b, 1)
			} else {
				b = append(b, 0)
			}
		}
	}
	return b
}

var errShort = errors.New("record is shorter than it says")

type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) varint() int64 {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = errShort
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) byte() byte {
	if len(r.b) < 1 {
		r.err = errShort
		return 0
	}
	v := r.b[0]
	r.b = r.b[1:]
	return v
}

func (r *reader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil || uint64(len(r.b)) < n {
		r.err = errShort
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return append([]byte(nil), v...)
}

// applyDelta changes st as the record says.
func applyDelta(st *State, payload []byte) error {
	r := &reader{b: payload}
	term, votedFor := int(r.varint()), int(r.varint())
	keep, count := r.uvarint(), r.uvarint()
	if r.err != nil {
		return r.err
	}
	if keep > uint64(len(st.Log)) {
		return fmt.Errorf("a delta keeps %d log entries but only %d exist", keep, len(st.Log))
	}
	log := st.Log[:keep:keep]
	for i := uint64(0); i < count; i++ {
		e := Entry{Term: int(r.uvarint()), NoOp: r.byte() == 1}
		if c := r.bytes(); len(c) > 0 {
			e.Command = c
		}
		for m := r.uvarint(); m > 0 && r.err == nil; m-- {
			id := int(r.varint())
			addr := string(r.bytes())
			e.Members = append(e.Members, Member{ID: id, Addr: addr, Learner: r.byte() == 1})
		}
		if r.err != nil {
			return r.err
		}
		log = append(log, e)
	}
	if len(r.b) != 0 {
		return errors.New("a delta has bytes left over")
	}
	st.CurrentTerm, st.VotedFor, st.Log = term, votedFor, log
	return nil
}

// Load reads the saved state back. ok is false if nothing was ever saved.
func (f *FileStorage) Load() (State, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := os.ReadFile(f.path)
	if os.IsNotExist(err) {
		return f.loadLegacy()
	}
	if err != nil {
		return State{}, false, fmt.Errorf("raft: load state: %w", err)
	}

	var st State
	pos, have := 0, false
	for pos < len(data) {
		rest := data[pos:]
		if len(rest) < headerSize {
			break // a torn header at the end
		}
		n := int(binary.LittleEndian.Uint32(rest[0:]))
		if n > len(rest)-headerSize {
			break // the record's body was cut short: a torn write at the end
		}
		end := pos + headerSize + n
		payload := rest[headerSize : headerSize+n]
		if crc32.Checksum(payload, crcTable) != binary.LittleEndian.Uint32(rest[4:]) {
			if end == len(data) {
				break // the last record never finished being written
			}
			return State{}, false, fmt.Errorf("raft: state file %s is corrupt at byte %d", f.path, pos)
		}
		if err := f.apply(&st, &have, payload); err != nil {
			return State{}, false, fmt.Errorf("raft: state file %s is corrupt at byte %d: %w", f.path, pos, err)
		}
		pos = end
	}
	if !have {
		return State{}, false, fmt.Errorf("raft: state file %s has no checkpoint", f.path)
	}
	if pos < len(data) { // drop the torn record so the next append follows valid data
		if err := os.Truncate(f.path, int64(pos)); err != nil {
			return State{}, false, fmt.Errorf("raft: trim torn record: %w", err)
		}
	}
	f.cur = cloneForCompare(st)
	f.haveFile, f.fileBytes = true, int64(pos)
	return st, true, nil
}

func (f *FileStorage) apply(st *State, have *bool, payload []byte) error {
	if len(payload) == 0 {
		return errors.New("empty record")
	}
	switch payload[0] {
	case recCheckpoint:
		if *have {
			return errors.New("a second checkpoint inside the file")
		}
		if err := gob.NewDecoder(bytes.NewReader(payload[1:])).Decode(st); err != nil {
			return err
		}
		*have = true
		return nil
	case recDelta:
		if !*have {
			return errors.New("a delta before any checkpoint")
		}
		return applyDelta(st, payload[1:])
	}
	return fmt.Errorf("unknown record type %d", payload[0])
}

// cloneForCompare copies the log so the copy kept for comparison is not shared with the node,
// which changes its own slice in place.
func cloneForCompare(st State) *State {
	st.Log = append([]Entry(nil), st.Log...)
	return &st
}

// loadLegacy reads a directory written before the append-only file existed.
func (f *FileStorage) loadLegacy() (State, bool, error) {
	data, err := os.ReadFile(f.legacy)
	if os.IsNotExist(err) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, fmt.Errorf("raft: load state: %w", err)
	}
	var st State
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		return State{}, false, fmt.Errorf("raft: state file %s is corrupt: %w", f.legacy, err)
	}
	f.cur = cloneForCompare(st)
	f.haveFile = false // the first save writes the new file
	return st, true, nil
}
