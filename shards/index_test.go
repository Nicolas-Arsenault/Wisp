package shards

import (
	"os"
	"path/filepath"
	"testing"
)

// useIndexCapacity sets indexCapacity for one test.
func useIndexCapacity(t *testing.T, n uint64) {
	t.Helper()
	old := indexCapacity
	indexCapacity = n
	t.Cleanup(func() { indexCapacity = old })
}

func openTestIndex(t *testing.T, path string) *index {
	t.Helper()
	x, err := openIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if x.data != nil {
			x.close()
		}
	})
	return x
}

func TestIndexCreatedAtFullSize(t *testing.T) {
	useIndexCapacity(t, 32)
	path := filepath.Join(t.TempDir(), "0.index")
	x := openTestIndex(t, path)

	if x.capacity != 32 {
		t.Errorf("capacity = %d, want 32", x.capacity)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 32*indexEntrySize {
		t.Errorf("file size = %d, want %d", info.Size(), 32*indexEntrySize)
	}
}

// floor finds the closest entry at or before the id asked for.
func TestIndexFloor(t *testing.T) {
	useIndexCapacity(t, 16)
	x := openTestIndex(t, filepath.Join(t.TempDir(), "0.index"))

	if _, _, ok := x.floor(0, 0); ok {
		t.Error("floor in an empty index found something")
	}
	// Sparse entries: ids 0, 100, 300.
	x.set(0, 0, 0)
	x.set(1, 100, 4100)
	x.set(2, 300, 8300)
	tests := []struct {
		id     uint64
		wantID uint64
		pos    int64
	}{
		{0, 0, 0}, {99, 0, 0}, {100, 100, 4100}, {299, 100, 4100}, {300, 300, 8300}, {350, 300, 8300},
	}
	for _, tt := range tests {
		id, pos, ok := x.floor(tt.id, 3)
		if !ok || id != tt.wantID || pos != tt.pos {
			t.Errorf("floor(%d) = %d, %d, %v, want %d, %d", tt.id, id, pos, ok, tt.wantID, tt.pos)
		}
	}
	// Entries beyond count are ignored.
	if id, _, _ := x.floor(350, 2); id != 100 {
		t.Errorf("floor(350) with 2 entries = %d, want 100", id)
	}
}

func TestIndexPersistsAndLastEntry(t *testing.T) {
	useIndexCapacity(t, 16)
	path := filepath.Join(t.TempDir(), "0.index")

	x := openTestIndex(t, path)
	if got := x.lastEntry(); got != 0 {
		t.Errorf("lastEntry of a new index = %d, want 0", got)
	}
	for i := range uint64(5) {
		x.set(i, i*10, int64(i*4096))
	}
	if err := x.close(); err != nil {
		t.Fatal(err)
	}

	// A smaller indexCapacity doesn't shrink an existing file.
	useIndexCapacity(t, 4)
	y := openTestIndex(t, path)
	if y.capacity != 16 {
		t.Errorf("capacity after reopen = %d, want 16", y.capacity)
	}
	if got := y.lastEntry(); got != 4 {
		t.Errorf("lastEntry = %d, want 4", got)
	}
	if id, pos := y.entry(4); id != 40 || pos != 4*4096 {
		t.Errorf("entry(4) = %d, %d, want 40, %d", id, pos, 4*4096)
	}
}

func TestOpenIndexRejectsBadSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "0.index")
	if err := os.WriteFile(path, make([]byte, 17), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := openIndex(path); err == nil {
		t.Error("openIndex accepted a 17-byte file")
	}
}
