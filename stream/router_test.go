package stream

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestShardFileName(t *testing.T) {
	fname := generateShardFileName(1, "testing123")

	if fname != filepath.Join("..", "data", "testing123", "1") {
		t.Errorf("generateShardFileName = %q, want ../data/testing123/1", fname)
	}
}

func TestWriteToStream(t *testing.T) {
	// Write into a temp dir instead of the repo, and start with a fresh cache.
	oldDataDir := dataDir
	dataDir = t.TempDir()
	cache = make(map[string]int)
	t.Cleanup(func() {
		dataDir = oldDataDir
		cache = make(map[string]int)
	})

	record := []byte("{\"test\":123}")
	for i := 0; i < NUMBER_SHARDS+1; i++ { // one extra write to test wrap around
		if err := WriteToStream(record, "test"); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	sizes := make([]int64, NUMBER_SHARDS)
	for id := 0; id < NUMBER_SHARDS; id++ {
		info, err := os.Stat(generateShardFileName(id, "test"))
		if err != nil {
			t.Fatalf("shard %d doesn't exist: %v", id, err)
		}
		sizes[id] = info.Size()
	}

	// Every write uses the same record, so each shard holds one copy except
	// shard 0, which got the wrap-around write as well.
	if sizes[0] != 2*sizes[1] {
		t.Errorf("shard 0 size = %d, want %d (two records)", sizes[0], 2*sizes[1])
	}
	for id := 2; id < NUMBER_SHARDS; id++ {
		if sizes[id] != sizes[1] {
			t.Errorf("shard %d size = %d, want %d (one record)", id, sizes[id], sizes[1])
		}
	}

	// No shard beyond NUMBER_SHARDS-1 should be created.
	if _, err := os.Stat(filepath.Join(dataDir, "test", strconv.Itoa(NUMBER_SHARDS))); !os.IsNotExist(err) {
		t.Errorf("shard %d should not exist, stat err = %v", NUMBER_SHARDS, err)
	}
}
