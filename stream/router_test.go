package stream

import (
	"os"
	"path/filepath"
	"strconv"
	"sync"
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
	cache = NewSafeCache[string, int]()
	t.Cleanup(func() {
		dataDir = oldDataDir
		cache = NewSafeCache[string, int]()
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

func TestSafeCacheGetSetDelete(t *testing.T) {
	c := NewSafeCache[string, int]()

	if v, ok := c.Get("missing"); ok || v != 0 {
		t.Errorf("Get(missing) = %d, %v, want 0, false", v, ok)
	}

	c.Set("a", 1)
	c.Set("a", 2) // overwrite
	if v, ok := c.Get("a"); !ok || v != 2 {
		t.Errorf("Get(a) = %d, %v, want 2, true", v, ok)
	}

	c.Delete("a")
	if _, ok := c.Get("a"); ok {
		t.Error("Get(a) after Delete should not find the key")
	}
	c.Delete("a") // deleting a missing key must not panic
}

func TestSafeCacheUpdate(t *testing.T) {
	c := NewSafeCache[string, int]()
	inc := func(old int, _ bool) int { return old + 1 }

	if old, existed := c.Update("k", inc); old != 0 || existed {
		t.Errorf("first Update = %d, %v, want 0, false", old, existed)
	}
	if old, existed := c.Update("k", inc); old != 1 || !existed {
		t.Errorf("second Update = %d, %v, want 1, true", old, existed)
	}
	if v, _ := c.Get("k"); v != 2 {
		t.Errorf("Get(k) = %d, want 2", v)
	}
}

// Run with -race. Get + Set from many goroutines would lose increments;
// Update must not.
func TestSafeCacheConcurrentUpdate(t *testing.T) {
	const goroutines, perGoroutine = 50, 100
	c := NewSafeCache[string, int]()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				c.Update("k", func(old int, _ bool) int { return old + 1 })
				c.Get("k")
			}
		}()
	}
	wg.Wait()

	if v, _ := c.Get("k"); v != goroutines*perGoroutine {
		t.Errorf("Get(k) = %d, want %d (increments were lost)", v, goroutines*perGoroutine)
	}
}

// Concurrent writers must still spread records evenly over the shards.
func TestWriteToStreamConcurrent(t *testing.T) {
	oldDataDir := dataDir
	dataDir = t.TempDir()
	cache = NewSafeCache[string, int]()
	t.Cleanup(func() {
		dataDir = oldDataDir
		cache = NewSafeCache[string, int]()
	})

	const perShard = 10
	record := []byte("{\"test\":123}")

	var wg sync.WaitGroup
	for i := 0; i < NUMBER_SHARDS*perShard; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := WriteToStream(record, "test"); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	wg.Wait()

	// Every record is identical, so even spreading means equal file sizes.
	first, err := os.Stat(generateShardFileName(0, "test"))
	if err != nil {
		t.Fatalf("shard 0 doesn't exist: %v", err)
	}
	for id := 1; id < NUMBER_SHARDS; id++ {
		info, err := os.Stat(generateShardFileName(id, "test"))
		if err != nil {
			t.Fatalf("shard %d doesn't exist: %v", id, err)
		}
		if info.Size() != first.Size() {
			t.Errorf("shard %d size = %d, want %d (uneven round robin)", id, info.Size(), first.Size())
		}
	}
}
