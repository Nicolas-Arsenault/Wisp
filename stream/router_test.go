package stream

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"wisp/shards"
)

func TestShardFileName(t *testing.T) {
	fname := generateShardFileName(1, "testing123")

	if fname != filepath.Join("..", "data", "testing123", "1") {
		t.Errorf("generateShardFileName = %q, want ../data/testing123/1", fname)
	}
}

// uniqueTopic returns a topic name no other test or run has used. The shard
// sequence numbers in package shards last for the whole test binary
// (including -count reruns), so a fresh topic is the only way to know each
// shard starts at seq_n 0.
func uniqueTopic() string {
	return "topic-" + strconv.FormatInt(topicCounter.Add(1), 10)
}

// topicCounter backs uniqueTopic; it lives for the whole test binary.
var topicCounter atomic.Int64

// compressed returns record as AppendToFile stores it.
func compressed(t *testing.T, record string) string {
	t.Helper()
	c, err := shards.CompressRawJSON([]byte(record))
	if err != nil {
		t.Fatal(err)
	}
	return c
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

	topic := uniqueTopic()
	record := []byte("{\"test\":123}")
	for i := 0; i < NUMBER_SHARDS+1; i++ { // one extra write to test wrap around
		if _, err := WriteToStream(record, topic); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Each shard holds its seq_n 0 record; shard 0 also got the wrap-around
	// write, which is its seq_n 1.
	first := compressed(t, `{"seq_n":0,"test":123}`)
	for id := 0; id < NUMBER_SHARDS; id++ {
		want := first
		if id == 0 {
			want += compressed(t, `{"seq_n":1,"test":123}`)
		}
		got, err := os.ReadFile(generateShardFileName(id, topic))
		if err != nil {
			t.Fatalf("shard %d doesn't exist: %v", id, err)
		}
		if string(got) != want {
			t.Errorf("shard %d content is not the expected records", id)
		}
	}

	// No shard beyond NUMBER_SHARDS-1 should be created.
	if _, err := os.Stat(filepath.Join(dataDir, topic, strconv.Itoa(NUMBER_SHARDS))); !os.IsNotExist(err) {
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

	topic := uniqueTopic()
	const perShard = 10
	record := []byte("{\"test\":123}")

	var wg sync.WaitGroup
	for i := 0; i < NUMBER_SHARDS*perShard; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := WriteToStream(record, topic); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	wg.Wait()

	// Even spreading means every shard holds seq_n 0..perShard-1, in order
	// (shards writes under a file lock), so all files have the same size.
	first, err := os.Stat(generateShardFileName(0, topic))
	if err != nil {
		t.Fatalf("shard 0 doesn't exist: %v", err)
	}
	for id := 1; id < NUMBER_SHARDS; id++ {
		info, err := os.Stat(generateShardFileName(id, topic))
		if err != nil {
			t.Fatalf("shard %d doesn't exist: %v", id, err)
		}
		if info.Size() != first.Size() {
			t.Errorf("shard %d size = %d, want %d (uneven round robin)", id, info.Size(), first.Size())
		}
	}
}

func TestWriteToStreamResult(t *testing.T) {
	oldDataDir := dataDir
	dataDir = t.TempDir()
	t.Cleanup(func() { dataDir = oldDataDir })
	topic := uniqueTopic()

	// A fresh topic: shard 0, then shard 1, each with seq_n 0.
	for shard := 0; shard < 2; shard++ {
		got, err := WriteToStream([]byte(`{"a":1}`), topic)
		if err != nil {
			t.Fatalf("WriteToStream: %v", err)
		}
		want := WriteResult{Topic: topic, Shard: shard, Seq: 0, Payload: []byte(`{"a":1,"seq_n":0}`)}
		if got.Topic != want.Topic || got.Shard != want.Shard || got.Seq != want.Seq || string(got.Payload) != string(want.Payload) {
			t.Errorf("write %d = %+v, want %+v", shard, got, want)
		}
		if id := got.EventID(); id != topic+"/"+strconv.Itoa(shard)+"/0" {
			t.Errorf("EventID = %q, want %s/%d/0", id, topic, shard)
		}
	}
}

func TestWriteToStreamInvalidPayload(t *testing.T) {
	oldDataDir := dataDir
	dataDir = t.TempDir()
	t.Cleanup(func() { dataDir = oldDataDir })

	got, err := WriteToStream([]byte("not json"), uniqueTopic())
	if !errors.Is(err, shards.ErrInvalidPayload) {
		t.Fatalf("err = %v, want shards.ErrInvalidPayload", err)
	}
	if got.Payload != nil || got.Topic != "" {
		t.Errorf("result on error = %+v, want zero value", got)
	}
}
