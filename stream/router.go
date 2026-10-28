// decides to which shard it goes
package stream

// for now we will use a hardcoded value for the number of shards
// lets say 5
import (
	"os"
	"path/filepath"
	"sync"
	"wisp/shards"
)

// SafeCache is a map guarded by a RWMutex, safe for concurrent use.
// Each method is atomic on its own; to read and then write a key without
// another goroutine slipping in between, use Update instead of Get + Set.
type SafeCache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]V
}

// NewSafeCache returns an empty cache ready to use.
func NewSafeCache[K comparable, V any]() *SafeCache[K, V] {
	return &SafeCache[K, V]{
		items: make(map[K]V),
	}
}

func (c *SafeCache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = value
}

func (c *SafeCache[K, V]) Delete(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

func (c *SafeCache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, exist := c.items[key]
	return val, exist
}

// Update atomically replaces the value of key with fn(old, exists), where old
// is the zero value of V when the key is missing. It returns the old value and
// whether it existed. fn runs while the lock is held, so it must be quick and
// must not call back into the cache (that would deadlock).
func (c *SafeCache[K, V]) Update(key K, fn func(old V, exists bool) V) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	old, exists := c.items[key]
	c.items[key] = fn(old, exists)
	return old, exists
}

// dirPerm is the permission used when creating a topic's directory.
const dirPerm = 0o755

var NUMBER_SHARDS = 5

// cache holds the next shard to write to for each topic. It must be shared by
// all calls, so it lives at package level. TODO: check eviction policies later.
var cache = NewSafeCache[string, int]()

// WriteToStream appends data to the next shard of topic, picking shards
// round robin (0, 1, ..., NUMBER_SHARDS-1, 0, ...). The topic directory is
// created if it does not exist.
//
// Returns an error if the topic directory cannot be created or the append
// fails. The round-robin position advances even on failure.
//
// Safe for concurrent use: each call gets its own shard turn atomically.
func WriteToStream(data []byte, topic string) error {
	// A missing topic has turn 0 (the zero value), so it starts at shard 0.
	turn, _ := cache.Update(topic, func(old int, _ bool) int {
		return (old + 1) % NUMBER_SHARDS //simple round robin for now
	})

	fileName := generateShardFileName(turn, topic)
	if err := os.MkdirAll(filepath.Dir(fileName), dirPerm); err != nil {
		log().Error("failed to create topic dir", "topic", topic, "err", err)
		return err
	}
	//TODO: writer pool later...
	if err := shards.AppendToFile(fileName, string(data)); err != nil {
		log().Error("failed to write to stream", "topic", topic, "shard", turn, "err", err)
		return err
	}
	log().Debug("wrote to stream", "topic", topic, "shard", turn, "bytes", len(data))
	return nil
}
