// Package stream keeps the open shards of every topic.
package stream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"wisp/shards"
)

// NumShards is the number of shards of a new topic.
const NumShards = 5

var ErrInvalidTopic = errors.New("stream: invalid topic name")

// Registry maps topics to their shards. The lock only guards the map
// (topic creation); the shards need no lock.
type Registry struct {
	dataDir string
	mu      sync.RWMutex
	topics  map[string][]*shards.Shard
	closed  bool
}

// OpenRegistry opens every shard already in dataDir (<topic>/<id>.log),
// which starts one writer per shard.
func OpenRegistry(dataDir string) (*Registry, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}

	r := &Registry{dataDir: dataDir, topics: make(map[string][]*shards.Shard)}
	for _, e := range entries {
		if !e.IsDir() || validTopic(e.Name()) != nil {
			continue
		}
		topicShards, err := openTopic(dataDir, e.Name())
		if err != nil {
			r.Close()
			return nil, err
		}
		if len(topicShards) > 0 {
			r.topics[e.Name()] = topicShards
		}
	}
	log().Info("opened data dir", "dir", dataDir, "topics", len(r.topics))
	return r, nil
}

// openTopic opens the shards of a topic found on disk. Ids must be 0..n-1.
func openTopic(dataDir, topic string) ([]*shards.Shard, error) {
	files, err := os.ReadDir(filepath.Join(dataDir, topic))
	if err != nil {
		return nil, err
	}
	count := 0
	for _, f := range files {
		if name, ok := strings.CutSuffix(f.Name(), shards.LogExt); ok {
			if _, err := strconv.Atoi(name); err == nil {
				count++
			}
		}
	}

	var opened []*shards.Shard
	for id := range count {
		if _, err := os.Stat(filepath.Join(dataDir, topic, strconv.Itoa(id)+shards.LogExt)); err != nil {
			closeAll(opened)
			return nil, fmt.Errorf("stream: topic %q is missing shard %d", topic, id)
		}
		s, err := shards.Open(dataDir, topic, id)
		if err != nil {
			closeAll(opened)
			return nil, err
		}
		opened = append(opened, s)
	}
	return opened, nil
}

// validTopic rejects names that aren't a single directory name, so a client
// can't write outside the data dir.
func validTopic(topic string) error {
	if topic == "" || topic == "." || topic == ".." || strings.ContainsAny(topic, "/\\\x00") {
		return fmt.Errorf("%w: %q", ErrInvalidTopic, topic)
	}
	return nil
}

// Topic returns the shards of topic, creating it with NumShards shards if
// needed.
func (r *Registry) Topic(topic string) ([]*shards.Shard, error) {
	if s, ok := r.Lookup(topic); ok {
		return s, nil
	}
	if err := validTopic(topic); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, shards.ErrShardClosed
	}
	if s, ok := r.topics[topic]; ok {
		return s, nil
	}
	var created []*shards.Shard
	for id := range NumShards {
		s, err := shards.Open(r.dataDir, topic, id)
		if err != nil {
			closeAll(created)
			return nil, err
		}
		created = append(created, s)
	}
	r.topics[topic] = created
	log().Info("created topic", "topic", topic, "shards", NumShards)
	return created, nil
}

// Lookup returns the shards of an existing topic.
func (r *Registry) Lookup(topic string) ([]*shards.Shard, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.topics[topic]
	return s, ok
}

// Close closes every shard. Nothing may read from them afterwards.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	var errs []error
	for topic, topicShards := range r.topics {
		errs = append(errs, closeAll(topicShards))
		delete(r.topics, topic)
	}
	return errors.Join(errs...)
}

func closeAll(list []*shards.Shard) error {
	var errs []error
	for _, s := range list {
		errs = append(errs, s.Close())
	}
	return errors.Join(errs...)
}
