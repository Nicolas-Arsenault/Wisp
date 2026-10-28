package stream

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openTestRegistry(t *testing.T, dir string) *Registry {
	t.Helper()
	r, err := OpenRegistry(dir)
	if err != nil {
		t.Fatalf("OpenRegistry: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestTopicCreatesShards(t *testing.T) {
	dir := t.TempDir()
	r := openTestRegistry(t, dir)

	if _, ok := r.Lookup("orders"); ok {
		t.Fatal("Lookup found a topic that was never created")
	}
	topicShards, err := r.Topic("orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(topicShards) != NumShards {
		t.Errorf("got %d shards, want %d", len(topicShards), NumShards)
	}
	if _, err := os.Stat(filepath.Join(dir, "orders", "4.log")); err != nil {
		t.Errorf("shard file not created: %v", err)
	}
	again, _ := r.Topic("orders")
	if again[0] != topicShards[0] {
		t.Error("Topic opened the shards a second time")
	}
}

func TestTopicRejectsBadNames(t *testing.T) {
	r := openTestRegistry(t, t.TempDir())
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "../escape"} {
		if _, err := r.Topic(name); !errors.Is(err, ErrInvalidTopic) {
			t.Errorf("Topic(%q) = %v, want ErrInvalidTopic", name, err)
		}
	}
}

// Shards on disk are opened at startup, and ids continue where they stopped.
func TestReopenFindsTopics(t *testing.T) {
	dir := t.TempDir()
	r, err := OpenRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	topicShards, _ := r.Topic("orders")
	for range 3 {
		if _, err := topicShards[2].Append(t.Context(), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	r2 := openTestRegistry(t, dir)
	reopened, ok := r2.Lookup("orders")
	if !ok || len(reopened) != NumShards {
		t.Fatalf("Lookup after reopen = %d shards, %v", len(reopened), ok)
	}
	if id, err := reopened[2].Append(t.Context(), []byte(`{}`)); err != nil || id != 3 {
		t.Errorf("next id = %d, %v, want 3", id, err)
	}
}

func TestOpenRegistryMissingShard(t *testing.T) {
	dir := t.TempDir()
	os.MkdirAll(filepath.Join(dir, "t"), 0o755)
	os.WriteFile(filepath.Join(dir, "t", "1.log"), nil, 0o644) // no shard 0

	if _, err := OpenRegistry(dir); err == nil {
		t.Error("OpenRegistry accepted a topic without shard 0")
	}
}
