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

// Offsets survive a restart, per topic, group and shard.
func TestOffsets(t *testing.T) {
	dir := t.TempDir()
	r, err := OpenRegistry(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Topic("orders"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.LoadOffset("orders", "g", 1); ok || err != nil {
		t.Fatalf("LoadOffset before any save = %v, %v", ok, err)
	}
	for _, next := range []uint64{5, 12} { // the last save wins
		if err := r.SaveOffset("orders", "g", 1, next); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()

	r2 := openTestRegistry(t, dir) // the groups directory isn't taken for a shard
	if next, ok, err := r2.LoadOffset("orders", "g", 1); next != 12 || !ok || err != nil {
		t.Errorf("LoadOffset = %d, %v, %v, want 12", next, ok, err)
	}
	if _, ok, _ := r2.LoadOffset("orders", "g", 2); ok {
		t.Error("found an offset for another shard")
	}
	if _, ok, _ := r2.LoadOffset("orders", "other", 1); ok {
		t.Error("found an offset for another group")
	}
}

func TestOffsetRejectsBadGroup(t *testing.T) {
	r := openTestRegistry(t, t.TempDir())
	for _, group := range []string{"", "..", "a/b"} {
		if err := r.SaveOffset("orders", group, 0, 1); !errors.Is(err, ErrInvalidGroup) {
			t.Errorf("SaveOffset(%q) = %v, want ErrInvalidGroup", group, err)
		}
		if _, _, err := r.LoadOffset("orders", group, 0); !errors.Is(err, ErrInvalidGroup) {
			t.Errorf("LoadOffset(%q) = %v, want ErrInvalidGroup", group, err)
		}
	}
}

// A damaged offset counts as none, rather than stopping the group.
func TestDamagedOffsetIsIgnored(t *testing.T) {
	dir := t.TempDir()
	r := openTestRegistry(t, dir)
	if err := r.SaveOffset("orders", "g", 0, 5); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders", "groups", "g", "0.offset"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.LoadOffset("orders", "g", 0); ok || err != nil {
		t.Errorf("LoadOffset = %v, %v, want none", ok, err)
	}
}
