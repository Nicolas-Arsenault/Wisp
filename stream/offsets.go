package stream

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const offsetExt = ".offset"

// ValidateGroup rejects names that aren't a single directory name, so a
// client can't write outside the data dir.
func ValidateGroup(group string) error {
	if validTopic(group) != nil {
		return fmt.Errorf("%w: %q", ErrInvalidGroup, group)
	}
	return nil
}

// offsetPath returns <dataDir>/<topic>/groups/<group>/<shard>.offset.
func (r *Registry) offsetPath(topic, group string, shard int) string {
	return filepath.Join(r.dataDir, topic, "groups", group, strconv.Itoa(shard)+offsetExt)
}

// SaveOffset stores the next event id group reads from shard. Calls must not
// overlap for the same shard and group. It replaces the file by renaming, so
// a crash leaves the old or the new offset. It doesn't fsync: a power cut can
// lose a recent offset, and the group then re-reads those records.
func (r *Registry) SaveOffset(topic, group string, shard int, next uint64) error {
	if err := ValidateGroup(group); err != nil {
		return err
	}
	path := r.offsetPath(topic, group, shard)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, strconv.AppendUint(nil, next, 10), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadOffset returns the stored offset of group for shard. ok is false if
// there is none. A damaged file counts as none, and the group restarts from
// the position its consumer asks for.
func (r *Registry) LoadOffset(topic, group string, shard int) (next uint64, ok bool, err error) {
	if err := ValidateGroup(group); err != nil {
		return 0, false, err
	}
	path := r.offsetPath(topic, group, shard)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	next, err = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		log().Warn("ignoring damaged offset", "file", path, "err", err)
		return 0, false, nil
	}
	return next, true, nil
}
