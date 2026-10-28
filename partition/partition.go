// Package partition picks the shard of a partition key. Producers call it,
// so all records with the same key go to the same shard, in order.
package partition

import "hash/fnv"

// Shard returns FNV-1a(key) % numShards. numShards must be > 0.
func Shard(key []byte, numShards int) int {
	h := fnv.New32a()
	h.Write(key)
	return int(h.Sum32() % uint32(numShards))
}
