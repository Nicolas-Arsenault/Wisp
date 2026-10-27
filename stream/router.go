// decides to which shard it goes
package stream

// for now we will use a hardcoded value for the number of shards
// lets say 5
import (
	"os"
	"path/filepath"
	"wisp/shards"
)

// dirPerm is the permission used when creating a topic's directory.
const dirPerm = 0o755

var NUMBER_SHARDS = 5
var cache = make(map[string]int)

// WriteToStream appends data to the next shard of topic, picking shards
// round robin (0, 1, ..., NUMBER_SHARDS-1, 0, ...). The topic directory is
// created if it does not exist.
//
// Returns an error if the topic directory cannot be created or the append
// fails. The round-robin position advances even on failure.
//
// Not safe for concurrent use (cache is an unguarded map).
func WriteToStream(data []byte, topic string) error {
	turn := cache[topic]                      // missing topic starts at shard 0 TODO: check eviction policies later + multithread....
	cache[topic] = (turn + 1) % NUMBER_SHARDS //simple round robin for now

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
