# Streams

Def: a named, logical unit (a topic) that related events are written to. Each topic is split into shards.

Code: `stream/`.

## Registry

`stream.Registry` holds the open shards of every topic.

- **Startup:** `OpenRegistry(dataDir)` opens every shard found on disk (`<dataDir>/<topic>/<id>.log`) and starts one writer per shard. Shard ids of a topic must go from 0 to n-1.
- **New topics:** `Topic(name)` creates the topic with `NumShards` (5) shards the first time it's used. `Lookup(name)` only finds existing topics.
- **Topic names:** must be a single directory name (no `/`, `\`, `.` or `..`). Otherwise `ErrInvalidTopic`.
- **Locking:** a mutex only guards the topic map (topic creation). Writing and reading shards takes no lock.
- **Close:** `Close()` closes every shard; records still queued are stored first.

There is no round robin: the producer chooses the shard (see [records.md](records.md)).
