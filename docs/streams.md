# Streams

Def: logical unit into which related events are written. A producer sends an event to a stream. 

Streams are named.

## Writing to a stream

`stream.WriteToStream(data, topic)` appends a record to one of the topic's shards.

- **Shard files:** `<dataDir>/<topic>/<id>`, where `dataDir` defaults to `../data` and `id` goes from `0` to `NUMBER_SHARDS - 1` (currently 5 shards).
- **Shard selection:** round robin for each topic (0, 1, 2, 3, 4, 0, ...). There is no partition key yet.
- **Directories:** the topic directory is created if it doesn't exist.
- **Payload:** `data` must be a JSON object. It is stored with the shard's next `seq_n` added (see [shards.md](shards.md#sequence-numbers)).
- **Errors:** returns the underlying error, unwrapped, if the directory can't be created or the append fails. Failures are logged at Error level and successful writes at Debug level, through `log()` (slog), the same as the `shards` package. Use `stream.SetLogger` to change the logger. The round-robin position still moves forward when a write fails.
- **Concurrency:** safe for concurrent use. The per-topic position is kept in a `SafeCache` (a map guarded by a `sync.RWMutex`), and each write gets its shard through `SafeCache.Update`, which reads and moves the position forward under one lock. Using `Get` and then `Set` instead would let two writers get the same shard.
- **Result:** on success, `WriteToStream` returns a `WriteResult`: topic, shard, `seq_n` and the stored payload. `EventID()` formats it as `"<topic>/<shard>/<seq_n>"`.
