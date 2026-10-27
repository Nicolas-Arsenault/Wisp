# Streams

Def: logical unit into which related events are written. A producer sends an event to a stream. 

Streams are named.

## Writing to a stream

`stream.WriteToStream(data, topic)` appends a record to one of the topic's shards.

- **Shard files:** `<dataDir>/<topic>/<id>`, where `dataDir` defaults to `../data` and `id` goes from `0` to `NUMBER_SHARDS - 1` (currently 5 shards).
- **Shard selection:** round robin for each topic (0, 1, 2, 3, 4, 0, ...). There is no partition key yet.
- **Directories:** the topic directory is created if it doesn't exist.
- **Errors:** returns the underlying error, unwrapped, if the directory can't be created or the append fails. Failures are logged at Error level and successful writes at Debug level, through `log()` (slog), the same as the `shards` package. Use `stream.SetLogger` to change the logger. The round-robin position still moves forward when a write fails.
- **Concurrency:** not safe for concurrent use, because the round-robin state is a plain map.
