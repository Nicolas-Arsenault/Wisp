# Shards

Def: One portion of a stream. Singular append-only logs that store the records. 

Records partition key determine which shard within a stream it ends up in.

## File locking

`AppendToFile` and `ReadFromFile` hold a mutex for each file while they run, so goroutines in the same process can't interleave a write with another write or a read.

- **Lock key:** the absolute, cleaned path (`filepath.Abs`). Different spellings of one file (`../data/t/0`, `data/t/../t/0`, an absolute path) share one mutex.
- **Symlinks:** not resolved, so a path through a symlink gets its own mutex.
- **Errors:** if the absolute path can't be built (the working directory can't be read), `Getlock` returns an error and the read or append fails without touching the file.
- **Limits:** locks only work within one process. One mutex is kept per file for the life of the process.

## Sequence numbers

Every record stored in a shard gets a `seq_n` field: its position in that shard, starting at 0.

- **Per shard:** each shard (`topic` + shard id, keyed as `"<topic>/<shard>"`) has its own counter, so `orders/0` and `orders/1` both start at 0.
- **Thread safe:** counters live in `GlobalShardSequences`, a mutex-guarded map. `ShardSequences.Next(key)` returns the current number and advances it under one lock, so concurrent callers never get the same number.
- **In file order:** `AppendToFile(filename, topic, shard, content)` takes the number only after taking the file lock. Records are therefore written to the file in `seq_n` order, even with concurrent writers.
- **Gaps:** an invalid payload (not a JSON object) doesn't use up a number. If opening or writing the file fails after the number was taken, that number is skipped.
- **Not persisted:** counters live in memory and restart at 0 when the process restarts.
- **Breaking change:** `AppendToFile` and `DecoratePayload` now take `topic` and `shard`, and `content` must be a JSON object. `AppendToFile` returns the `StoredRecord` (`Seq` and the stored `Payload`), and `DecoratePayload` also returns the `seq_n` it used.
