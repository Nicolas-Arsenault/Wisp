# Shards

Def: One portion of a stream. Singular append-only logs that store the records. 

Records partition key determine which shard within a stream it ends up in.

## File locking

`AppendToFile` and `ReadFromFile` hold a mutex for each file while they run, so goroutines in the same process can't interleave a write with another write or a read.

- **Lock key:** the absolute, cleaned path (`filepath.Abs`). Different spellings of one file (`../data/t/0`, `data/t/../t/0`, an absolute path) share one mutex.
- **Symlinks:** not resolved, so a path through a symlink gets its own mutex.
- **Errors:** if the absolute path can't be built (the working directory can't be read), `Getlock` returns an error and the read or append fails without touching the file.
- **Limits:** locks only work within one process. One mutex is kept per file for the life of the process.
