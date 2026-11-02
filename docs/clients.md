# Clients

Two command-line clients in `clients/`. Both connect to `localhost:50051` by default; change it with `-addr`.

## Producer (`clients/producer`)

Reads one JSON object per line from stdin and publishes them, with one `Publish` stream per shard. A full shard doesn't hold up the acks of the others.

Lines waiting together are sent as one request, of at most 1024 lines or 256 KiB, so the broker stores them in one fsync. It doesn't wait for more lines: a lone line goes out at once, and requests only grow under load. Each request is written to a local spool file before it's sent, so its lines survive the producer or the broker going down.

```sh
echo '{"temp":21}' | go run ./clients/producer -topic sensors
go run ./clients/producer -topic sensors -key-field device < events.jsonl
```

- **Shard:** `-key-field device` uses each line's `device` value as the partition key. The shard is computed on the client (see [records.md](records.md)). Without the flag, or without the field, lines take turns over the shards.
- **Blank lines:** skipped.
- **Invalid lines:** a line that isn't a JSON object is logged and skipped, without being sent.
- **Spool:** files are in `-spool` (default: `wisp/spool` in the user's cache directory, e.g. `~/Library/Caches` or `~/.cache`), in a folder per topic. Each batch file holds about 1 MiB of requests, stored as protobuf. It's deleted once it's full and all its lines are acked, or when the run ends. Each file costs one fsync, which is why they aren't smaller. A crash can leave acked lines in the file being written; they're resent and the broker drops them.
- **Producer ids:** a run locks a `<id>.producer` file and reuses that id, so there is one id per producer running at once, not one per run. The file holds the last acked sequence per shard, fsynced before a batch is deleted. A run continues after that sequence and after any leftover batch, so a new line can't reuse a sequence and be dropped as a duplicate. Never delete a `.producer` file on its own: delete the topic's whole spool folder, or the same id restarts at sequence 1.
- **After a crash:** a batch file whose producer id isn't locked was left by a run that ended early. At startup the producer sends those first, with their original producer id and sequences, so the broker drops what it already stored. They count in the stored total.
- **Broker down at startup:** the first run for a topic waits for the broker to learn the shard count, then saves it in the spool. Later runs start right away and save lines while the broker is down. They stop reading stdin at 1024 saved requests per shard, which bounds the spool.
- **Retries:** after `UNAVAILABLE`, `INTERNAL`, `FAILED_PRECONDITION` or `RESOURCE_EXHAUSTED`, or a lost connection, the producer tries again from the first line not acked. The delay starts at 200 ms and doubles up to 30 s, and resets after progress. There is no limit on attempts, because the lines are safe on disk; Ctrl+C stops it, and the next run sends them.
- **Limits:** the spool isn't fsynced, for speed, so it survives the process dying but not a power cut. Up to 1024 lines per shard can be read from stdin but not yet saved, and are lost if the process dies; this only builds up under load. Lines not yet read from stdin aren't in the spool.
- **Other errors:** any other error stops the producer with exit code 1. Lines not acked stay in the spool.
- **When it finishes:** logs how many events were stored. Ctrl+C stops it.

## Consumer (`clients/consumer`)

Joins a group of a topic and prints the events of the shards the broker assigns to it, until Ctrl+C. When other consumers of the group join or leave, it follows its new assignment: it starts reading new shards and stops reading the ones taken away.

Every second, and when it stops, it commits what it has printed to the broker, so the group resumes there, even if another consumer reads the shard next. A record can be printed twice (crash or rebalance before a commit, up to about a second of records), but isn't skipped. Output is buffered, and is always flushed before a commit.

```sh
go run ./clients/consumer -topic sensors -id a                # terminal 1: gets shards [0 1 2 3 4]
go run ./clients/consumer -topic sensors -id b                # terminal 2: a now gets [0 1 2], b gets [3 4]
go run ./clients/consumer -topic sensors -id c -group audit   # its own copy of the whole topic
```

Output, tab separated, on stdout (logs go to stderr, including each new assignment):

```
<topic>/<shard>/<event id>	<timestamp, RFC 3339 UTC, ms>	<payload>
sensors/0/0	2026-10-28T04:25:08.026Z	{"dev":"a","n":1}
```

- **`-id`:** must be unique in the group. Defaults to `<hostname>-<pid>`.
- **`-group`:** who shares the shards (default: `default`). Each group reads the whole topic.
- **`-offset` / `-latest`:** where to start a shard the group has no stored offset for (default: offset 0). They're ignored once the group has one, so to read from the start again, use a new group.
- **`-max-bytes`:** caps each batch the broker sends (0 = broker default, 1 MiB).
- **Exit 0:** on Ctrl+C, or when the broker ends the stream.
- **Exit 1:** for errors, e.g. a duplicate id, or an offset after the end of a shard.

## Tests

`clients/*/main_test.go` run `run` against a fake broker over an in-memory `bufconn` connection. The consumer tests check that it reads the assigned shards, stops reading a shard that is taken away, doesn't resubscribe to shards it keeps, commits on a timer and when stopping, never commits lines not yet flushed, continues after a refused commit, and returns join, shard and write errors. The producer tests check round robin, one stream per shard, a stuck shard not blocking the others, retries and backoff, spool files appearing before a send and going after the ack, resending what an earlier run left, request grouping, ids and sequences carrying over between runs, starting with the broker down, skipping ids in use, and torn lines. Run with `go test -race ./clients/...`.
