# Clients

Two command-line clients in `clients/`. Both connect to `localhost:50051` by default; change it with `-addr`.

## Producer (`clients/producer`)

Reads one JSON object per line from stdin and publishes them, with one `Publish` stream per shard. A full shard doesn't hold up the acks of the others. Once a shard's buffer (1024 lines) is full, reading stdin waits for it.

```sh
echo '{"temp":21}' | go run ./clients/producer -topic sensors
go run ./clients/producer -topic sensors -key-field device < events.jsonl
```

- **Shard:** `-key-field device` uses each line's `device` value as the partition key. The shard is computed on the client (see [records.md](records.md)). Without the flag, or without the field, lines take turns over the shards.
- **Blank lines:** skipped.
- **Invalid lines:** a line that isn't a JSON object is logged and skipped, without being sent.
- **Retries:** after `UNAVAILABLE`, `INTERNAL` or `FAILED_PRECONDITION`, or a lost connection, the shard's stream is reopened and resends from the first line not acked. Each run has a random producer id, and the broker drops what it already stored, so a retry doesn't store a line twice. It gives up after 5 failures in a row, 200 ms apart.
- **Limit:** retries only cover one run. Lines not acked when the process exits aren't resent by the next run.
- **Other errors:** any other error stops the producer with exit code 1.
- **When it finishes:** logs how many events were stored. Ctrl+C stops it.

## Consumer (`clients/consumer`)

Joins a group of a topic and prints the events of the shards the broker assigns to it, until Ctrl+C. When other consumers of the group join or leave, it follows its new assignment: it starts reading new shards and stops reading the ones taken away.

After printing each batch it commits its offset to the broker, so the group resumes there, even if another consumer reads the shard next. A record can be printed twice (crash or rebalance before the commit), but isn't skipped.

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

`clients/*/main_test.go` run `run` against a fake broker over an in-memory `bufconn` connection. The consumer tests check that it reads the assigned shards, stops reading a shard that is taken away, doesn't resubscribe to shards it keeps, commits each batch after printing it, continues after a refused commit, and returns join, shard and write errors. The producer tests check round robin, one stream per shard, a stuck shard not blocking the others, and retries. Run with `go test -race ./clients/...`.
