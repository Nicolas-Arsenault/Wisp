# Clients

Two command-line clients in `clients/`. Both connect to `localhost:50051` by default; change it with `-addr`.

## Producer (`clients/producer`)

Reads one JSON object per line from stdin and publishes them over **one** `Publish` stream.

```sh
echo '{"temp":21}' | go run ./clients/producer -topic sensors
go run ./clients/producer -topic sensors -key-field device < events.jsonl
```

- **Shard:** `-key-field device` uses each line's `device` value as the partition key. The shard is computed on the client (see [records.md](records.md)). Without the flag, or without the field, every line has the same key and goes to the same shard.
- **Blank lines:** skipped.
- **Invalid lines:** a line the broker rejects (`INVALID_ARGUMENT`) is logged and skipped.
- **Other errors:** any other error stops the producer with exit code 1.
- **When it finishes:** logs how many events were stored. Ctrl+C stops it.

## Consumer (`clients/consumer`)

Joins a topic and prints the events of the shards the broker assigns to it, until Ctrl+C. When other consumers join or leave, it follows its new assignment: it starts reading new shards and stops reading the ones taken away.

```sh
go run ./clients/consumer -topic sensors -id a           # terminal 1: gets shards [0 1 2 3 4]
go run ./clients/consumer -topic sensors -id b           # terminal 2: a now gets [0 2 4], b gets [1 3]
go run ./clients/consumer -topic sensors -id c -latest   # only new events
```

Output, tab separated, on stdout (logs go to stderr, including each new assignment):

```
<topic>/<shard>/<event id>	<timestamp, RFC 3339 UTC, ms>	<payload>
sensors/0/0	2026-10-28T04:25:08.026Z	{"dev":"a","n":1}
```

- **`-id`:** must be unique in the topic. Defaults to `<hostname>-<pid>`.
- **`-offset` / `-latest`:** where to start reading each newly assigned shard (default: offset 0).
- **`-max-bytes`:** caps each batch the broker sends (0 = broker default, 1 MiB).
- **Exit 0:** on Ctrl+C, or when the broker ends the stream.
- **Exit 1:** for errors, e.g. a duplicate id, or an offset after the end of a shard.

## Tests

`clients/*/main_test.go` run `run` against a fake broker over an in-memory `bufconn` connection. The consumer tests check that it reads the assigned shards, stops reading a shard that is taken away, doesn't resubscribe to shards it keeps, and returns join, shard and write errors. Run with `go test -race ./clients/...`.
