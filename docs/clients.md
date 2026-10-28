# Clients

Two simple command-line clients for the broker, in `clients/`. Both connect to `localhost:50051` by default; change it with `-addr`.

## Producer (`clients/producer`)

Reads events from stdin, **one JSON object per line**, and publishes each one to `-topic`, in order.

```sh
echo '{"temp":21}' | go run ./clients/producer -topic sensors
go run ./clients/producer -topic sensors < events.jsonl
```

- **Blank lines:** skipped.
- **Invalid lines:** a line the broker rejects (`InvalidArgument`, i.e. not a JSON object) is logged and skipped, and the following lines are still sent.
- **Other errors:** any other error (broker down, write failure on the broker) stops the producer with exit code 1, because the remaining lines would fail too.
- **When it finishes:** logs how many events were published, then exits 0. Ctrl+C stops it early.

## Consumer (`clients/consumer`)

Subscribes to `-topic` and prints every event published from then on, one per line, until Ctrl+C.

```sh
go run ./clients/consumer -topic sensors
```

Output on stdout is tab separated; logs go to stderr, so stdout can be piped:

```
<event_id>	<timestamp, RFC 3339 UTC, ms>	<payload as stored>
sensors/0/0	2026-10-28T01:49:48.421Z	{"seq_n":0,"temp":21}
```

- **Exit 0:** on Ctrl+C, or when the broker ends the stream.
- **Exit 1:** if the broker can't be reached, the connection is lost, or stdout can't be written.
- **No history:** only events published *after* subscribing are shown; older events aren't replayed yet (see [broker.md](broker.md)).

## Code layout

Each client keeps its logic in a `run(ctx, client, topic, …)` function that takes the gRPC client and an `io.Reader`/`io.Writer`. `main` only parses flags and connects. This keeps `run` testable without a network or a terminal.

## Tests

`clients/*/main_test.go` run `run` against a **fake broker** over an in-memory `bufconn` connection. The real broker can't be used there, because it lives in package `main` at the repo root, and Go can't import a `main` package.

- **Producer:** lines published in order to the right topic; blank lines skipped; empty input; invalid line skipped while the rest continues; broker error stops it and later lines aren't sent; cancelled context.
- **Consumer:** output format; events printed in order and the right topic subscribed; Ctrl+C (cancelled context) is a clean exit; broker error returned; stdout write error returned.

Run with `go test -race ./clients/...`.

For a manual end-to-end check against the real broker, run `go run .`, then a consumer, then a producer in three terminals. The broker writes shards to `../data`, relative to the directory you start it from.
