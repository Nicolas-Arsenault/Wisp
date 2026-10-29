# Broker

Def: server that producers publish to and consumers read from.

## gRPC server (`main.go`)

Listens on `:50051` (`go run .`) and stores data in `../data`, relative to where it's started. At startup it opens every shard already on disk.

### `DescribeTopic(topic)`

Returns the topic's number of shards and creates the topic if needed. Producers call it once so they can hash keys to shards.

### `Publish` (bidirectional stream)

One long-lived stream per producer.

- **Request:** `{request_id, topic, shard, payload}`. The shard is computed by the producer.
- **Writing:** each request is put on that shard's queue, in the order received.
- **Ack:** each request gets one ack, in request order, once the record is on disk: `{request_id, shard, event_id}`.
- **Errors:** a bad request gets an ack with `error_code` set, and the stream stays open.

  | Error | Cause |
  |---|---|
  | `INVALID_ARGUMENT` | not a JSON object, bad topic name, or shard out of range |
  | `RESOURCE_EXHAUSTED` | the shard's index is full |
  | `UNAVAILABLE` | the broker is shutting down |
  | `INTERNAL` | the write failed |
- **Backpressure:** if a shard queue is full, or 256 acks are waiting, the broker stops reading the stream until there is room.

### `Subscribe(topic, shard, start_id, latest, max_bytes)`

Streams **one shard**.

- **History, then live:** first every stored record from `start_id`, then each new record as soon as it's stored, until the consumer leaves.
- **`latest`:** starts at the end of the shard, so only new records are sent.
- **Batches:** records come in `EventBatch` messages of at most `max_bytes` of payload. The default is 1 MiB and the cap is about 4 MiB (gRPC's default message limit). A batch always holds at least one record.
- **Slow consumers:** records are read from disk, so a slow consumer only falls behind; it never loses records.
- **Errors:**

  | Error | Cause |
  |---|---|
  | `NOT_FOUND` | unknown topic |
  | `INVALID_ARGUMENT` | bad shard |
  | `OUT_OF_RANGE` | `start_id` after the end of the shard |
  | `UNAVAILABLE` | the broker is shutting down |

### `Join(topic, consumer_id)` (server stream)

Consumers don't choose their shards: they join the topic, and the broker splits the shards between them (code: `groups.go`).

1. **Join:** consumer A joins topic `t`. The broker registers it and sends `Assignment{shards: [0 1 2 3 4]}`.
2. **Another consumer joins:** B joins. The broker splits the shards again and sends an update to both: A gets `[0 2 4]` and B gets `[1 3]`.
3. **A consumer leaves:** when B leaves, A gets `[0 1 2 3 4]` again.

- **Split:** consumers are kept in join order, and shard `i` goes to consumer `i % n`.
- **Generation:** each assignment has a `generation` that increases with every rebalance of the topic.
- **Membership:** a consumer is a member while its `Join` stream is open.
  - It leaves when the stream ends (Ctrl+C, closed connection).
  - A connection that dies silently is detected by gRPC keepalive pings (every 10 s, 5 s timeout).
- **Reading:** the consumer reads each assigned shard with `Subscribe`.
- **Errors:**

  | Error | Cause |
  |---|---|
  | `INVALID_ARGUMENT` | empty `consumer_id` or bad topic name |
  | `ALREADY_EXISTS` | the id is already in the topic |

  A topic that doesn't exist is created.
- **Limit:** offsets aren't stored. When a shard moves to another consumer, that consumer starts at its own `-offset` / `-latest`, not where the previous owner stopped. It can re-read records.

### Shutdown

On Ctrl+C or SIGTERM, the broker:

1. Disconnects every client.
2. Waits for the handlers to return.
3. Closes the shards. Records already queued are still stored.

## Breaking changes

- **RPCs:** `PublishEvent` and `SubscribeTopic` are replaced by `DescribeTopic`, `Publish` and `Subscribe`.
- **Event id:** now a number per shard (it was the string `topic/shard/seq_n`).
- **Payloads:** no longer contain `seq_n`.
- **On-disk format:** new, and old data isn't supported.

## Tests (`main_test.go`)

These run the real broker over an in-memory connection, with its data in a temp dir:

- **Publish:** acks and error acks.
- **Subscribe:** replay then live, `latest`, `max_bytes` batching, and errors.
- **Join:** the A/B/C rebalance flow, every shard having exactly one owner, and errors.

Run with `go test -race .`.
