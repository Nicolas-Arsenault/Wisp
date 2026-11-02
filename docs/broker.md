# Broker

Def: server that producers publish to and consumers read from.

## gRPC server (`main.go`)

Listens on `:50051` (`go run .`) and stores data in `../data`, relative to where it's started. At startup it opens every shard already on disk.

### `DescribeTopic(topic)`

Returns the topic's number of shards and creates the topic if needed. Producers call it once so they can hash keys to shards.

### `Publish` (bidirectional stream)

A long-lived stream. The command-line producer opens one per shard, so a full shard only slows its own stream.

- **Request:** `{request_id, topic, shard, payloads, producer_id, first_sequence}`, a batch of records. The shard is computed by the producer.
- **Writing:** each request is put on that shard's queue, in the order received. Its records are stored together, in one fsync: all of them or none, except that a broker crash during the write can keep the first ones (see [shards.md](shards.md)). One invalid payload rejects the request.
- **Ack:** each request gets one ack, in request order, once its records are on disk: `{request_id, shard, first_event_id, count}`. `count` is how many records were added.
- **Errors:** a bad request gets an ack with `error_code` set, and the stream stays open.

  | Error | Cause |
  |---|---|
  | `INVALID_ARGUMENT` | not a JSON object, bad topic name, or shard out of range |
  | `RESOURCE_EXHAUSTED` | the shard's index can't grow, e.g. the disk is full |
  | `UNAVAILABLE` | the broker is shutting down |
  | `INTERNAL` | the write failed |
  | `ALREADY_EXISTS` | the record was stored by an earlier attempt; nothing was added |
  | `FAILED_PRECONDITION` | an earlier sequence is missing; resend from the first record not acked |
- **Duplicates:** with a `producer_id`, sequences count that producer's records on the shard: 1, 2, 3, …, and `first_sequence` is that of the first payload. A request that's already stored is dropped (`ALREADY_EXISTS`), a partly stored one adds only its new records, and one that leaves a gap is refused (`FAILED_PRECONDITION`), so a retry can't store a record twice or out of order. `producer_id` 0 turns this off. Use a new id whenever the numbering restarts: a reused id with sequences from 1 is dropped as duplicates. Each shard remembers its 1024 most recently active producers (see [shards.md](shards.md)).
- **Backpressure:** if a shard queue is full, or 256 requests are waiting for their ack, the broker stops reading the stream until there is room.

### `Subscribe(topic, shard, start_id, latest, max_bytes, group)`

Streams **one shard**.

- **History, then live:** first every stored record from `start_id`, then each new record as soon as it's stored, until the consumer leaves.
- **`latest`:** starts at the end of the shard, so only new records are sent.
- **`group`:** starts at the group's stored offset, ignoring `start_id` and `latest`. If it has none, the position from `start_id` or `latest` is stored as its offset, so a later owner of the shard doesn't skip what came after it. Without `group`, nothing is stored.
- **Batches:** records come in `EventBatch` messages of at most `max_bytes` of payload. The default is 1 MiB and the cap is about 4 MiB (gRPC's default message limit). A batch always holds at least one record.
- **Slow consumers:** records are read from disk, so a slow consumer only falls behind; it never loses records.
- **Errors:**

  | Error | Cause |
  |---|---|
  | `NOT_FOUND` | unknown topic |
  | `INVALID_ARGUMENT` | bad shard |
  | `OUT_OF_RANGE` | `start_id` after the end of the shard |
  | `UNAVAILABLE` | the broker is shutting down |

### `Join(topic, consumer_id, group)` (server stream)

Consumers don't choose their shards: they join a group of the topic, and the broker splits the shards between the group's consumers (code: `groups.go`).

An empty `group` means `default`. Each group reads the whole topic, so two apps in different groups each get every record.

1. **Join:** consumer A joins group `g` of topic `t`. The broker registers it and sends `Assignment{shards: [0 1 2 3 4]}`.
2. **Another consumer joins:** B joins. The broker sends an update to both: A gets `[0 1 2]` and B gets `[3 4]`.
3. **Another consumer joins:** C joins. A gets `[0 1]`, B keeps `[3 4]` and C gets `[2]`. Only one shard moved.
4. **A consumer leaves:** when a consumer leaves, its shards go to the others, who keep the ones they have.

- **Split:** each consumer gets `shards / consumers` shards, and the oldest get one more if it doesn't divide evenly. A consumer keeps its shards up to that share, and only the rest move. Consumers beyond the number of shards get none until another leaves.
- **Generation:** each assignment has a `generation` that increases with every rebalance of the group.
- **Membership:** a consumer is a member while its `Join` stream is open.
  - It leaves when the stream ends (Ctrl+C, closed connection).
  - A connection that dies silently is detected by gRPC keepalive pings (every 10 s, 5 s timeout).
- **Reading:** the consumer reads each assigned shard with `Subscribe`, passing its group.
- **Errors:**

  | Error | Cause |
  |---|---|
  | `INVALID_ARGUMENT` | empty `consumer_id`, bad topic name or bad group name |
  | `ALREADY_EXISTS` | the id is already in the group |

  A topic that doesn't exist is created.

### `Commit(topic, group, consumer_id, generation, shard, next_id)`

Stores `next_id`, the first event not yet processed, as the group's offset for the shard. The next `Subscribe` of the group starts there, also after a broker restart. Offsets are in `<dataDir>/<topic>/groups/<group>/<shard>.offset`.

- **Fencing:** only the shard's owner, using the group's current generation, can commit. This stops a consumer that lost a shard from overwriting the new owner's offset.
- **Delivery:** at-least-once. Consumers commit after processing, so a crash or rebalance re-reads what wasn't committed, but nothing is skipped. The command-line consumer commits once a second and when it stops.
- **Handoff:** the broker doesn't wait for the old owner to stop, so two consumers can briefly read the same shard. Only `Commit` is checked; `Subscribe` isn't.
- **Durability:** offset files aren't fsynced, to keep commits cheap. A power cut can lose a recent offset, which re-reads records. A damaged offset file is logged and treated as no offset.
- **Errors:**

  | Error | Cause |
  |---|---|
  | `NOT_FOUND` | unknown topic |
  | `INVALID_ARGUMENT` | bad shard |
  | `OUT_OF_RANGE` | `next_id` after the end of the shard |
  | `FAILED_PRECONDITION` | the caller doesn't own the shard in this generation |

### Shutdown

On Ctrl+C or SIGTERM, the broker:

1. Disconnects every client.
2. Waits for the handlers to return.
3. Closes the shards. Records already queued are still stored.

## Breaking changes

- **RPCs:** `PublishEvent` and `SubscribeTopic` are replaced by `DescribeTopic`, `Publish` and `Subscribe`.
- **Event id:** now a number per shard (it was the string `topic/shard/seq_n`).
- **Payloads:** no longer contain `seq_n`.
- **On-disk format:** new, and old data isn't supported. Frames now hold the producer id and sequence and are compressed with S2 instead of zlib, so data written before this change can't be read either. Delete `../data` before upgrading.
- **Publish:** requests hold many records (`payloads`, `first_sequence`), and acks have `first_event_id` and `count`.
- **Producers:** the command-line producer now opens one `Publish` stream per shard, sends lines without a key round robin, keeps unacked lines in a local spool, reuses producer ids between runs, and retries without limit.
- **Rebalancing:** shards are now kept where they are when possible, instead of going to consumer `i % n`.
- **Consumers:** they now resume from their group's stored offset, so `-offset` and `-latest` only apply to a shard the group has no offset for.

## Tests (`main_test.go`)

These run the real broker over an in-memory connection, with its data in a temp dir:

- **Publish:** acks, error acks, batches, and dropped retries.
- **Subscribe:** replay then live, `latest`, `max_bytes` batching, and errors.
- **Join:** the A/B/C rebalance flow, every shard having exactly one owner, shards staying put, independent groups, and errors.
- **Commit:** an offset following its shard to a new owner, a stored `latest` position, and fencing.

Run with `go test -race .`.
