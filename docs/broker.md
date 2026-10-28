# Broker

Def: Server that handles consumer subscription to a topic. Also handles producers pushing to a topic.

Supports actions such as:

- creating a new topic
- pushing to a topic
- ...

## gRPC server (`main.go`)

The broker listens on port 50051 (`go run .`). It has two calls.

### `PublishEvent(topic, payload)`

1. **Write:** writes the payload to the topic's stream with `stream.WriteToStream`. That adds the shard's `seq_n`.
2. **Send:** sends the event *as stored* to every consumer currently subscribed to the topic, as an `EventResponse`:
   - **`event_id`:** `"<topic>/<shard>/<seq_n>"`, e.g. `orders/3/41`. It says exactly where the event is on disk. It's unique while the process runs; `seq_n` restarts at 0 on restart.
   - **`payload`:** the stored JSON, including `seq_n`.
   - **`timestamp`:** when the broker sent the event, in Unix milliseconds.
3. **Reply:** returns `Success: true`.

Errors, where nothing is stored and nothing is sent:

- **`codes.InvalidArgument`:** the payload isn't a JSON object.
- **`codes.Internal`:** the event couldn't be written to disk.

**Breaking change:** a failed write used to return `Success: true`. It now returns these errors.

### `SubscribeTopic(topic)`

Registers the caller as a consumer of `topic` and streams it every event published on that topic **from then on**. Older events aren't replayed yet. The consumer is removed when it disconnects or a send fails.

### How sending works

- **One buffer per consumer:** each consumer gets a channel that holds up to `subscriberBuffer` (64) events. `PublishEvent` only puts events on the channel; the consumer's own `SubscribeTopic` call is the only one that calls `Send`. gRPC doesn't allow two goroutines to `Send` on the same stream at once.
- **Slow consumers:** if a consumer's buffer is full, the event is **dropped for that consumer** and a warning is logged. Publishing is never slowed down by a consumer, and the event is still on disk. With `event_id` the consumer can see the gap.
- **Order:** events published one after another on a topic reach consumers in that order. Events published *at the same time* may arrive in a different order than their `seq_n`.
- **Logging:** `main.go` logs with `slog` (Debug for each event, Info for subscribe and unsubscribe, Warn for dropped or failed sends).

### Tests (`main_test.go`)

- **Publishing:** a direct call and a gRPC round trip; invalid payload → `InvalidArgument` with nothing written; write failure → `Internal`.
- **Subscribing:** two consumers of a topic both get every event in order, with the right `event_id`, payload and timestamp. A consumer of another topic gets only its own topic's events.
- **Cleanup:** cancelling a subscription removes the consumer.
- **Slow consumer:** a full buffer doesn't block `broadcast`, the event is dropped for that consumer, and other consumers still get it.

Isolation:

- **Temp working directory:** each test changes into a temp directory, because `stream` writes to `../data` relative to the working directory.
- **New topic per test:** each test uses a new topic, because the round-robin position and shard sequences last for the whole test binary.
- **Wait for registration:** tests wait until the broker has registered a consumer before publishing, because `SubscribeTopic` registers on the server side after the client call returns.
- **Deadline:** subscriptions have a 5 s deadline, so a missing event fails the test instead of hanging.

Run with `go test -race .`.
