# Broker

Def: Server that handles consumer subscription to a topic. Also handles producers pushing to a topic.

Supports actions such as:

- creating a new topic
- pushing to a topic
- ...

## gRPC server (`broker/server.go`)

`PublishEvent(topic, payload)` writes the payload to the topic's stream with `stream.WriteToStream` and returns `Success: true`.

Known issue: the error from `WriteToStream` is ignored, so a failed write still returns `Success: true`.

### Tests

`broker/server_test.go` covers two paths:

- **Direct call:** calls `PublishEvent` directly.
- **gRPC round trip:** runs a full round trip over an in-memory `bufconn` listener, with no real port.

Two details keep the tests isolated:

- **Temp working directory:** each test changes into a temp directory, because `stream` writes to `../data` relative to the working directory.
- **New topic per test:** each test uses a new topic, because the round-robin position lasts for the whole test binary.

Run with `go test -race ./broker/`.
