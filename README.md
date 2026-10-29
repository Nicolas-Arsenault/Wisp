# Wisp

[![CI](https://github.com/Nicolas-Arsenault/Wisp/actions/workflows/ci.yml/badge.svg)](https://github.com/Nicolas-Arsenault/Wisp/actions/workflows/ci.yml)

Wisp is a lightweight data streaming platform like AWS kinesis. The goal is for it to be compatible with AI connectors so that you can scan data in real time and fast and scale easily.


## Docs

- [Broker](docs/broker.md): gRPC API (`DescribeTopic`, `Publish`, `Subscribe`, `Join`).
- [Streams](docs/streams.md), [shards](docs/shards.md) and [records](docs/records.md): topics, the on-disk format, offsets and partition keys.
- [Clients](docs/clients.md): command-line producer and consumer.

## Quick start

```sh
go run .                                              # broker on :50051, writes shards to ../data
echo '{"temp":21}' | go run ./clients/producer -topic sensors   # in a 2nd terminal
go run ./clients/consumer -topic sensors -id a                  # prints its assigned shards from offset 0, then new events
```

CI runs formatting, module, build, vet and race-detector test checks on every PR and push to `main`; see [docs/ci.md](docs/ci.md). Issue and PR templates: [docs/contributing.md](docs/contributing.md).
