# Wisp

[![CI](https://github.com/Nicolas-Arsenault/Wisp/actions/workflows/ci.yml/badge.svg)](https://github.com/Nicolas-Arsenault/Wisp/actions/workflows/ci.yml)

Wisp is a lightweight data streaming platform like AWS kinesis. The goal is for it to be compatible with AI connectors so that you can scan data in real time and fast.


## Packages

- [`records`](records/README.md): record encoding, on-disk append, error and logging conventions.

## Quick start

```sh
go run .                                              # broker on :50051, writes shards to ../data
go run ./clients/consumer -topic sensors              # in a 2nd terminal: prints events
echo '{"temp":21}' | go run ./clients/producer -topic sensors   # in a 3rd terminal
```

See [docs/broker.md](docs/broker.md) and [docs/clients.md](docs/clients.md).

CI runs formatting, module, build, vet and race-detector test checks on every PR and push to `main`; see [docs/ci.md](docs/ci.md).
