# CI

GitHub Actions workflow: `.github/workflows/ci.yml`.

## When it runs

- **Pushes to `main`**.
- **Pull requests**, against any branch.
- **Manually**, with "Run workflow" on the Actions tab.

Pushes to feature branches don't trigger it on their own; open a PR to get CI. A newer push to the same branch or PR cancels the run still in progress.

## What it checks

One job on `ubuntu-latest`, with a 15-minute limit. Steps run in order and stop at the first failure.

| Step | Command | Fails when |
|---|---|---|
| Formatting | `gofmt -l .` | any file isn't gofmt-formatted (they're listed) |
| Tidy modules | `go mod tidy -diff` | `go.mod`/`go.sum` don't match the imports. Fix with `go mod tidy` |
| Checksums | `go mod verify` | downloaded modules don't match `go.sum` |
| Build | `go build ./...` | any package doesn't compile |
| Vet | `go vet ./...` | `go vet` reports a problem |
| Tests | `go test -race -count=1 ./...` | any test fails or a data race is detected |

The tests cover every package: the broker (`main_test.go`), `clients/producer`, `clients/consumer`, `shards` and `stream`. They need no network or running broker; gRPC tests use an in-memory `bufconn` connection and write to temp directories.

## Versions

- **Go:** the version comes from `go.mod` (`go-version-file`), so CI and local builds match. To upgrade, change `go.mod`.
- **Actions:** `actions/checkout@v7` and `actions/setup-go@v7`. setup-go caches modules, keyed on `go.sum`.
- **Permissions:** read-only (`contents: read`).

## Running the same checks locally

```sh
gofmt -l . && go mod tidy -diff && go mod verify && go build ./... && go vet ./... && go test -race -count=1 ./...
```

## Not checked yet

- **Generated proto code:** CI doesn't check that `proto/*.pb.go` matches `event.proto`, because that would need `protoc` in CI. Regenerate after editing the proto.
- **Platforms:** only Linux is tested.
