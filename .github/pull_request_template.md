## Summary

<!-- What does this PR do, in one or two sentences? -->

Closes #

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor (no behavior change)
- [ ] Docs only
- [ ] CI / tooling

## Changes

<!-- The main changes, per package if it helps (broker, clients, shards, stream, proto). -->

-

## Breaking changes

<!-- API signatures, proto messages, file format on disk, CLI flags. Write "None" if there are none. -->

None

## How it was tested

- [ ] `go test -race -count=1 ./...` passes
- [ ] New or changed behavior has tests
- [ ] Tried manually (describe below)

<!-- Manual steps, e.g. broker + consumer + producer run. -->

## Checklist

- [ ] `gofmt -l .` prints nothing and `go vet ./...` is clean
- [ ] `go mod tidy -diff` shows no changes
- [ ] Docs updated (`docs/`, README, doc comments)
- [ ] If `proto/event.proto` changed: `proto/*.pb.go` regenerated
