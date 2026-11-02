# Durable agents

Def: a runtime that journals each step of an LLM agent's run to Wisp. If the process dies, the run resumes from its log without firing committed steps again.

Code: `durable/`.

## Journal

A run is a topic `run.<run_id>` with one shard. `run_id` must match `[A-Za-z0-9_-]+`.

- **Record:** one JSON object, one Wisp record, one `Publish` request. A request's records aren't atomic across a crash (see [shards.md](shards.md)), so a request never holds more than one.
- **Position:** `seq` counts from 1, and always equals the event id + 1.
- **Writer:** producer id 1, with `first_sequence = seq`, and one append in flight.
- **Reader:** `Subscribe` from 0 with `stop_at_end`. The position comes from the log, so consumer groups aren't used.
- **Size:** a record is at most 1 MiB (`MaxRecordBytes`).

## Records

Every record has `v`, `run`, `seq`, `session` and `kind`.

| `kind` | Other fields | Meaning |
|---|---|---|
| `run_started` | `agent`, `input`, `input_hash` | Always `seq` 1. |
| `session_opened` | | A process took over the run. Fences the one before it. |
| `step_started` | `step`, `type`, `name`, `input`, `input_hash`, `attempt`, `policy`, `idempotency_key` | Intent, durable before the effect fires. |
| `step_completed` | `step`, `attempt`, `output`, `duration_ms`, `resolved_by` | The commit point of a step. |
| `step_failed` | `step`, `attempt`, `error` | A failure, returned again on resume and replay. |
| `run_finished` | `status`, `output` or `error` | Nothing may follow it. |

```json
{"v":1,"run":"r1","seq":7,"session":"9f3a","kind":"step_started","step":3,"type":"tool","name":"refund",
 "input":{"order":42},"input_hash":"sha256:…","attempt":1,"policy":"idempotent","idempotency_key":"r1/3"}
```

- **`type`:** `llm`, `tool`, `observation` or `decision`.
- **`step`:** counts the agent's calls, from 1. `seq` is a position in the log, and one step takes at least two.
- **`session`:** random, one per process.
- **`idempotency_key`:** `<run_id>/<step>`, the same across attempts, so a retry can be recognized.
- **`input_hash`:** SHA-256 of canonical JSON, with sorted keys and numbers kept as written.

## Conditional append

A shard stores sequence `s` from a known producer only if its last one is `s-1`. With one producer per shard, that means each position can be filled once, by one writer. No broker feature was added for this: it's the duplicate check of [broker.md](broker.md).

The first sequence of an unknown producer is always accepted. That can only happen on an empty shard here: the shard has one producer, can't forget it, and finds it again after a restart.

| Result of `Publish` | Meaning | What the journal does |
|---|---|---|
| OK | Stored and fsynced | Checks `first_event_id == seq-1` and continues. |
| `ALREADY_EXISTS` | The position is taken | Reads it. Its own bytes: the ack was lost, so it continues. Anything else: `ErrFenced`. |
| `FAILED_PRECONDITION` | The log is shorter than it wrote | `ErrJournalGap`. Acked records were lost. |
| Stream error, `UNAVAILABLE`, `INTERNAL`, `RESOURCE_EXHAUSTED` | Outcome unknown | Reconnects and resends the same bytes for up to 30 s, then stops the run. |
| Anything else | A retry can't help | Stops the run. |

A resend can't store a record twice, even if the first copy is still queued in the broker.

## Reading a journal

`Fold` validates a whole journal and returns its state. It is the only code that decides whether a history is legal. It returns `ErrCorruptJournal`, and nothing runs, for:

- A record that isn't valid, has another version or run, or has an unknown field.
- A `seq` that doesn't follow the one before it, or doesn't match its event id.
- A record from a session that was fenced.
- A second open step, a skipped step or attempt, or an outcome for a step that isn't open.
- A record after `run_finished`, or `run_finished` with a step open.

So a record from another producer on a `run.*` topic is detected on read. Only the SDK should write to those topics.

## Tests

`durable/*_test.go` run against a fake log with the rules of a one-producer shard, with hooks to fail before storing, or to lose the ack after. They cover each row of the table above and each rule of `Fold`. Run with `go test -race ./durable/`.
