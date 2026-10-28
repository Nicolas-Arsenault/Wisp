# Shards

Def: one portion of a stream, an append-only log of records. Producers pick the shard by hashing a partition key (see [records.md](records.md)).

Code: `shards/`.

## Files

Each shard has two files in `<dataDir>/<topic>/`:

| File | What it holds |
|---|---|
| `<id>.log` | The records, one frame after the other. |
| `<id>.index` | Where records are in the `.log`: 16-byte entries `[8 B event id][8 B byte position]`, one per 4 KB of `.log` (sparse). |

A frame in the `.log` (big-endian):

```
[4 B body length][8 B event id][8 B timestamp ms][4 B CRC-32C][body = zlib(payload)]
```

- **Event id:** the record's position in the shard: 0, 1, 2, … It is the consumer's offset.
- **Timestamp:** when the record was stored (Unix ms).
- **CRC:** detects a torn or damaged frame.
- **Payload:** stored exactly as the producer sent it. Only checked to be a JSON object; no field is added.

## The index

- **Sparse:** the writer adds an entry each time the `.log` has grown by 4 KB (`indexInterval`) since the last entry. The first record always gets one.
- **Lookup:** a consumer asks for offset 350, and the index has entries for 0, 100 and 300. The broker:
  1. Binary-searches the index (mmapped) for the last entry ≤ 350, here 300.
  2. Reads the `.log` forward from 300's byte position, skipping 300 to 349 (at most about 4 KB).
  3. Starts sending from 350.
- **Fixed size:** a new `.index` is created at 1 Mi entries (16 MiB, sparse on disk), which covers about 4 GiB of `.log`. It **never grows**. When it's full, records that would need a new entry are refused with `ErrShardFull` (the producer gets `RESOURCE_EXHAUSTED`).
- **Current offset:** found at startup by reading the `.log` from the last index entry to the end, so ids continue after a restart. There is no in-memory sequence map.

## Writer and queue

- **One writer per shard:** a goroutine is the only one writing the shard's files, so there are no locks.
- **Bounded queue:** `Enqueue` puts records on a channel of 1024. When it's full, `Enqueue` blocks, which slows the producer down (backpressure).
- **Batching:** the writer takes everything waiting in the queue (up to 8 MiB), writes it in one write, calls `fsync` once, then updates the index.
- **Durability:** a record is acked only after its `fsync`. The index isn't fsynced; it can be rebuilt from the `.log`.
- **Readers:** `ReadFrom` and `Wait` never block the writer. They only see records up to the committed count, which the writer sets (atomically) after the write.

## Recovery

At startup, each shard checks its `.index` against its `.log`. The `.log` is the source of truth:

- **Missing index entries:** the `.log` is read from the last index entry to the end, and entries the index doesn't have yet (crash after the fsync) are added.
- **Torn record:** a torn frame at the end of the `.log` (crash during a write, never acked) is cut off.
- **Index that doesn't match:** if the index doesn't match the `.log`, it's rebuilt from the start of the `.log`.

## Limits

- Unix only (macOS, Linux), because of mmap.
- About 4 GiB of `.log` per shard (1 Mi index entries); no segments or growth yet.
- Old data from before this format isn't supported. Delete `../data` before upgrading.
