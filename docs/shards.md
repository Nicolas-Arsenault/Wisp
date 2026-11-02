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
[4 B body length][8 B event id][8 B timestamp ms][8 B producer id][8 B sequence][4 B CRC-32C][body = S2(payload)]
```

- **Event id:** the record's position in the shard: 0, 1, 2, … It is the consumer's offset.
- **Timestamp:** when the record was stored (Unix ms).
- **Producer id, sequence:** used to drop retried records. Producer id 0 means the record has none.
- **CRC:** detects a torn or damaged frame.
- **Payload:** stored exactly as the producer sent it. Only checked to be a JSON object; no field is added.

## The index

- **Sparse:** the writer adds an entry each time the `.log` has grown by 4 KB (`indexInterval`) since the last entry. The first record always gets one.
- **Lookup:** a consumer asks for offset 350, and the index has entries for 0, 100 and 300. The broker:
  1. Binary-searches the index (mmapped) for the last entry ≤ 350, here 300.
  2. Reads the `.log` forward from 300's byte position, skipping 300 to 349 (at most about 4 KB).
  3. Starts sending from 350.
- **Size:** a new `.index` has 1 Mi entries (16 MiB, sparse on disk), which covers about 4 GiB of `.log`. When it's full, the writer doubles the file and maps it again. Readers take no lock: they keep using the old mapping until they load the new one, and old mappings stay open until the shard closes. Records are only refused with `ErrShardFull` (the producer gets `RESOURCE_EXHAUSTED`) if the index can't grow, e.g. the disk is full.
- **Current offset:** found at startup by reading the `.log` from the last index entry to the end, so ids continue after a restart. There is no in-memory sequence map.

## Writer and queue

- **One writer per shard:** a goroutine is the only one writing the shard's files, so there are no locks.
- **Bounded queue:** `EnqueueBatch` puts requests on a channel of 1024. When it's full, it blocks, which slows the producer down (backpressure).
- **Batching:** a producer's request is one item on the queue, stored whole. The writer takes everything waiting in the queue (up to 8 MiB), writes it in one write, calls `fsync` once, then updates the index. This is what makes publishing fast: many records share each `fsync`.
- **Durability:** a record is acked only after its `fsync`. The index isn't fsynced; it can be rebuilt from the `.log`.
- **Duplicates:** for records with a producer id, the writer keeps each producer's last sequence in memory. A request whose records are all stored gets `ErrDuplicate`, a partly stored one keeps only its new records, and one that leaves a gap gets `ErrOutOfOrder`. A sequence only counts once its record is stored, so a failed write can be retried. The first sequence of a producer the shard doesn't remember is accepted, whatever it is. After a failed write, the shard remembers the producers it didn't know, so their later records wait for the resend.
- **Cursors:** `Read` takes a `Cursor` and returns the next one, which knows its position in the `.log`, so a consumer that keeps reading skips the index search. `ReadFrom` is `Read` from a new cursor.
- **Readers:** `Read`, `ReadFrom` and `Wait` never block the writer. They only see records up to the committed count, which the writer sets (atomically) after the write.

## Recovery

At startup, each shard checks its `.index` against its `.log`. The `.log` is the source of truth:

- **Missing index entries:** the `.log` is read from the last index entry to the end, and entries the index doesn't have yet (crash after the fsync) are added.
- **Torn record:** a torn frame at the end of the `.log` (crash during a write, never acked) is cut off.
- **Index that doesn't match:** if the index doesn't match the `.log`, it's rebuilt from the start of the `.log`.
- **Producer sequences:** the whole `.log` is read to find each producer's last sequence, so duplicates are still caught after a restart. A damaged record found here stops the shard from opening, and so the broker from starting.

## Limits

- Unix only (macOS, Linux), because of mmap.
- One `.log` per shard, with no segments or retention: a shard is limited by disk space, and old records are never deleted.
- Startup time grows with the size of the `.log`, because it's read in full.
- Each shard remembers its 1024 most recently active producers (`maxProducers`). A forgotten producer can carry on, but a retry of one of its stored records is no longer caught and is stored twice. This takes 1024 other active producers on the shard since its last record. Producer ids are reused between runs, so repeated runs from one machine count as one.
- Old data from before this format isn't supported. Delete `../data` before upgrading.
