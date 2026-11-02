# Record

Def: a single event inside a stream.

It contains:

- **Payload:** a JSON object, stored as sent.
- **Event id:** its position within its shard (0, 1, 2, …). Consumers use it as their offset. Full id: `<topic>/<shard>/<event id>`.
- **Timestamp:** when the broker stored it.
- **Producer id and sequence:** let the broker drop retried records (see [broker.md](broker.md)). Not sent to consumers.

## Partition key

The producer picks each record's shard itself. A record with a key goes to:

```
shard = FNV-1a-32(key) % num_shards
```

- **Code:** in `partition.Shard`. Other clients (e.g. a future SDK) must use the same function.
- **Ordering:** records with the same key from one producer go to the same shard, in the order they were sent. Records from different producers interleave in arrival order.
- **No key:** records take turns over the shards (round robin), so they're balanced but have no overall order.
- **Shard count:** comes from `DescribeTopic`.
