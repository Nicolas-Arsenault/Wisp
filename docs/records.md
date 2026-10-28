# Record

Def: a single event inside a stream.

It contains:

- **Payload:** a JSON object, stored as sent.
- **Event id:** its position within its shard (0, 1, 2, …). Consumers use it as their offset. Full id: `<topic>/<shard>/<event id>`.
- **Timestamp:** when the broker stored it.

## Partition key

The producer picks each record's shard itself:

```
shard = FNV-1a-32(key) % num_shards
```

- **Code:** in `partition.Shard`. Other clients (e.g. a future SDK) must use the same function.
- **Ordering:** records with the same key always go to the same shard, in the order they were sent.
- **Shard count:** comes from `DescribeTopic`.
