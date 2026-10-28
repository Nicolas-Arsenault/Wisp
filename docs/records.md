# Record

Def: A record is a single event inside a stream. 

It contains:

- A JSON structure (payload)
- A partition key, which will be used for sharding
- A sequence number (`seq_n`), its position within its shard. See [shards.md](shards.md#sequence-numbers).

