# Compacted message erasure coding

**Status**: Implemented

## Summary

Compacted messages (level ≥ 1) are stored in an erasure-coded form across spools instead of full copies. The same `delivery_count` and `select_acceptors` logic chooses how many acceptors and which ones; a fixed parity map then decides whether to send a full copy (0..=1 acceptors) or shards (2+ acceptors). Each selected spool stores either one shard or the full message. Backfill returns full messages or shards; the device buffers shards by `MessageId` and decodes when it has enough data shards.

## Parity map (no config)

| Acceptors (count) | Parity shards | data_shards | Behaviour |
|-------------------|---------------|-------------|-----------|
| 0..=1 | 0 | — | Full copy to selected acceptors |
| 2..=5 | 1 | count − 1 | Reed–Solomon encode, one shard per acceptor |
| 6..=9 | 2 | count − 2 | Same, tolerate 2 failures |
| 10+ | 3 | count − 3 | Same, tolerate 3 failures |

## Flow

1. **Send (weave)**: `count = delivery_count(level, counts_len, num_acceptors)`, `selected = select_acceptors(..., count)`. Parity = `parity_for_count(count)`. If parity == 0, send full ciphertext to each of `selected`. If parity > 0, erasure-encode ciphertext into `count` shards (reed-solomon-erasure), send shard `i` to `selected[i]`.
2. **Store (spool)**: Single request `MessageRequest::Send { id, level, data: Bytes }`. If `level == 0` → store full message; if `level >= 1` → store shard. Same key `(sender, seq)`; value is `StoredMessageItem { level, data }` (level 0 = full ciphertext).
3. **Backfill (spool → weave)**: `get_messages_after` returns `Vec<(MessageId, StoredMessageItem)>`. Server sends `MessageResponse::Message { id, level: item.level, data: item.data }`.
4. **Receive (weave)**: Full messages (level 0) go to `handle_encrypted_message`. Shards (level ≥ 1) go into a buffer keyed by `MessageId`; when `collected_shards.len() >= data_shards`, decode and apply to app.

## Components

- **filament-core**: `MessageRequest::Send { id, level, data: Bytes }` (level 0 = full, ≥1 = shard); `MessageResponse::Message { id, level, data: Bytes }` (same convention). Payloads use `Bytes` throughout.
- **filament-weave**: `crate::erasure` (parity_for_count, encode, decode); group_actor uses it in `encrypt_and_send_to_acceptors` and in shard buffer on receive; acceptor_actor sends `AppMessage { id, level, data }` and forwards incoming `Message` as `AcceptorInbound::EncryptedMessage { id, level, data }`; group_actor branches on `level` (0 = full message, ≥1 = shard).
- **filament-spool**: Stored value is `StoredMessageItem { level, data }` (level 0 = full); single `store_message(group_id, id, level, data)`; `get_messages_after` returns `StoredMessageItem`; server builds `MessageResponse::Message { id, level: item.level, data: item.data }` for subscription/backfill.

## Library

[reed-solomon-erasure](https://docs.rs/reed-solomon-erasure/) 6.x, GF(2^8). Encode adds a 4-byte big-endian length prefix before splitting so decode can strip padding.

## See also

- [ARCHITECTURE.md](../ARCHITECTURE.md) — message routing and acceptor storage
- [MESSAGE-STORE-KEY-FORMAT.md](MESSAGE-STORE-KEY-FORMAT.md) — key/value format including shards
