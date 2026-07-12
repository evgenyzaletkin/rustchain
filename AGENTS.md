# AGENTS.md

## Project Overview

This is a Rust blockchain and peer networking project. It runs multiple peers that accept signed client transactions, group them into blocks, exchange peer messages, synchronize missing blocks, and use a pluggable consensus engine to decide when blocks should be proposed or committed.

The codebase is intentionally split by responsibility. Preserve those boundaries when making changes.

## Main Modules

- `src/peer.rs`: peer message handling and side effects. `Peer` validates incoming messages, applies transactions, caches validated Raft block payloads, commits or rolls back blocks, and delegates consensus decisions to `ConsensusEngine`.
- `src/peer/action_executor.rs`: executes `ConsensusAction`s returned by consensus. It owns side effects such as staging transactions/blocks, sending network messages, committing blocks, and rolling back blocks.
- `src/peer/messages.rs`: peer message types and message payload structures.
- `src/peer/consensus.rs`: consensus abstraction. `ConsensusEngine` accepts `ConsensusInput` and returns `ConsensusAction`.
- `src/peer/consensus/voting.rs`: current voting-based block approval logic.
- `src/peer/consensus/raft.rs`: Raft-mode leader election, heartbeats, leader tracking, client forwarding, log replication, and commit advancement.
- `src/peer/consensus/raft_log_store.rs`: Raft log storage abstraction with file-backed runtime storage and in-memory test storage.
- `src/peer_runtime.rs`: runtime wiring and orchestration. It builds the network, block keeper, synchronization service, consensus engine, signing key, server task, and async event loop.
- `src/network/`: peer transport abstractions and implementations.
- `src/network/discovery_client.rs`: discovery abstraction and HTTP discovery client.
- `src/bin/discovery.rs`: HTTP/in-memory discovery server.
- `src/synchronization.rs`: block synchronization for retrieving missing blocks.
- `src/storage.rs`: block persistence, block state, and mempool-to-block creation.
- `src/transactions.rs`: transaction model, signing, verification, and processing.
- `src/config.rs`: shared runtime defaults and environment variable names.

## Current Consensus Model

Consensus is isolated from peer side effects:

- Consensus receives events through `ConsensusInput`.
- Consensus returns requested effects through `ConsensusAction`.
- `Peer` and `ConsensusActionExecutor` are responsible for executing effects such as broadcasting messages, sending direct peer messages, staging accepted Raft blocks, committing blocks, rolling back blocks, and applying client transactions.

Supported modes:

- `raft`: (default) leader election, heartbeats, leader tracking, client transaction forwarding, bounded log replication, persisted Raft log entries, follower match indexes, and majority commit advancement.
- `voting`: block proposal and approval/rejection by peer votes.

Raft log replication is implemented, but it is still a first-pass implementation. Current limitations include: `current_term` and `voted_for` are not persisted, snapshots are not implemented, conflict optimization is simplified, and membership is still based on known peers rather than formal Raft configuration changes.

Raft-specific boundaries:

- Consensus must not perform network side effects.
- Raft consensus owns Raft log persistence through `RaftLogStorage`.
- `Peer` must not own or instantiate Raft log storage.
- `Peer` may validate received Raft block payloads before consensus, but must not stage them in `BlockKeeper` until consensus accepts the corresponding `RaftLogEntry`s and returns `ConsensusAction::StageRaftEntries`.
- `BlockFile::verify_block_vec` / `BlockFile::verify_block` validate signature, hash, and internal block content only.
- `BlockKeeper::block_can_be_added` owns the current-chain or staged-chain previous-hash check.

## Runtime Configuration

`peer_runner` uses `PeerConfig::from_env()` from `src/peer_runtime.rs`.

Relevant environment variables:

- `PEER_ID`: numeric peer id.
- `CONSENSUS_MODE`: `voting` or `raft`; defaults to `voting`.

Shared defaults live in `src/config.rs`.

## Development Guidelines

- Preserve behavior when refactoring unless the user explicitly asks for behavior changes.
- Keep changes small and testable.
- Prefer existing module boundaries over adding cross-module shortcuts.
- Keep consensus logic out of `Peer`; use `ConsensusInput` and `ConsensusAction`.
- Keep network/discovery concerns out of consensus.
- Keep block storage side effects in `Peer` / `ConsensusActionExecutor`. Raft log persistence is the exception and belongs to Raft consensus through `RaftLogStorage`.
- Do not stage Raft-replicated blocks before consensus validates and accepts the Raft log entries.
- Prefer focused unit tests around consensus behavior and peer message handling when changing those areas.
- Run `cargo test` after behavior changes.
- Use `rustfmt --edition 2024 --check ...` or format touched Rust files before finishing.

## Common Commands

```text
cargo test
rustfmt --edition 2024 --check src/lib.rs src/config.rs src/peer.rs src/peer/action_executor.rs src/peer/messages.rs src/peer/consensus.rs src/peer/consensus/raft.rs src/peer/consensus/raft_log_store.rs src/peer/consensus/voting.rs src/peer_runtime.rs tests/peer.rs
PEER_ID=1 CONSENSUS_MODE=raft cargo run --bin peer_runner
```
