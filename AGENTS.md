# AGENTS.md

## Project Overview

This is a Rust blockchain and peer networking project. It runs multiple peers that accept signed client transactions, group them into blocks, exchange peer messages, synchronize missing blocks, and use a pluggable consensus engine to decide when blocks should be proposed or committed.

The codebase is intentionally split by responsibility. Preserve those boundaries when making changes.

## Main Modules

- `src/peer.rs`: peer message handling and side effects. `Peer` validates incoming messages, applies transactions, commits or rolls back blocks, and delegates consensus decisions to `ConsensusEngine`.
- `src/peer_runtime.rs`: runtime wiring and orchestration. It builds the network, block keeper, synchronization service, consensus engine, signing key, server task, and async event loop.
- `src/consensus.rs`: consensus abstraction. `ConsensusEngine` accepts `ConsensusInput` and returns `ConsensusOutput`.
- `src/consensus/voting.rs`: current voting-based block approval logic.
- `src/consensus/raft.rs`: Raft-mode leader election, heartbeats, leader tracking, and client forwarding. This is not full Raft log replication yet.
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
- Consensus returns requested effects through `ConsensusOutput`.
- `Peer` is responsible for executing effects such as broadcasting messages, sending direct peer messages, committing blocks, rolling back blocks, and applying client transactions.

Supported modes:

- `voting`: block proposal and approval/rejection by peer votes.
- `raft`: leader election and heartbeats, with client transactions forwarded to the known leader.

Important limitation: Raft currently uses `RaftAppendEntries` as heartbeat/leader discovery only. Blocks are still propagated through the existing block proposal and synchronization paths. Do not assume full Raft log replication semantics are implemented.

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
- Keep consensus logic out of `Peer`; use `ConsensusInput` and `ConsensusOutput`.
- Keep network/discovery concerns out of consensus.
- Keep storage side effects in `Peer` or runtime-level orchestration unless deliberately extracting a new component.
- Prefer focused unit tests around consensus behavior and peer message handling when changing those areas.
- Run `cargo test` after behavior changes.
- Use `rustfmt --edition 2024 --check ...` or format touched Rust files before finishing.

## Common Commands

```text
cargo test
rustfmt --edition 2024 --check src/lib.rs src/config.rs src/consensus.rs src/consensus/raft.rs src/consensus/voting.rs src/peer.rs src/peer_runtime.rs tests/peer.rs
PEER_ID=1 CONSENSUS_MODE=raft cargo run --bin peer_runner
```
