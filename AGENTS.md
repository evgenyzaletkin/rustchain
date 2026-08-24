# AGENTS.md

## Project Overview

This is a Rust blockchain and peer networking project. It runs multiple peers that accept signed client transactions, group them into blocks, exchange peer messages, synchronize missing blocks, and use a pluggable consensus engine to decide when blocks should be proposed or committed.

The codebase is intentionally split by responsibility. Preserve those boundaries when making changes.

## Main Modules

- `src/peer.rs`: peer message routing and orchestration. `Peer` validates and stages payloads that require `PeerEffects` before consensus can decide anything (client transactions, synchronized transactions, block proposals), executes effects through `PeerEffects`, and publishes consensus state snapshots through a Tokio watch channel. `Peer` is intentionally unaware of consensus-mode details: every message that needs no pre-consensus validation (votes, Raft RPCs) is forwarded as-is through `ConsensusEngine::on_message`, so adding or changing a consensus mode's wire protocol never requires touching `peer.rs`.
- `src/peer/effects.rs`: owns peer-side services and executes `ConsensusEffect`s. It owns transaction processing, block storage, signing, network delivery, commits, and rollbacks.
- `src/peer/messages.rs`: peer message types and message payload structures.
- `src/peer/consensus.rs`: consensus abstraction. `ConsensusEngine` exposes typed event handlers that return `ConsensusEffect`s, plus `on_message` — the single dispatch entrypoint `Peer` uses for messages it doesn't need to pre-validate.
- `src/peer/consensus/voting.rs`: current voting-based block approval logic.
- `src/peer/consensus/raft.rs`: Raft state, typed event dispatch, and accepted pending block payloads.
- `src/peer/consensus/raft/election.rs`: Raft membership, elections, votes, and leader tracking.
- `src/peer/consensus/raft/replication.rs`: Raft heartbeats, log replication, persistence, and commit advancement.
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

- Consensus receives validated events through typed `ConsensusEngine::on_*` methods.
- Consensus returns requested effects through `ConsensusEffect`.
- `PeerEffects` executes effects such as broadcasting messages, sending direct peer messages, staging accepted Raft blocks, committing blocks, rolling back blocks, and applying client transactions.

Supported modes:

- `raft`: (default) leader election, heartbeats, leader tracking, client transaction forwarding, bounded log replication, persisted Raft log entries, follower match indexes, and majority commit advancement.
- `voting`: block proposal and approval/rejection by peer votes.

Raft log replication is implemented, but it is still a first-pass implementation. Current limitations include: `current_term` and `voted_for` are not persisted, snapshots are not implemented, conflict optimization is simplified, and membership is still based on known peers rather than formal Raft configuration changes.

Uncommitted block payloads are intentionally kept in memory in both Raft and voting/BFT-style consensus. Only committed blocks are durable. This is an accepted simplification: a crash may lose an accepted or proposed block payload even when related consensus metadata survives, so crash recovery of in-flight blocks is not guaranteed. Do not add durable pending-block storage or embed block payloads in the Raft log unless explicitly requested.

Raft-specific boundaries:

- Consensus must not perform network side effects.
- Raft consensus owns Raft log persistence through `RaftLogStorage`.
- `Peer` must not own or instantiate Raft log storage.
- Raft consensus validates received replicated block payloads (signature, hash, noop/payload consistency) itself, inside `RaftConsensus::on_append_entries`; `Peer` never inspects or retains them.
- Raft consensus owns accepted pending block payloads until `ConsensusEffect::StageRaftEntries` is executed; only then may the executor stage them in `BlockKeeper`.
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
- Keep consensus logic out of `Peer`; use typed `ConsensusEngine` handlers and `ConsensusEffect`.
- Keep network/discovery concerns out of consensus.
- Keep transaction, block storage, signing, and network side effects in `PeerEffects`. Raft log persistence is the exception and belongs to Raft consensus through `RaftLogStorage`.
- Do not stage Raft-replicated blocks before consensus validates and accepts the Raft log entries.
- Prefer focused unit tests around consensus behavior and peer message handling when changing those areas.
- Run `cargo test` after behavior changes.
- Use `rustfmt --edition 2024 --check ...` or format touched Rust files before finishing.

## Common Commands

```text
cargo test
rustfmt --edition 2024 --check src/lib.rs src/config.rs src/peer.rs src/peer/effects.rs src/peer/messages.rs src/peer/consensus.rs src/peer/consensus/raft.rs src/peer/consensus/raft/election.rs src/peer/consensus/raft/replication.rs src/peer/consensus/raft_log_store.rs src/peer/consensus/voting.rs src/peer_runtime.rs tests/peer.rs
PEER_ID=1 CONSENSUS_MODE=raft cargo run --bin peer_runner
```
