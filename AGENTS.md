# AGENTS.md

## Project Shape

This is a Rust blockchain and peer networking project. Core areas:

- `src/peer.rs`: peer runtime, message handling, transaction flow, and block proposal handling.
- `src/network/`: network transport implementations and network message types.
- `src/bin/discovery.rs`: current HTTP/in-memory discovery server.
- `src/synchronization.rs`: block synchronization across peers.
- `src/storage.rs`: block persistence and block state.
- `src/transactions.rs`: signed transaction handling.

## Architectural Direction

Prefer separating responsibilities as the project grows:

- Discovery should be abstracted behind a `DiscoveryClient` so implementations can later include HTTP, ZooKeeper, or another registry.
- Network transport should not permanently own discovery-specific logic.
- Consensus logic should move out of `Peer` into a dedicated module or set of modules.
- Raft should be added as a separate consensus mode/backend, not mixed into the existing voting consensus.

## Development Guidelines

- Preserve current behavior when refactoring.
- Prefer small, testable steps over broad rewrites.
- Keep peer orchestration, networking, discovery, synchronization, storage, and consensus boundaries clear.
- Use existing project patterns before introducing new abstractions.
- Add focused tests around consensus and discovery behavior when changing those areas.

## Near-Term Roadmap

1. Introduce a `DiscoveryClient` abstraction.
2. Implement current REST discovery as an HTTP discovery client.
3. Refactor existing voting logic into a consensus module.
4. Add consensus mode configuration.
5. Add Raft mode behind the consensus abstraction.
