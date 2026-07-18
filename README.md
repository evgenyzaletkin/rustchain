# Rustchain

Rustchain is a study-oriented blockchain and peer networking project written in Rust. Peers accept signed client transactions, create blocks, exchange messages, synchronize missing blocks, and use a pluggable consensus engine to decide when blocks are proposed or committed.

## Consensus Modes

- `raft` (default): elects a leader, forwards client transactions to it, replicates a persisted Raft log, and commits blocks after majority replication.
- `voting`: peers exchange transactions and approve or reject proposed blocks without an elected leader.

Raft replication is a first-pass implementation. Terms and votes are not persisted, snapshots are not implemented, conflict optimization is simplified, and membership follows discovery rather than formal Raft configuration changes.

## Features

- HTTP peer discovery with inactive-peer expiration
- Signed client transactions and signed transaction/block payloads exchanged by peers
- Mempool-based block creation and block validation
- Voting and Raft consensus engines isolated from peer side effects
- File-backed blocks, signing keys, and Raft logs
- Missing-block synchronization in voting mode
- Read-only peer state at `GET /peer/state`
- Docker Compose demo with isolated persistent storage per peer

## Run Locally

Requirements: a current Rust toolchain and Cargo.

Start discovery in one terminal:

```bash
cargo run --bin discovery
```

Start peers in separate terminals. A peer listens on port `3000 + PEER_ID`:

```bash
PEER_ID=1 CONSENSUS_MODE=raft cargo run --bin peer_runner
PEER_ID=2 CONSENSUS_MODE=raft cargo run --bin peer_runner
PEER_ID=3 CONSENSUS_MODE=raft cargo run --bin peer_runner
```

Use `CONSENSUS_MODE=voting` to run the leaderless voting engine. Runtime data is stored under `data/peer_<id>`.

Important environment variables:

| Variable | Purpose | Default |
|---|---|---|
| `PEER_ID` | Numeric peer identifier; required | none |
| `CONSENSUS_MODE` | `raft` or `voting` | `raft` |
| `DISCOVERY_HOST` | Discovery host used by peers | `127.0.0.1` |
| `DISCOVERY_PORT` | Discovery port | `3000` |
| `DISCOVERY_BIND_HOST` | Discovery bind address | `127.0.0.1` |
| `PEER_BIND_HOST` | Peer HTTP bind address | `127.0.0.1` |
| `PEER_ADVERTISE_HOST` | Host advertised through discovery | peer bind host |
| `MY_LOG_LEVEL` | Logging filter | trace with HTTP noise reduced |

## Docker Compose Demo

Requirements: Docker Compose, `curl`, and `jq`. Run all commands from the repository root.

Build the shared image once:

```bash
docker build -f docker/demo/Dockerfile -t rustchain-raft-demo:local .
```

The demo publishes discovery on port `3000` and peers 1-4 on ports `3001`-`3004`. Each peer has an independent named volume. Peer 4 is profile-gated so it can join after the initial cluster is established.

### Raft Demo

Start discovery and three fresh Raft peers:

```bash
docker compose --profile joiner -f docker/demo/compose.yaml down -v
docker compose -f docker/demo/compose.yaml up --no-build -d discovery peer1 peer2 peer3
```

Wait approximately 15 seconds for election, then inspect every peer:

```bash
for port in 3001 3002 3003; do
  curl -s "http://127.0.0.1:$port/peer/state" |
    jq '{peer_id, known_peers, block, consensus}'
done
```

Exactly one peer should report `role: "leader"`; the others should report `role: "follower"` with the same `leader_id` and term.

Submit five transactions across different peers. Five transactions are required to fill the default mempool and create one block:

```bash
for target in 3001:1 3002:2 3003:3 3001:4 3002:5; do
  port="${target%%:*}"
  seq="${target##*:}"
  timestamp=$(( $(date +%s) * 1000 + seq ))

  curl -s -X POST "http://127.0.0.1:$port/test/transactions" \
    -H 'content-type: application/json' \
    -H 'client_id: compose-demo' \
    -d "{\"operation\":{\"AddCoin\":{\"amount\":10,\"asset_type\":\"BTC\"}},\"metadata\":{\"timestamp_nanos\":$timestamp,\"sequence_number\":$seq}}"
  echo
done
```

Verify that all peers report height 1, commit index 1, log index 1, and the same block hash:

```bash
for port in 3001 3002 3003; do
  curl -s "http://127.0.0.1:$port/peer/state" |
    jq '{peer_id, height: .block.block_height, hash: .block.last_commited_hash, consensus}'
done
```

Add peer 4 and wait for the next heartbeat:

```bash
docker compose -f docker/demo/compose.yaml up --no-build -d peer4
sleep 6
curl -s http://127.0.0.1:3004/peer/state | jq
curl -s http://127.0.0.1:3004/block/1 | jq '{index, hash, transactions}'
```

Peer 4 should be a follower with the same leader, committed height, log index, commit index, and block hash.

To demonstrate leader failover, find and stop the current leader:

```bash
leader=$(
  for port in 3001 3002 3003 3004; do
    curl -s "http://127.0.0.1:$port/peer/state"
  done | jq -r 'select(.consensus.role == "leader") | "peer\(.peer_id)"'
)

echo "Stopping $leader"
docker compose -f docker/demo/compose.yaml stop "$leader"
```

After approximately 15 seconds, query the surviving peers and confirm that one has become leader in a newer term.

### Voting Demo

The voting demo uses a different Compose project name so its volumes do not overlap with the Raft demo. Stop the Raft containers first because both projects publish the same host ports:

```bash
docker compose --profile joiner -f docker/demo/compose.yaml down
CONSENSUS_MODE=voting docker compose -p rustchain-voting-demo \
  --profile joiner -f docker/demo/compose.yaml down -v
CONSENSUS_MODE=voting docker compose -p rustchain-voting-demo \
  -f docker/demo/compose.yaml up --no-build -d discovery peer1 peer2 peer3
```

Inspect the initial cluster:

```bash
for port in 3001 3002 3003; do
  curl -s "http://127.0.0.1:$port/peer/state" |
    jq '{peer_id, known_peers, block, consensus}'
done
```

All peers should report `mode: "voting"`; voting mode has no role or leader. Use the same five-transaction loop from the Raft demo, then verify that all peers report height 1 and the same block hash.

Add voting peer 4:

```bash
CONSENSUS_MODE=voting docker compose -p rustchain-voting-demo \
  -f docker/demo/compose.yaml up --no-build -d peer4
```

Voting-mode synchronization runs every 20 seconds. Poll peer 4 until it reports the same height and hash, then inspect the retrieved block:

```bash
until [ "$(curl -s http://127.0.0.1:3004/peer/state | jq -r '.block.block_height')" = "1" ]; do
  sleep 1
done

curl -s http://127.0.0.1:3004/peer/state | jq
curl -s http://127.0.0.1:3004/block/1 | jq '{index, hash, transactions}'
```

### Logs And Cleanup

Follow Raft project logs:

```bash
docker compose -f docker/demo/compose.yaml logs -f
```

Follow voting project logs:

```bash
CONSENSUS_MODE=voting docker compose -p rustchain-voting-demo \
  -f docker/demo/compose.yaml logs -f
```

Stop containers while preserving their named volumes by omitting `-v`. Include the `joiner` profile so peer 4 is also removed:

```bash
docker compose --profile joiner -f docker/demo/compose.yaml down
CONSENSUS_MODE=voting docker compose -p rustchain-voting-demo \
  --profile joiner -f docker/demo/compose.yaml down
```

Add `-v` to the corresponding `down` command to delete that demo's blocks, keys, Raft logs, and test-client key.

## HTTP Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/peer/state` | Peer ID, known peers, block state, and consensus state |
| `GET` | `/block/state/latest` | Latest committed block state |
| `GET` | `/block/state/{index}` | State derived from a specific block |
| `GET` | `/block/{index}` | Full block contents |
| `POST` | `/transactions` | Submit a signed transaction |
| `POST` | `/test/transactions` | Sign and submit a test transaction using `client_id` |

## Tests

```bash
cargo test
rustfmt --edition 2024 --check \
  src/lib.rs src/config.rs src/peer.rs src/peer/action_executor.rs \
  src/peer/messages.rs src/peer/consensus.rs src/peer/consensus/raft.rs \
  src/peer/consensus/raft_log_store.rs src/peer/consensus/voting.rs \
  src/peer_runtime.rs tests/peer.rs
```
