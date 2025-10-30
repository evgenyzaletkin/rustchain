# Rustchain

A working, study-oriented blockchain project written in **Rust**, featuring a **leaderless consensus model**, **peer discovery**, and support for an **arbitrary number of peers**.  
This project was built to explore the Rust language, decentralized system design, and the internal mechanics of a minimal blockchain network.

---

## Overview

This blockchain operates without a central coordinator or elected leader.  
Every peer is equal, participates in consensus, and helps maintain the network.

When a new [**peer**](src/bin/peer_runner.rs) starts, it:

1. Registers itself with the [**discovery service**](src/bin/discovery.rs)
2. Requests a list of known nodes.
3. Becomes an active participant in the consensus process.


---

## Features

- **Leaderless consensus** — no single authority or coordinating node
- **Byzantine voting process** — peers participate in a tolerant, majority-based validation method resilient to faulty or malicious nodes
- **Peer discovery** — automatic registration and retrieval of active peers
- **Dynamic network size** — supports any number of nodes
- **Simple blockchain model** — blocks, transactions, validation rules
- **On-disk block storage** — persists the blockchain locally for durability and fast recovery, and automatically fetches any missing blocks from other peers

---

## Running the project

- Run the **discovery service** first. By default it starts at port `3000`. But it can be changed with `DISCOVERY_PORT` environment variable.
- Run the new **peer** instance. It should be assigned a unique `PEER_ID` environment variable and it will be listening on port `3000` + `PEER_ID`.