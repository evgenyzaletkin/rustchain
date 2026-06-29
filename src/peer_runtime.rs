use crate::config::{
    CONSENSUS_MODE_ENV_VAR, DEFAULT_BASE_PORT, DEFAULT_CHANNEL_SIZE,
    DEFAULT_CONSENSUS_TICK_INTERVAL, DEFAULT_LOCAL_HOST, DEFAULT_MEMPOOL_SIZE,
    DEFAULT_PATH_TO_BLOCKS, DEFAULT_SYNC_INTERVAL, PEER_ID_ENV_VAR,
};
use crate::crypto::KeyManager;
use crate::network::NetworkInterface;
use crate::network::rest_network::RestNetwork;
use crate::peer::consensus::raft_log_store::FileRaftLogStore;
use crate::peer::consensus::{ConsensusEngine, ConsensusInput};
use crate::peer::{Peer, PeerId};
use crate::server;
use crate::storage::{BlockKeeper, BlockStorageView};
use crate::synchronization::Synchronization;
use log::info;
use std::future::pending;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use time::Interval;
use tokio::sync::mpsc;
use tokio::time;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsensusMode {
    Voting,
    Raft,
}

impl FromStr for ConsensusMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "voting" => Ok(Self::Voting),
            "raft" => Ok(Self::Raft),
            _ => Err(format!(
                "{CONSENSUS_MODE_ENV_VAR} must be either 'voting' or 'raft'"
            )),
        }
    }
}

pub struct PeerConfig {
    pub peer_id: PeerId,
    pub listening_addr: SocketAddr,
    pub channel_size: usize,
    pub consensus_mode: ConsensusMode,
}

impl PeerConfig {
    pub fn from_env() -> Result<PeerConfig, String> {
        let peer_id: PeerId = std::env::var(PEER_ID_ENV_VAR)
            .map_err(|_| "PEER_ID must be specified")?
            .parse()
            .map_err(|_| "Peer Id must be a number")?;
        let consensus_mode = std::env::var(CONSENSUS_MODE_ENV_VAR)
            .unwrap_or_else(|_| "raft".to_string())
            .parse()?;

        let port_offset: u16 = peer_id.try_into()?;
        let listening_addr =
            SocketAddr::from((DEFAULT_LOCAL_HOST, DEFAULT_BASE_PORT + port_offset));

        Ok(Self {
            peer_id,
            listening_addr,
            channel_size: DEFAULT_CHANNEL_SIZE,
            consensus_mode,
        })
    }
}

pub async fn run_peer(peer_config: PeerConfig) -> Result<(), String> {
    let (sender, receiver) = mpsc::channel(peer_config.channel_size);
    let network = Arc::new(RestNetwork::new(peer_config.peer_id, sender));
    let peer_dir =
        PathBuf::from(DEFAULT_PATH_TO_BLOCKS).join(format!("peer_{}", peer_config.peer_id));
    let block_keeper = BlockKeeper::new(peer_dir.clone(), DEFAULT_MEMPOOL_SIZE);
    let view = create_block_storage_view(&block_keeper);
    let signing_key = KeyManager::get_or_create_key(&peer_dir);
    let mut synchronization = (peer_config.consensus_mode == ConsensusMode::Voting)
        .then(|| Synchronization::new(network.clone()));
    let consensus = match peer_config.consensus_mode {
        ConsensusMode::Voting => ConsensusEngine::new_voting(peer_config.peer_id),
        ConsensusMode::Raft => {
            create_raft_consensus(peer_config.peer_id, &peer_dir, &block_keeper)?
        }
    };
    let requires_consensus_tick = consensus.requires_tick();
    let mut peer = Peer::new(
        peer_config.peer_id,
        network.clone(),
        consensus,
        block_keeper,
        signing_key,
    );

    let network_for_server = network.clone();
    let network_for_transport = network.clone();
    let listening_addr = peer_config.listening_addr;
    info!(
        "Starting peer {} on {} with {:?} consensus",
        peer_config.peer_id, peer_config.listening_addr, peer_config.consensus_mode
    );
    tokio::spawn(async move {
        server::run_server::<RestNetwork>(network_for_server, view, listening_addr).await;
    });
    tokio::spawn(async move {
        let res = network_for_transport.run(listening_addr).await;
        info!("Network stopped with result {:?}", res);
    });

    network.wait_for_readiness().await;
    let mut receiver = receiver;
    let mut sync_interval = synchronization
        .as_ref()
        .map(|_| time::interval(DEFAULT_SYNC_INTERVAL));
    let mut consensus_interval =
        requires_consensus_tick.then(|| time::interval(DEFAULT_CONSENSUS_TICK_INTERVAL));
    loop {
        tokio::select! {
            message = receiver.recv() => {
                let message = message.ok_or_else(|| "Channel is closed".to_string())?;
                peer.handle_message(message);
            },
            _ = next_interval_tick(&mut sync_interval) => {
                if let Some(synchronization) = synchronization.as_mut() {
                    synchronization
                        .check_and_retrieve_missing_blocks(peer.block_keeper_mut())
                        .await;
                }
            },
            _ = next_interval_tick(&mut consensus_interval) => {
                peer.handle_consensus_input(ConsensusInput::Tick {
                    now: Instant::now(),
                    known_peers: network.known_peers(),
                })?;
            }
        }
    }
}

async fn next_interval_tick(interval: &mut Option<Interval>) {
    match interval {
        Some(interval) => {
            interval.tick().await;
        }
        None => pending().await,
    }
}

fn create_block_storage_view(block_keeper: &BlockKeeper) -> BlockStorageView {
    block_keeper.create_block_storage_view()
}

fn create_raft_consensus(
    peer_id: PeerId,
    peer_dir: &PathBuf,
    block_keeper: &BlockKeeper,
) -> Result<ConsensusEngine, String> {
    let raft_log_store = FileRaftLogStore::new(peer_dir);
    let commit_index = block_keeper
        .get_block_storage_state()
        .read()
        .map_err(|e| format!("Failed to read block storage state: {}", e))?
        .block_height as u64;
    ConsensusEngine::new_raft_with_storage(peer_id, Box::new(raft_log_store), commit_index)
}

#[cfg(test)]
mod tests {
    use super::ConsensusMode;
    use std::str::FromStr;

    #[test]
    fn parses_consensus_mode() {
        assert_eq!(ConsensusMode::from_str("voting"), Ok(ConsensusMode::Voting));
        assert_eq!(ConsensusMode::from_str("raft"), Ok(ConsensusMode::Raft));
        assert_eq!(ConsensusMode::from_str("RAFT"), Ok(ConsensusMode::Raft));
    }

    #[test]
    fn rejects_unknown_consensus_mode() {
        assert!(ConsensusMode::from_str("pow").is_err());
    }
}
