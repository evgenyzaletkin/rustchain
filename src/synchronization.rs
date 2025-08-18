use crate::network::{NetworkInterface, NetworkMessage};
use crate::peer::PeerId;
use crate::storage::{BlockFile, BlockKeeper, BlockStorageState};
use crate::synchronization::SyncState::{FAIL, SUCCESS};
use log::{debug, error, trace};
use rand::Rng;
use rand::rngs::ThreadRng;
use rand::seq::IndexedRandom;
use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tokio::time::Interval;

const SYNC_INTERVAL: Duration = Duration::from_secs(20);

pub enum SyncState {
    SUCCESS,
    FAIL,
}

pub struct Synchronization<N: NetworkInterface> {
    network: Arc<N>,
    rng: ThreadRng,
}

struct PeersStates {
    states: HashMap<PeerId, BlockStorageState>,
}

impl PeersStates {
    fn new(states: HashMap<PeerId, BlockStorageState>) -> Self {
        Self { states }
    }

    fn get_peers_for_index(&self, index: u32) -> Vec<PeerId> {
        self.states
            .iter()
            .filter(|(_, state)| state.block_height >= index)
            .map(|(peer_id, _)| *peer_id)
            .collect()
    }

    fn get_max_index(&self) -> u32 {
        self.states
            .values()
            .map(|state| state.block_height)
            .max()
            .unwrap_or(0)
    }
}

impl<N: NetworkInterface> Synchronization<N> {
    pub fn new(network: Arc<N>) -> Self {
        Self {
            network,
            rng: rand::rng(),
        }
    }

    pub async fn create_interval(&mut self) -> Interval {
        time::interval(SYNC_INTERVAL)
    }

    pub async fn check_and_retrieve_missing_blocks(
        &mut self,
        block_keeper: &mut BlockKeeper,
    ) -> SyncState {
        debug!("Checking other peers for new blocks");
        let latest_states = self.get_latest_indexes().await;

        let peer_height = block_keeper
            .get_block_storage_state()
            .read()
            .unwrap()
            .block_height;
        let peers_states = PeersStates::new(latest_states);

        debug!(
            "known peers: {:?}, Peer's height: {:?}, max network height: {:?}",
            self.network.known_peers(),
            peer_height,
            peers_states.get_max_index()
        );

        if peer_height == peers_states.get_max_index() {
            debug!("All peers have the same height, no need to sync");
            return SUCCESS;
        }
        let mut idx = peer_height + 1;
        let mut errors_count = 0;
        loop {
            if idx > peers_states.get_max_index() {
                return SUCCESS;
            }
            if errors_count == 3 {
                return FAIL;
            }
            if let Some(block_file) = self.get_block_file(idx, &peers_states).await {
                let block_hash = block_file.hash.clone();
                debug!("Adding block {} from peer", idx);
                block_keeper.add_external_block(block_file).unwrap();
                block_keeper.commit_block(&block_hash).unwrap();
                debug!("Block {} added and commited", idx);
                idx += 1;
                continue;
            }
            errors_count += 1;
            error!("Failed to get block {idx} from other peers, errors: {errors_count}");
        }
    }

    async fn get_block_file(&mut self, idx: u32, peers_states: &PeersStates) -> Option<BlockFile> {
        trace!("Getting block file for index {}", idx);
        let peers = self.network.known_peers();
        let number_of_peers = min(peers.len(), 3);
        let peers_to_request: Vec<PeerId> = peers_states
            .get_peers_for_index(idx)
            .choose_multiple(&mut self.rng, number_of_peers)
            .cloned()
            .collect();
        trace!("Requesting block file from peers: {:?}", peers_to_request);
        let results: Vec<BlockStorageState> = self
            .network
            .send_and_wait_for_all(NetworkMessage::GetBlockState(idx), &peers_to_request)
            .await
            .into_values()
            .filter(|state| state.is_ok())
            .map(|state| state.unwrap())
            .collect();
        let all_match = results
            .windows(2)
            .all(|w| w[0].block_height == w[1].block_height);
        trace!("Received block file states from peers: {:?}", results);
        if results.len() == number_of_peers && all_match {
            let random_peer = &peers_to_request[self.rng.random_range(0..number_of_peers)];
            debug!("Synchronizing block {} with peer {}", idx, random_peer);
            let res: Result<BlockFile, String> = self
                .network
                .send_and_wait(random_peer.clone(), NetworkMessage::GetBlock(idx))
                .await;
            let result = res.ok();
            result
        } else {
            None
        }
    }

    async fn get_latest_indexes(&mut self) -> HashMap<PeerId, BlockStorageState> {
        let mut results = HashMap::new();
        for (peer_id, res) in self
            .network
            .send_and_wait_for_all::<BlockStorageState>(
                NetworkMessage::GetLatestBlockState,
                &self.network.known_peers(),
            )
            .await
        {
            if res.is_ok() {
                results.insert(peer_id, res.unwrap());
            }
        }
        results
    }
}
