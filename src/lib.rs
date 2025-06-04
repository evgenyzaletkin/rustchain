use crate::network::Network;
use crate::storage::{BlockHash, BlockStatus, KeyManager};
use derive_more::with_trait::From;
use derive_more::{Constructor, Display};
use k256::ecdsa::signature::{Signer, Verifier};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use storage::BlockKeeper;
use transactions::{Transaction, TransactionProcessor};

pub mod crypto;
pub mod network;
pub mod storage;
pub mod transactions;

#[derive(Clone, Eq, PartialEq, Hash, Copy, Debug, Display, From, Constructor)]
pub struct PeerId {
    id: u32,
}

pub type TxPayload = Vec<u8>;

#[derive(Display, Clone)]
pub enum MessageBody {
    Ping,
    Pong,
    #[display("Transaction")]
    ClientTransaction(Transaction),
    #[display("Transaction")]
    Synchronization {
        // for broadcasting
        transaction: TxPayload,
        signature: Signature,
        public_key: VerifyingKey,
    },
    #[display("BlockProposal")]
    BlockProposal {
        // for broadcasting
        block_hash: BlockHash,
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
    },
    #[display("BlockReject")]
    BlockReject {
        block_hash: BlockHash,
    },
    #[display("BlockReject")]
    BlockApproved {
        // TODO add singature and public key
        block_hash: BlockHash,
    },
}

#[derive(Display)]
#[display("{from} -> {to} ")]
pub struct Message {
    from: PeerId,
    to: PeerId,
    body: MessageBody,
}

struct Consensus {
    participants: HashSet<PeerId>,
    approvals: HashSet<PeerId>,
    rejections: HashSet<PeerId>,
}

impl Consensus {
    fn new(peer_id: PeerId, known_peers: &Vec<PeerId>) -> Consensus {
        let mut participants: HashSet<PeerId> = HashSet::from_iter(known_peers.clone());
        participants.insert(peer_id);
        let mut approvals = HashSet::with_capacity(participants.len());
        approvals.insert(peer_id);
        Consensus {
            approvals,
            rejections: HashSet::with_capacity(participants.len()),
            participants,
        }
    }

    fn make_vote(&mut self, peer_id: PeerId, approve: bool) -> ConsensusResult {
        if (self.participants.contains(&peer_id)) {
            if approve {
                self.approvals.insert(peer_id);
            } else {
                self.rejections.insert(peer_id);
            }
            let total_peers = self.participants.len();
            let f = (total_peers - 1) / 3;
            if self.approvals.len() >= 2 * f + 1 {
                return ConsensusResult::Approved;
            } else if self.rejections.len() >= f {
                return ConsensusResult::Rejected;
            }
        }
        ConsensusResult::InProgress
    }
}

enum ConsensusResult {
    InProgress,
    Approved,
    Rejected,
}

pub struct Peer {
    pub id: PeerId,
    known_peers: Vec<PeerId>,
    last_ping_times: HashMap<PeerId, Instant>,
    last_response_times: HashMap<PeerId, Instant>,
    receiver: Receiver<Message>,
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    votings: HashMap<BlockHash, Consensus>,
    signing_key: SigningKey,
    public_key: VerifyingKey,
}

impl Peer {
    const PING_INTERVAL: Duration = Duration::from_secs(10);
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);

    pub fn new(id: u32, receiver: Receiver<Message>) -> Peer {
        let peer_dir = PathBuf::from(storage::DEFAULT_PATH_TO_BLOCKS).join(format!("peer_{}", id));
        let signing_key = KeyManager::get_or_create_key(&peer_dir);
        let public_key = VerifyingKey::from(signing_key.clone());
        Peer {
            id: id.into(),
            known_peers: Vec::new(),
            last_response_times: HashMap::new(),
            last_ping_times: HashMap::new(),
            receiver,
            transaction_processor: TransactionProcessor::default(),
            signing_key,
            public_key,
            block_keeper: BlockKeeper::new(peer_dir, storage::DEFAULT_MEMPOOL_SIZE),
            votings: HashMap::new(),
        }
    }

    pub fn create_with_storage(
        id: u32,
        receiver: Receiver<Message>,
        peer_dir: PathBuf,
        block_keeper: BlockKeeper,
    ) -> Peer {
        let signing_key = KeyManager::get_or_create_key(&peer_dir);
        let public_key = VerifyingKey::from(signing_key.clone());
        Peer {
            id: id.into(),
            known_peers: Vec::new(),
            last_response_times: HashMap::new(),
            last_ping_times: HashMap::new(),
            receiver,
            transaction_processor: TransactionProcessor::default(),
            signing_key,
            public_key,
            block_keeper,
            votings: HashMap::new(),
        }
    }

    pub fn connect_with_peer(&mut self, peer: PeerId) {
        self.known_peers.push(peer);
    }

    pub fn send_ping(&self, to: PeerId, network: &Network) {
        network.send(Message {
            from: self.id.clone(),
            to,
            body: MessageBody::Ping,
        });
    }

    fn process_message(&mut self, network: &Network) -> bool {
        let result = self.receiver.recv_timeout(Self::RECV_TIMEOUT);
        match result {
            Ok(message) => Some(message),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("Channel disconnected"),
        }
        .map_or(false, |message| {
            self.handle_message(message, network);
            true
        })
    }

    fn handle_message(&mut self, message: Message, network: &Network) {
        println!("Received message: {message}");
        self.last_response_times
            .insert(message.from, Instant::now());
        if let Err(e) = match message.body {
            MessageBody::ClientTransaction(transaction) => {
                self.process_client_transaction(transaction, network)
            }
            MessageBody::Ping => {
                network.send(Message {
                    from: self.id,
                    to: message.from,
                    body: MessageBody::Pong,
                });
                Ok(())
            }
            MessageBody::Pong => Ok(()),
            MessageBody::Synchronization {
                transaction,
                signature,
                public_key,
            } => self.synchronize_transaction(
                message.from,
                transaction,
                signature,
                public_key,
                network,
            ),
            MessageBody::BlockProposal {
                block_hash,
                block_file,
                signature,
                public_key,
            } => {
                self.process_block_proposal(block_hash, block_file, signature, public_key, network)
            }
            MessageBody::BlockApproved { block_hash } => {
                Err("Block approved is not supported yet".to_string())
            }
            MessageBody::BlockReject { block_hash } => {
                Err("Block reject is not supported yet".to_string())
            }
        } {
            eprintln!("Failed to process message: {e}");
        }
    }

    fn process_client_transaction(
        &mut self,
        transaction: Transaction,
        network: &Network,
    ) -> Result<(), String> {
        self.transaction_processor
            .process_transaction(transaction.clone());
        let status = self.block_keeper.add_transaction(transaction.clone());
        self.broadcast_transaction(network, &transaction)?;
        if let BlockStatus::NewBlockCreated { block_hash } = status {
            self.broadcast_block_proposal(network, block_hash)?
        }
        Ok(())
    }

    fn synchronize_transaction(
        &mut self,
        from: PeerId,
        transaction_bytes: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
        network: &Network,
    ) -> Result<(), String> {
        match KeyManager::verify_message(&public_key, &signature, &transaction_bytes) {
            Ok(_) => {
                let Some(transaction) =
                    serde_json::from_slice::<Transaction>(&transaction_bytes).ok()
                else {
                    return Err("Failed to deserialize transaction".to_string());
                };

                if let BlockStatus::NewBlockCreated { block_hash } =
                    self.block_keeper.add_transaction(transaction.clone())
                {
                    self.broadcast_block_proposal(network, block_hash)?
                }
                // for now and for simplicity, we don't broadcast transaction processed by other peer
                // else {
                //     self.broadcast_transaction(network, &transaction);
                // }

                Ok(())
            }
            Err(e) => {
                // Add peer to blacklist
                Err(format!(
                    "Failed to verify message from peer {:?}: {:?}",
                    from, e
                ))
            }
        }
    }

    fn broadcast_transaction(
        &mut self,
        network: &Network,
        transaction: &Transaction,
    ) -> Result<(), String> {
        let transaction_bytes = serde_json::to_vec(&transaction)
            .map_err(|e| format!("Failed to serialize transaction: {}", e))?;
        let signature: Signature = self.signing_key.sign(&transaction_bytes);
        network.broadcast(
            &MessageBody::Synchronization {
                transaction: transaction_bytes,
                signature,
                public_key: self.public_key,
            },
            self.id,
            &self.known_peers,
        );
        Ok(())
    }

    fn broadcast_block_proposal(
        &mut self,
        network: &Network,
        block_hash: BlockHash,
    ) -> Result<(), String> {
        if (!self.known_peers.is_empty()) {
            if let Some(block_file) = self.block_keeper.get_uncommited_block(&block_hash) {
                let block_as_bytes =
                    serde_json::to_vec(&block_file).expect("Failed to serialize block file");
                let signature: Signature = self.signing_key.sign(&block_as_bytes);
                network.broadcast(
                    &MessageBody::BlockProposal {
                        block_hash,
                        block_file: block_as_bytes,
                        signature,
                        public_key: self.public_key,
                    },
                    self.id,
                    &self.known_peers,
                )
            }
            self.votings
                .insert(block_hash, Consensus::new(self.id, &self.known_peers));
            Ok(())
        } else {
            self.block_keeper.commit_block(&block_hash)
        }
    }

    fn process_block_proposal(
        &mut self,
        block_hash: BlockHash,
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
        network: &Network,
    ) -> Result<(), String> {
        // Verification Failed - send vote rejected
        // Verification Succeeded and Result is Approved - commit block and send vote
        // Verification Succeeded and Result is already voted - do nothing
        // Verification Succeeded and Result is In Progress - send vote approved
        let verification_result = self
            .block_keeper
            .verify_block(block_hash.clone(), block_file, signature, public_key)
            .is_ok();
        let message: MessageBody = if verification_result {
            if let Some(consensus) = self.votings.get_mut(&block_hash) {
                if let ConsensusResult::Approved = consensus.make_vote(self.id, true) {
                    self.block_keeper.commit_block(&block_hash)?;
                }
                return Ok(());
            } else {
                self.votings.insert(
                    block_hash,
                    Consensus::new(self.id, &self.known_peers.clone()),
                );
                MessageBody::BlockApproved { block_hash }
            }
        } else {
            MessageBody::BlockReject { block_hash }
        };
        network.broadcast(&message, self.id, &self.known_peers);
        match self.update_consensus_and_get_result(
            &block_hash,
            &self.id.clone(),
            verification_result,
        ) {
            ConsensusResult::Approved => {
                self.block_keeper
                    .commit_block(&block_hash)
                    .expect("Failed to commit block");
            }
            ConsensusResult::Rejected => {
                //             TODO Logic to remove block from uncommited and restore transactions to mempool
            }
            ConsensusResult::InProgress => {
                //     Nothing to do here, just wait for more votes
            }
        }
        Ok(())
    }

    fn update_consensus_and_get_result(
        &mut self,
        block_hash: &BlockHash,
        voter_id: &PeerId,
        approve: bool,
    ) -> ConsensusResult {
        let mut cons = self
            .votings
            .entry(*block_hash)
            .or_insert_with_key(|_| Consensus::new(self.id, &self.known_peers.clone()));
        cons.make_vote(*voter_id, approve)
    }

    fn disconnect_dead_peers(&mut self) {
        self.known_peers.retain(|peer| {
            let last_ping_opt = self.last_ping_times.get(peer);
            let last_response_opt = self.last_response_times.get(peer);
            let retain = match (last_ping_opt, last_response_opt) {
                (None, _) => true,
                (Some(last_ping), None) => last_ping.elapsed() < Self::PING_INTERVAL,
                (Some(last_ping), Some(last_response)) => {
                    last_ping.elapsed() - last_response.elapsed() < Self::PING_INTERVAL
                }
            };
            if !retain {
                println!("{:?} is dead", peer);
            }
            retain
        });
    }

    fn send_ping_to_peers(&mut self, network: &Network) {
        for peer in &self.known_peers {
            let should_send_ping = match self.last_ping_times.get(peer) {
                Some(last_sent_time) => last_sent_time.elapsed() > Self::PING_INTERVAL,
                None => true,
            };
            if should_send_ping {
                self.send_ping(*peer, network);
                self.last_ping_times.insert(*peer, Instant::now());
            }
        }
    }

    pub fn run(&mut self, network: &Network) {
        loop {
            // Process any available messages
            while self.process_message(network) {}

            // Check for and disconnect dead peers
            self.disconnect_dead_peers();
            // Send pings only when needed (the method already has the timing logic)
            self.send_ping_to_peers(network);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::crypto::KeyManager;
    use crate::storage::BlockKeeper;
    use crate::transactions::{AssetType, Metadata, Operation, Transaction};
    use crate::{Message, MessageBody, Network, Peer, PeerId};
    use k256::ecdsa::{SigningKey, VerifyingKey};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::{SystemTime, UNIX_EPOCH};

    const TEST_DATA_PATH: &str = "target/test/data";

    #[test]
    fn simple_test() {
        let (sender1, receiver1) = mpsc::channel();
        let (sender2, receiver2) = mpsc::channel();
        let mut peer1 = Peer::new(1, receiver1);
        let mut peer2 = Peer::new(2, receiver2);
        peer1.connect_with_peer(peer2.id);
        peer2.connect_with_peer(peer1.id);
        let mut network = Network::default();
        network.add_peer(peer1.id, sender1);
        network.add_peer(peer2.id, sender2);

        peer1.send_ping(peer2.id, &network);
        assert!(peer2.process_message(&network));
        assert!(peer1.process_message(&network));
    }

    #[test]
    fn test_block_voting_between_2_peers() {
        let (sender1, receiver1) = mpsc::channel();
        let (sender2, receiver2) = mpsc::channel();
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        let peer_2_dir = PathBuf::from(TEST_DATA_PATH).join("peer_2");
        recreate_dir(&peer_1_dir);
        recreate_dir(&peer_2_dir);
        let mut peer1 = Peer::create_with_storage(
            1,
            receiver1,
            peer_1_dir.clone(),
            BlockKeeper::new(peer_1_dir.clone(), 1),
        );
        let mut peer2 = Peer::create_with_storage(
            2,
            receiver2,
            peer_2_dir.clone(),
            BlockKeeper::new(peer_2_dir.clone(), 1),
        );
        peer1.connect_with_peer(peer2.id);
        peer2.connect_with_peer(peer1.id);
        let mut network = Network::default();
        network.add_peer(peer1.id, sender1);
        network.add_peer(peer2.id, sender2);

        let operation = Operation::AddCoin {
            asset_type: AssetType::USDT,
            amount: 10,
        };
        let client_key = KeyManager::create_key();

        let client_msg = Message {
            from: PeerId::from(0),
            to: peer1.id.clone(),
            body: MessageBody::ClientTransaction(create_client_transaction(operation, &client_key)),
        };
        network.send(client_msg);
        assert!(
            peer1.process_message(&network),
            "Peer 1 failed to process client message"
        );
        assert!(
            peer2.process_message(&network),
            "Peer 2 failed to process block proposal message"
        );

        assert!(
            peer1.process_message(&network),
            "Peer 2 failed to process block approved message"
        );

        let commited_blocks = peer1.block_keeper.list_all_blocks();
        assert_eq!(1, commited_blocks.len());
    }

    fn create_client_transaction(operation: Operation, signing_key: &SigningKey) -> Transaction {
        let client_key = KeyManager::create_key();
        let public_key = VerifyingKey::from(&client_key);
        let operation = Operation::AddCoin {
            asset_type: AssetType::USDT,
            amount: 10,
        };
        let signature = KeyManager::sign_message(&client_key, &operation);
        Transaction {
            operation,
            signature,
            public_key,
            metadata: Metadata {
                timestamp_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                sequence_number: 1,
            },
        }
    }

    fn recreate_dir(path: &PathBuf) {
        fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("Failed to create directory");
    }
}
