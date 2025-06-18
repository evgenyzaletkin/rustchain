use crate::network::Network;
use crate::storage::{BlockFile, BlockHash, BlockStatus, KeyManager};
use derive_more::with_trait::From;
use derive_more::{Constructor, Display};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::time::{Duration, Instant};
use storage::BlockKeeper;
use transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};

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
    #[display("ClientTransaction")]
    ClientTransaction(SignedTransaction),
    #[display("Synchronization")]
    Synchronization(VerifiedTransaction),
    #[display("BlockProposal")]
    BlockProposal {
        block_hash: BlockHash,
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
    },
    #[display("BlockReject")]
    BlockReject {
        block_hash: BlockHash,
    },
    #[display("BlockApproved")]
    BlockApproved {
        // TODO add singature and public key
        block_hash: BlockHash,
    },
}

#[derive(Display)]
#[display("{from} -> {to}: {body} ")]
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
        Consensus {
            approvals: HashSet::with_capacity(participants.len()),
            rejections: HashSet::with_capacity(participants.len()),
            participants,
        }
    }

    fn already_voted(&self, peer_id: &PeerId) -> bool {
        self.approvals.contains(peer_id) || self.rejections.contains(peer_id)
    }

    fn make_vote(&mut self, peer_id: PeerId, approve: bool) -> ConsensusResult {
        if self.participants.contains(&peer_id) {
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
    const RECV_TIMEOUT: Duration = Duration::from_millis(100);

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
            MessageBody::ClientTransaction(client_tx) => {
                self.process_client_transaction(client_tx, network)
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
            MessageBody::Synchronization(verified_tx) => {
                self.synchronize_transaction(verified_tx, network)
            }
            MessageBody::BlockProposal {
                block_hash,
                block_file,
                signature,
                public_key,
            } => self.process_block_proposal(
                block_hash,
                block_file,
                signature,
                public_key,
                message.from,
                network,
            ),
            MessageBody::BlockApproved { block_hash } => {
                self.process_block_vote(block_hash, message.from, true)
            }
            MessageBody::BlockReject { block_hash } => {
                self.process_block_vote(block_hash, message.from, false)
            }
        } {
            eprintln!("Failed to process message: {e}");
        }
    }

    fn process_client_transaction(
        &mut self,
        client_tx: SignedTransaction,
        network: &Network,
    ) -> Result<(), String> {
        client_tx.verify()?;

        self.transaction_processor
            .process_transaction(client_tx.clone());
        let status = self.block_keeper.add_transaction(client_tx.clone());

        let verified_tx = VerifiedTransaction::new(client_tx, &self.signing_key);
        self.broadcast_transaction(network, &verified_tx)?;

        if let BlockStatus::NewBlockCreated { block_hash } = status {
            self.broadcast_block_proposal(network, block_hash)?
        }
        Ok(())
    }

    fn synchronize_transaction(
        &mut self,
        verified_tx: VerifiedTransaction,
        network: &Network,
    ) -> Result<(), String> {
        // Verify both client and peer signatures
        verified_tx.verify()?;

        let client_tx = verified_tx.client_tx;
        if let BlockStatus::NewBlockCreated { block_hash } =
            self.block_keeper.add_transaction(client_tx.clone())
        {
            self.broadcast_block_proposal(network, block_hash)?
        }
        Ok(())
    }

    fn broadcast_transaction(
        &mut self,
        network: &Network,
        verified_tx: &VerifiedTransaction,
    ) -> Result<(), String> {
        network.broadcast(
            &MessageBody::Synchronization(verified_tx.clone()),
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
        if !self.known_peers.is_empty() {
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
            let mut cons = Consensus::new(self.id, &self.known_peers);
            cons.make_vote(self.id, true);
            self.votings.insert(block_hash, cons);
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
        from: PeerId,
        network: &Network,
    ) -> Result<(), String> {
        // Shall be replaced by pattern matching. If signature verification or hash calculation is
        // failed - no need to save the block file. If the previous hash is different -
        // save and wait for other votes, probably. Now, for simplicity, we just save the block file.
        let verification_result = self
            .block_keeper
            .verify_block(block_hash.clone(), &block_file, signature, public_key)
            .is_ok();
        if self
            .block_keeper
            .get_uncommited_block(&block_hash)
            .is_none()
        {
            self.block_keeper
                .add_block_from_proposal(BlockFile::from(&block_file))?;
        }
        let current_peer = self.id.clone();
        let cons = self.get_consensus(block_hash);
        cons.make_vote(from, true);
        let broadcast_result = cons.already_voted(&current_peer);
        match cons.make_vote(from, verification_result) {
            ConsensusResult::Approved => {
                self.block_keeper.commit_block(&block_hash)?;
            }
            ConsensusResult::Rejected => {
                self.block_keeper.rollback_block(&block_hash)?;
            }
            ConsensusResult::InProgress => {}
        };
        if broadcast_result {
            if verification_result {
                network.broadcast(
                    &MessageBody::BlockApproved { block_hash },
                    self.id,
                    &self.known_peers,
                );
            } else {
                network.broadcast(
                    &MessageBody::BlockReject { block_hash },
                    self.id,
                    &self.known_peers,
                );
            }
        }
        Ok(())
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

    fn process_block_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        if self
            .block_keeper
            .get_uncommited_block(&block_hash)
            .is_none()
        {
            return Err(format!("Block ${block_hash} is not found"));
        }
        let cons = self.get_consensus(block_hash);
        if !cons.already_voted(&from) {
            match cons.make_vote(from, approve) {
                ConsensusResult::Approved => {
                    self.block_keeper.commit_block(&block_hash)?;
                }
                ConsensusResult::Rejected => {
                    self.block_keeper.rollback_block(&block_hash)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn get_consensus(&mut self, block_hash: BlockHash) -> &mut Consensus {
        self.votings
            .entry(block_hash)
            .or_insert_with(|| Consensus::new(self.id, &self.known_peers.clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::crypto::KeyManager;
    use crate::storage::BlockKeeper;
    use crate::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};
    use crate::{Message, MessageBody, Network, Peer, PeerId};
    use k256::ecdsa::SigningKey;
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

        let client_key = KeyManager::create_key();

        let client_msg = Message {
            from: PeerId::from(0),
            to: peer1.id.clone(),
            body: MessageBody::ClientTransaction(create_test_transaction(&client_key)),
        };
        network.send(client_msg);

        let mut should_process = true;

        while should_process {
            if peer1.process_message(&network) {
            } else if peer2.process_message(&network) {
            } else {
                should_process = false;
            }
        }

        assert_eq!(1, peer1.block_keeper.list_all_blocks().len());
        assert_eq!(1, peer2.block_keeper.list_all_blocks().len());
    }

    fn create_test_transaction(signing_key: &SigningKey) -> SignedTransaction {
        let transaction = Transaction {
            operation: Operation::AddCoin {
                asset_type: AssetType::USDT,
                amount: 10,
            },
            metadata: Metadata {
                timestamp_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                sequence_number: 1,
            },
        };

        SignedTransaction::new(transaction, signing_key)
    }

    fn recreate_dir(path: &PathBuf) {
        fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("Failed to create directory");
    }
}
