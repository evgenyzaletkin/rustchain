use crate::crypto::KeyManager;
use crate::network::NetworkInterface;
use crate::storage;
use crate::storage::{BlockHash, BlockKeeper, BlockStatus, BlockStorageView};
use crate::synchronization::Synchronization;
use crate::transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};
use derive_more::{Constructor, Display, From};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::time;

#[derive(
    Clone, Eq, PartialEq, Hash, Copy, Debug, Display, From, Constructor, Serialize, Deserialize,
)]
pub struct PeerId {
    id: u32,
}

pub type TxPayload = Vec<u8>;

#[derive(Display, Clone, Serialize, Deserialize)]
pub enum MessageBody {
    // Ping,
    // Pong,
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
    BlockReject { block_hash: BlockHash },
    #[display("BlockApproved")]
    BlockApproved {
        // TODO add singature and public key
        block_hash: BlockHash,
    },
}

#[derive(Display, Serialize, Deserialize, Clone)]
#[display("{from} -> {to}: {body} ")]
pub struct Message {
    pub from: PeerId,
    pub to: PeerId,
    pub body: MessageBody,
}

enum ConsensusResult {
    InProgress,
    Approved,
    Rejected,
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

pub struct Peer<N: NetworkInterface> {
    pub id: PeerId,
    receiver: Receiver<Message>,
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    votings: HashMap<BlockHash, Consensus>,
    signing_key: SigningKey,
    public_key: VerifyingKey,
    network: Arc<N>,
    synchronization: Synchronization<N>,
    last_completed_block: BlockHash,
}

impl<N: NetworkInterface> Peer<N> {
    const RECV_TIMEOUT: Duration = Duration::from_millis(100);

    pub fn new(id: u32, receiver: Receiver<Message>, network: Arc<N>) -> Peer<N> {
        let peer_dir = PathBuf::from(storage::DEFAULT_PATH_TO_BLOCKS).join(format!("peer_{}", id));
        let block_keeper = BlockKeeper::new(peer_dir.clone(), storage::DEFAULT_MEMPOOL_SIZE);
        Self::create_with_storage(id, receiver, peer_dir, block_keeper, network)
    }

    pub fn create_with_storage(
        id: u32,
        receiver: Receiver<Message>,
        peer_dir: PathBuf,
        block_keeper: BlockKeeper,
        network: Arc<N>,
    ) -> Peer<N> {
        let signing_key = KeyManager::get_or_create_key(&peer_dir);
        let public_key = VerifyingKey::from(signing_key.clone());
        let block_view = block_keeper.create_block_storage_view();
        Self {
            id: id.into(),
            receiver,
            transaction_processor: TransactionProcessor::default(),
            signing_key,
            public_key,
            block_keeper,
            votings: HashMap::new(),
            network: network.clone(),
            synchronization: Synchronization::new(network, block_view),
            last_completed_block: storage::EMPTY_HASH,
        }
    }

    pub fn create_block_storage_view(&self) -> BlockStorageView {
        self.block_keeper.create_block_storage_view()
    }

    fn process_message(&mut self) -> bool {
        let result = self.receiver.try_recv();
        match result {
            Ok(message) => {
                self.handle_message(message);
                true
            }
            Err(TryRecvError::Empty) => false,
            Err(TryRecvError::Disconnected) => panic!("Channel disconnected"),
        }
    }

    fn handle_message(&mut self, message: Message) {
        debug!("Received message: {message}");
        if let Err(e) = match message.body {
            MessageBody::ClientTransaction(client_tx) => self.process_client_transaction(client_tx),
            MessageBody::Synchronization(verified_tx) => self.synchronize_transaction(verified_tx),
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

    fn process_client_transaction(&mut self, client_tx: SignedTransaction) -> Result<(), String> {
        client_tx.verify()?;

        self.transaction_processor
            .process_transaction(client_tx.clone());
        let status = self.block_keeper.add_transaction(client_tx.clone());

        let verified_tx = VerifiedTransaction::new(client_tx, &self.signing_key);
        self.broadcast_transaction(&verified_tx)?;

        if let BlockStatus::NewBlockCreated { block_hash } = status {
            self.broadcast_block_proposal(block_hash)?
        }
        Ok(())
    }

    fn synchronize_transaction(&mut self, verified_tx: VerifiedTransaction) -> Result<(), String> {
        // Verify both client and peer signatures
        verified_tx.verify()?;

        let client_tx = verified_tx.client_tx;
        if let BlockStatus::NewBlockCreated { block_hash } =
            self.block_keeper.add_transaction(client_tx.clone())
        {
            self.broadcast_block_proposal(block_hash)?
        }
        Ok(())
    }

    fn broadcast_transaction(&mut self, verified_tx: &VerifiedTransaction) -> Result<(), String> {
        self.network
            .broadcast_peer_message(&MessageBody::Synchronization(verified_tx.clone()), self.id);
        Ok(())
    }

    fn broadcast_block_proposal(&mut self, block_hash: BlockHash) -> Result<(), String> {
        let known_peers = self.network.known_peers();
        if !known_peers.is_empty() {
            if let Some(block_file) = self.block_keeper.get_uncommited_block(&block_hash) {
                let block_as_bytes =
                    serde_json::to_vec(&block_file).expect("Failed to serialize block file");
                let signature: Signature = self.signing_key.sign(&block_as_bytes);
                self.network.broadcast_peer_message(
                    &MessageBody::BlockProposal {
                        block_hash,
                        block_file: block_as_bytes,
                        signature,
                        public_key: self.public_key,
                    },
                    self.id,
                )
            }
            let mut cons = Consensus::new(self.id, &known_peers);
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
    ) -> Result<(), String> {
        if self.last_completed_block == block_hash {
            return Ok(());
        }

        let verification_result = self.block_keeper.verify_block_vec(
            block_hash.clone(),
            &block_file,
            signature,
            public_key,
        );
        let mut is_ok = false;
        if let Ok(block_file) = verification_result {
            is_ok = true;
            if self.block_keeper.block_can_be_added(&block_file) {
                self.block_keeper
                    .add_external_block(block_file)
                    .map_err(|e| e.to_string())?;
            }
            // Probably, we should call synchronization here in else block if the height
            // is less than block_index - 1
        }
        let current_peer = self.id.clone();
        self.make_vote(block_hash.clone(), from, true)?;
        self.make_vote(block_hash.clone(), current_peer, is_ok)
    }

    pub async fn run(&mut self) {
        self.network.wait_for_readiness().await;
        loop {
            tokio::select! {
                _ = self.synchronization.tick() => {
                    self.synchronization.check_and_retrieve_missing_blocks(&mut self.block_keeper).await;
                }
                _ = time::sleep(Self::RECV_TIMEOUT) => {
                    loop {
                        if !self.process_message() {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn process_block_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        if self.last_completed_block == block_hash {
            return Ok(());
        } else if self
            .block_keeper
            .get_uncommited_block(&block_hash)
            .is_none()
        {
            return Err(format!("Block ${block_hash} is not found"));
        }
        self.make_vote(block_hash.clone(), from, approve)
    }

    pub fn make_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        let cons = self.get_consensus(block_hash);
        if !cons.already_voted(&from) {
            match cons.make_vote(from, approve) {
                ConsensusResult::Approved => {
                    debug!("Approved block {}", block_hash);
                    self.block_keeper.commit_block(&block_hash)?;
                    self.last_completed_block = block_hash;
                }
                ConsensusResult::Rejected => {
                    debug!("Rejected block {}", block_hash);
                    self.block_keeper.rollback_block(&block_hash)?;
                    self.last_completed_block = block_hash;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn get_consensus(&mut self, block_hash: BlockHash) -> &mut Consensus {
        self.votings
            .entry(block_hash)
            .or_insert_with(|| Consensus::new(self.id, &self.network.known_peers().clone()))
    }
}

#[cfg(test)]
mod tests {
    use crate::crypto::KeyManager;
    use crate::network::NetworkInterface;
    use crate::network::local_network::LocalNetwork;
    use crate::peer::{Message, MessageBody, Peer, PeerId};
    use crate::storage::BlockKeeper;
    use crate::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};
    use k256::ecdsa::SigningKey;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    const TEST_DATA_PATH: &str = "target/test/data";

    #[test]
    fn test_block_voting_between_2_peers() {
        let (sender1, receiver1) = mpsc::channel(1000);
        let (sender2, receiver2) = mpsc::channel(1000);
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        let peer_2_dir = PathBuf::from(TEST_DATA_PATH).join("peer_2");
        recreate_dir(&peer_1_dir);
        recreate_dir(&peer_2_dir);

        let mut network = LocalNetwork::default();
        let peer_id_1 = PeerId::from(1);
        let peer_id_2 = PeerId::from(2);
        network.add_peer(peer_id_1, sender1);
        network.add_peer(peer_id_2, sender2);

        let network = Arc::new(network);

        let mut peer1 = Peer::<LocalNetwork>::create_with_storage(
            1,
            receiver1,
            peer_1_dir.clone(),
            BlockKeeper::new(peer_1_dir.clone(), 1),
            network.clone(),
        );
        let mut peer2 = Peer::<LocalNetwork>::create_with_storage(
            2,
            receiver2,
            peer_2_dir.clone(),
            BlockKeeper::new(peer_2_dir.clone(), 1),
            network.clone(),
        );

        let client_key = KeyManager::create_key();

        let client_msg = Message {
            from: PeerId::from(0),
            to: peer1.id.clone(),
            body: MessageBody::ClientTransaction(create_test_transaction(&client_key)),
        };
        network.send_peer_message(client_msg);

        let mut should_process = true;

        while should_process {
            if peer1.process_message() {
            } else if peer2.process_message() {
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
