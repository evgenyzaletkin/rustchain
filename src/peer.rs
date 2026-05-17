use crate::consensus::{ConsensusOutcome, VotingConsensus};
use crate::crypto::KeyManager;
use crate::network::NetworkInterface;
use crate::peer::MessageBody::{BlockApproved, BlockReject};
use crate::storage;
use crate::storage::{BlockHash, BlockKeeper, BlockStatus, BlockStorageView};
use crate::synchronization::Synchronization;
use crate::transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};
use derive_more::{Constructor, Display, From};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

#[derive(
    Clone, Eq, PartialEq, Hash, Copy, Debug, Display, From, Constructor, Serialize, Deserialize,
)]
pub struct PeerId(u32);

impl FromStr for PeerId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self).map_err(|e| e.to_string())
    }
}

impl TryInto<u16> for PeerId {
    type Error = String;

    fn try_into(self) -> Result<u16, Self::Error> {
        if self.0 > u16::MAX as u32 {
            Err(format!("PeerId is too big: {}", self.0))
        } else {
            Ok(self.0 as u16)
        }
    }
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
        // TODO add signature and public key
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

pub struct Peer<N: NetworkInterface> {
    pub id: PeerId,
    receiver: Receiver<Message>,
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    votings: HashMap<BlockHash, VotingConsensus>,
    signing_key: SigningKey,
    public_key: VerifyingKey,
    network: Arc<N>,
    synchronization: Synchronization<N>,
    last_completed_block: BlockHash,
}

impl<N: NetworkInterface> Peer<N> {
    pub fn new(id: PeerId, receiver: Receiver<Message>, network: Arc<N>) -> Peer<N> {
        let peer_dir = PathBuf::from(storage::DEFAULT_PATH_TO_BLOCKS).join(format!("peer_{}", id));
        let block_keeper = BlockKeeper::new(peer_dir.clone(), storage::DEFAULT_MEMPOOL_SIZE);
        Self::create_with_storage(id, receiver, peer_dir, block_keeper, network)
    }

    pub fn create_with_storage(
        id: PeerId,
        receiver: Receiver<Message>,
        peer_dir: PathBuf,
        block_keeper: BlockKeeper,
        network: Arc<N>,
    ) -> Peer<N> {
        let signing_key = KeyManager::get_or_create_key(&peer_dir);
        let public_key = VerifyingKey::from(signing_key.clone());
        Self {
            id,
            receiver,
            transaction_processor: TransactionProcessor::default(),
            signing_key,
            public_key,
            block_keeper,
            votings: HashMap::new(),
            network: network.clone(),
            synchronization: Synchronization::new(network),
            last_completed_block: storage::EMPTY_HASH,
        }
    }

    pub fn create_block_storage_view(&self) -> BlockStorageView {
        self.block_keeper.create_block_storage_view()
    }

    pub async fn run(&mut self) -> Result<(), String> {
        self.network.wait_for_readiness().await;
        let mut sync_interval = self.synchronization.create_interval().await;
        loop {
            tokio::select! {
                m = self.get_next_message() => {
                    self.handle_message(m?);
                },
                _ = sync_interval.tick() => {
                    self.synchronization.check_and_retrieve_missing_blocks(&mut self.block_keeper).await;
                }

            }
        }
    }

    async fn get_next_message(&mut self) -> Result<Message, String> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| "Channel is closed".to_string())
    }

    pub fn handle_message(&mut self, message: Message) {
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
            .process_transaction(client_tx.clone())
            .map_err(|e| e.to_string())?;
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
            let mut cons = VotingConsensus::new(self.id, &known_peers);
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

    fn process_block_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        if self.last_completed_block != block_hash {
            self.block_keeper
                .get_uncommited_block(&block_hash)
                .ok_or_else(|| format!("Block ${block_hash} is not found"))?;
            self.make_vote(block_hash, from, approve)?;
        }
        Ok(())
    }

    pub fn make_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        let cons = self.get_consensus(block_hash);
        if !cons.already_voted(&from) && cons.outcome().is_none() {
            let res = match cons.make_vote(from, approve) {
                ConsensusOutcome::Approved => {
                    debug!("Approved block {}", block_hash);
                    self.block_keeper.commit_block(&block_hash)?;
                    self.last_completed_block = block_hash.clone();
                    Some(BlockApproved { block_hash })
                }
                ConsensusOutcome::Rejected => {
                    debug!("Rejected block {}", block_hash);
                    self.block_keeper.rollback_block(&block_hash)?;
                    self.last_completed_block = block_hash.clone();
                    Some(BlockReject { block_hash })
                }
                ConsensusOutcome::Pending => None,
            };
            if let Some(body) = res {
                self.network.broadcast_peer_message(&body, self.id);
            };
        }
        Ok(())
    }

    fn get_consensus(&mut self, block_hash: BlockHash) -> &mut VotingConsensus {
        self.votings
            .entry(block_hash)
            .or_insert_with(|| VotingConsensus::new(self.id, &self.network.known_peers()))
    }
}
