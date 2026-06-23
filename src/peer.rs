pub use crate::config::DEFAULT_CHANNEL_SIZE;
use crate::consensus::{ConsensusEngine, ConsensusInput, ConsensusOutput};
use crate::network::NetworkInterface;
use crate::storage;
use crate::storage::{BlockHash, BlockKeeper, BlockStatus};
use crate::transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};
use derive_more::{Constructor, Display, From};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

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
    #[display("RaftRequestVote")]
    RaftRequestVote { term: u64, candidate_id: PeerId },
    #[display("RaftRequestVoteResponse")]
    RaftRequestVoteResponse { term: u64, vote_granted: bool },
    #[display("RaftAppendEntries")]
    RaftAppendEntries { term: u64, leader_id: PeerId },
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
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    consensus: ConsensusEngine,
    signing_key: SigningKey,
    public_key: VerifyingKey,
    network: Arc<N>,
    last_completed_block: BlockHash,
}

impl<N: NetworkInterface> Peer<N> {
    pub fn new(
        id: PeerId,
        network: Arc<N>,
        consensus: ConsensusEngine,
        block_keeper: BlockKeeper,
        signing_key: SigningKey,
    ) -> Peer<N> {
        let public_key = VerifyingKey::from(signing_key.clone());
        Self {
            id,
            transaction_processor: TransactionProcessor::default(),
            signing_key,
            public_key,
            block_keeper,
            consensus,
            network: network.clone(),
            last_completed_block: storage::EMPTY_HASH,
        }
    }

    pub fn block_keeper_mut(&mut self) -> &mut BlockKeeper {
        &mut self.block_keeper
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
            MessageBody::RaftRequestVote { term, candidate_id } => {
                self.process_raft_request_vote(term, candidate_id, message.from)
            }
            MessageBody::RaftRequestVoteResponse { term, vote_granted } => {
                self.process_raft_request_vote_response(term, message.from, vote_granted)
            }
            MessageBody::RaftAppendEntries { term, leader_id } => {
                self.process_raft_append_entries(term, leader_id, message.from)
            }
        } {
            eprintln!("Failed to process message: {e}");
        }
    }

    fn process_client_transaction(&mut self, client_tx: SignedTransaction) -> Result<(), String> {
        client_tx.verify()?;

        self.handle_consensus_input(ConsensusInput::ClientTransactionReceived(client_tx))
    }

    fn apply_client_transaction(&mut self, client_tx: SignedTransaction) -> Result<(), String> {
        self.transaction_processor
            .process_transaction(client_tx.clone())
            .map_err(|e| e.to_string())?;
        let status = self.block_keeper.add_transaction(client_tx.clone());

        let verified_tx = VerifiedTransaction::new(client_tx, &self.signing_key);
        self.broadcast_transaction(&verified_tx)?;

        if let BlockStatus::NewBlockCreated { block_hash } = status {
            self.handle_new_block_created(block_hash)?
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
            self.handle_new_block_created(block_hash)?
        }
        Ok(())
    }

    fn handle_new_block_created(&mut self, block_hash: BlockHash) -> Result<(), String> {
        self.handle_consensus_input(ConsensusInput::NewBlockCreated { block_hash })
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
            self.handle_consensus_input(ConsensusInput::LocalBlockProposed {
                block_hash,
                known_peers,
            })?;
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
        self.handle_consensus_input(ConsensusInput::BlockProposalValidated {
            block_hash,
            proposer: from,
            valid: is_ok,
            known_peers: self.network.known_peers(),
        })
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
            self.handle_consensus_input(ConsensusInput::BlockVoteReceived {
                block_hash,
                from,
                approve,
                known_peers: self.network.known_peers(),
            })?;
        }
        Ok(())
    }

    pub fn make_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        self.handle_consensus_input(ConsensusInput::BlockVoteReceived {
            block_hash,
            from,
            approve,
            known_peers: self.network.known_peers(),
        })
    }

    fn process_raft_request_vote(
        &mut self,
        term: u64,
        candidate_id: PeerId,
        from: PeerId,
    ) -> Result<(), String> {
        self.handle_consensus_input(ConsensusInput::RaftRequestVote {
            term,
            candidate_id,
            from,
        })
    }

    fn process_raft_request_vote_response(
        &mut self,
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    ) -> Result<(), String> {
        self.handle_consensus_input(ConsensusInput::RaftRequestVoteResponse {
            term,
            voter_id,
            vote_granted,
        })
    }

    fn process_raft_append_entries(
        &mut self,
        term: u64,
        leader_id: PeerId,
        from: PeerId,
    ) -> Result<(), String> {
        self.handle_consensus_input(ConsensusInput::RaftAppendEntries {
            term,
            leader_id,
            from,
            now: Instant::now(),
        })
    }

    pub fn handle_consensus_input(&mut self, input: ConsensusInput) -> Result<(), String> {
        let outputs = self.consensus.handle_input(input);
        for output in outputs {
            self.apply_consensus_output(output)?;
        }
        Ok(())
    }

    fn apply_consensus_output(&mut self, output: ConsensusOutput) -> Result<(), String> {
        match output {
            ConsensusOutput::ApplyClientTransaction(client_tx) => {
                self.apply_client_transaction(client_tx)?;
            }
            ConsensusOutput::ProposeBlock(block_hash) => {
                self.broadcast_block_proposal(block_hash)?;
            }
            ConsensusOutput::CommitBlock(block_hash) => {
                debug!("Approved block {}", block_hash);
                self.block_keeper.commit_block(&block_hash)?;
                self.last_completed_block = block_hash;
            }
            ConsensusOutput::RollbackBlock(block_hash) => {
                debug!("Rejected block {}", block_hash);
                self.block_keeper.rollback_block(&block_hash)?;
                self.last_completed_block = block_hash;
            }
            ConsensusOutput::Broadcast(body) => {
                self.network.broadcast_peer_message(&body, self.id);
            }
            ConsensusOutput::Send { to, body } => {
                self.network.send_peer_message(Message {
                    from: self.id,
                    to,
                    body,
                });
            }
            ConsensusOutput::Reject(message) => {
                return Err(message);
            }
        }
        Ok(())
    }
}
