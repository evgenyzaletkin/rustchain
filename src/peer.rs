mod action_executor;
pub mod consensus;
mod messages;

pub use crate::config::DEFAULT_CHANNEL_SIZE;
use crate::network::NetworkInterface;
use crate::peer::action_executor::{ActionResult, ConsensusActionExecutor};
use crate::peer::consensus::{ConsensusAction, ConsensusEngine, ConsensusInput, RaftLogEntry};
pub use crate::peer::messages::{Message, MessageBody, PeerId, RaftReplicatedBlock, TxPayload};
use crate::storage;
use crate::storage::{BlockFile, BlockHash, BlockKeeper, BlockStatus};
use crate::transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

pub struct Peer<Network: NetworkInterface> {
    pub id: PeerId,
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    consensus: ConsensusEngine,
    pending_raft_blocks: HashMap<BlockHash, BlockFile>,
    signing_key: SigningKey,
    public_key: VerifyingKey,
    network: Arc<Network>,
    last_completed_block: BlockHash,
}

impl<Network: NetworkInterface> Peer<Network> {
    pub fn new(
        id: PeerId,
        network: Arc<Network>,
        consensus: ConsensusEngine,
        block_keeper: BlockKeeper,
        signing_key: SigningKey,
    ) -> Peer<Network> {
        let public_key = VerifyingKey::from(signing_key.clone());
        Self {
            id,
            transaction_processor: TransactionProcessor::default(),
            signing_key,
            public_key,
            block_keeper,
            consensus,
            pending_raft_blocks: HashMap::new(),
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
            MessageBody::RaftAppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.process_raft_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                message.from,
            ),
            MessageBody::RaftAppendEntriesResponse {
                term,
                success,
                match_index,
            } => {
                self.process_raft_append_entries_response(term, message.from, success, match_index)
            }
        } {
            eprintln!("Failed to process message: {e}");
        }
    }

    fn process_client_transaction(&mut self, client_tx: SignedTransaction) -> Result<(), String> {
        client_tx.verify()?;

        self.handle_consensus_input(ConsensusInput::ClientTransactionReceived(client_tx))
    }

    fn synchronize_transaction(&mut self, verified_tx: VerifiedTransaction) -> Result<(), String> {
        // Verify both client and peer signatures
        verified_tx.verify()?;

        let client_tx = verified_tx.client_tx;
        self.transaction_processor
            .process_transaction(client_tx.clone())
            .map_err(|e| e.to_string())?;
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

        let verification_result =
            BlockFile::verify_block_vec(block_hash.clone(), &block_file, signature, public_key);
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
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftReplicatedBlock>,
        leader_commit: u64,
        from: PeerId,
    ) -> Result<(), String> {
        let log_entries = self.stage_raft_entries(entries)?;
        self.handle_consensus_input(ConsensusInput::RaftAppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries: log_entries,
            leader_commit,
            from,
            now: Instant::now(),
        })
    }

    fn process_raft_append_entries_response(
        &mut self,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Result<(), String> {
        self.handle_consensus_input(ConsensusInput::RaftAppendEntriesResponse {
            term,
            from,
            success,
            match_index,
        })
    }

    fn stage_raft_entries(
        &mut self,
        entries: Vec<RaftReplicatedBlock>,
    ) -> Result<Vec<RaftLogEntry>, String> {
        let mut log_entries = Vec::with_capacity(entries.len());
        for replicated_block in entries {
            let block_file = BlockFile::verify_block_vec(
                replicated_block.entry.block_hash,
                &replicated_block.block_file,
                replicated_block.signature,
                replicated_block.public_key,
            )
            .map_err(|e| e.to_string())?;
            self.pending_raft_blocks
                .insert(replicated_block.entry.block_hash, block_file);
            log_entries.push(replicated_block.entry);
        }
        Ok(log_entries)
    }

    pub fn handle_consensus_input(&mut self, input: ConsensusInput) -> Result<(), String> {
        let mut pending_inputs = VecDeque::from([input]);
        while let Some(input) = pending_inputs.pop_front() {
            let actions = self.consensus.handle_input(input);
            let action_results = self.execute_consensus_actions(actions)?;
            pending_inputs.extend(
                action_results
                    .into_iter()
                    .map(ActionResult::into_consensus_input),
            );
        }
        Ok(())
    }

    fn execute_consensus_actions(
        &mut self,
        actions: Vec<ConsensusAction>,
    ) -> Result<Vec<ActionResult>, String> {
        let mut executor = ConsensusActionExecutor {
            peer_id: self.id,
            transaction_processor: &mut self.transaction_processor,
            block_keeper: &mut self.block_keeper,
            pending_raft_blocks: &mut self.pending_raft_blocks,
            signing_key: &self.signing_key,
            public_key: self.public_key,
            network: &self.network,
            last_completed_block: &mut self.last_completed_block,
        };
        let mut action_results = Vec::new();
        for action in actions {
            action_results.extend(executor.execute(action)?);
        }
        Ok(action_results)
    }
}
