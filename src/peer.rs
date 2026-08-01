pub mod consensus;
mod effects;
mod messages;

pub use crate::config::DEFAULT_CHANNEL_SIZE;
use crate::network::NetworkInterface;
use crate::peer::consensus::{
    ConsensusEffect, ConsensusEngine, ConsensusState, ValidatedRaftBlock,
};
use crate::peer::effects::PeerEffects;
pub use crate::peer::messages::{Message, MessageBody, PeerId, RaftReplicatedBlock, TxPayload};
use crate::storage::{BlockFile, BlockHash, BlockKeeper, BlockStorageState, BlockStorageView};
use crate::transactions::{SignedTransaction, VerifiedTransaction};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use serde::Serialize;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::watch;

pub struct Peer<Network: NetworkInterface> {
    pub id: PeerId,
    consensus: ConsensusEngine,
    consensus_state_tx: watch::Sender<ConsensusState>,
    effects: PeerEffects<Network>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct PeerState {
    pub peer_id: PeerId,
    pub known_peers: Vec<PeerId>,
    pub block: BlockStorageState,
    pub consensus: ConsensusState,
}

pub struct PeerStateView {
    peer_id: PeerId,
    block_storage_view: BlockStorageView,
    consensus_state_rx: watch::Receiver<ConsensusState>,
}

impl PeerStateView {
    pub fn get_state(&self, known_peers: Vec<PeerId>) -> PeerState {
        PeerState {
            peer_id: self.peer_id,
            known_peers,
            block: self.block_storage_view.get_latest_state(),
            consensus: self.consensus_state_rx.borrow().clone(),
        }
    }
}

impl<Network: NetworkInterface> Peer<Network> {
    pub fn new(
        id: PeerId,
        network: Arc<Network>,
        consensus: ConsensusEngine,
        block_keeper: BlockKeeper,
        signing_key: SigningKey,
    ) -> Peer<Network> {
        let (consensus_state_tx, _) = watch::channel(consensus.state());
        Self {
            id,
            consensus,
            consensus_state_tx,
            effects: PeerEffects::new(id, network, block_keeper, signing_key),
        }
    }

    pub fn block_keeper_mut(&mut self) -> &mut BlockKeeper {
        self.effects.block_keeper_mut()
    }

    pub fn create_state_view(&self) -> PeerStateView {
        PeerStateView {
            peer_id: self.id,
            block_storage_view: self.effects.create_block_storage_view(),
            consensus_state_rx: self.consensus_state_tx.subscribe(),
        }
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
        let effects = self.consensus.on_client_transaction(client_tx)?;
        self.execute_consensus_effects(effects)
    }

    fn synchronize_transaction(&mut self, verified_tx: VerifiedTransaction) -> Result<(), String> {
        // Verify both client and peer signatures
        verified_tx.verify()?;

        let client_tx = verified_tx.client_tx;
        if let Some(block_hash) = self.effects.stage_client_transaction(client_tx)? {
            let effects = self.consensus.on_block_created(block_hash)?;
            self.execute_consensus_effects(effects)?;
        }
        Ok(())
    }

    fn process_block_proposal(
        &mut self,
        block_hash: BlockHash,
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
        from: PeerId,
    ) -> Result<(), String> {
        if self.effects.last_completed_block() == block_hash {
            return Ok(());
        }

        let verification_result =
            BlockFile::verify_block_vec(block_hash.clone(), &block_file, signature, public_key);
        let mut is_ok = false;
        if let Ok(block_file) = verification_result {
            is_ok = true;
            self.effects.stage_external_block(block_file)?;
            // Probably, we should call synchronization here in else block if the height
            // is less than block_index - 1
        }
        let effects = self.consensus.on_block_proposal_validated(
            block_hash,
            from,
            is_ok,
            self.effects.known_peers(),
        )?;
        self.execute_consensus_effects(effects)
    }

    fn process_block_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        if self.effects.last_completed_block() != block_hash {
            self.effects.ensure_uncommitted_block(&block_hash)?;
            let effects = self.consensus.on_block_vote(
                block_hash,
                from,
                approve,
                self.effects.known_peers(),
            )?;
            self.execute_consensus_effects(effects)?;
        }
        Ok(())
    }

    pub fn make_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
    ) -> Result<(), String> {
        let effects =
            self.consensus
                .on_block_vote(block_hash, from, approve, self.effects.known_peers())?;
        self.execute_consensus_effects(effects)
    }

    fn process_raft_request_vote(
        &mut self,
        term: u64,
        candidate_id: PeerId,
        from: PeerId,
    ) -> Result<(), String> {
        let effects = self.consensus.on_request_vote(term, candidate_id, from)?;
        self.execute_consensus_effects(effects)
    }

    fn process_raft_request_vote_response(
        &mut self,
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    ) -> Result<(), String> {
        let effects = self
            .consensus
            .on_request_vote_response(term, voter_id, vote_granted)?;
        self.execute_consensus_effects(effects)
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
        let validated_blocks = Self::validate_raft_blocks(entries)?;
        let effects = self.consensus.on_append_entries(
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            validated_blocks,
            leader_commit,
            from,
            Instant::now(),
        )?;
        self.execute_consensus_effects(effects)
    }

    fn process_raft_append_entries_response(
        &mut self,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Result<(), String> {
        let effects =
            self.consensus
                .on_append_entries_response(term, from, success, match_index)?;
        self.execute_consensus_effects(effects)
    }

    fn validate_raft_blocks(
        entries: Vec<RaftReplicatedBlock>,
    ) -> Result<Vec<ValidatedRaftBlock>, String> {
        let mut validated_blocks = Vec::with_capacity(entries.len());
        for replicated_block in entries {
            let block_file = BlockFile::verify_block_vec(
                replicated_block.entry.block_hash,
                &replicated_block.block_file,
                replicated_block.signature,
                replicated_block.public_key,
            )
            .map_err(|e| e.to_string())?;
            validated_blocks.push(ValidatedRaftBlock {
                entry: replicated_block.entry,
                block_file,
            });
        }
        Ok(validated_blocks)
    }

    pub fn handle_tick(&mut self, now: Instant) -> Result<(), String> {
        let effects = self.consensus.on_tick(now, self.effects.known_peers())?;
        self.execute_consensus_effects(effects)
    }

    fn execute_consensus_effects(&mut self, effects: Vec<ConsensusEffect>) -> Result<(), String> {
        self.effects.execute_all(&mut self.consensus, effects)?;
        self.consensus_state_tx.send_replace(self.consensus.state());
        Ok(())
    }
}
