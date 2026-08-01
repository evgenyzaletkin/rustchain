//! Owns peer services and executes side effects requested by consensus.

use super::{Message, MessageBody, PeerId, RaftReplicatedBlock};
use crate::network::NetworkInterface;
use crate::peer::consensus::{ConsensusEffect, ConsensusEngine, RaftLogEntry};
use crate::storage::{self, BlockFile, BlockHash, BlockKeeper, BlockStatus, BlockStorageView};
use crate::transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use std::collections::VecDeque;
use std::sync::Arc;

pub(super) struct PeerEffects<Network: NetworkInterface> {
    peer_id: PeerId,
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    signing_key: SigningKey,
    public_key: VerifyingKey,
    network: Arc<Network>,
    last_completed_block: BlockHash,
}

impl<Network: NetworkInterface> PeerEffects<Network> {
    pub(super) fn new(
        peer_id: PeerId,
        network: Arc<Network>,
        block_keeper: BlockKeeper,
        signing_key: SigningKey,
    ) -> Self {
        let public_key = VerifyingKey::from(signing_key.clone());
        Self {
            peer_id,
            transaction_processor: TransactionProcessor::default(),
            block_keeper,
            signing_key,
            public_key,
            network,
            last_completed_block: storage::EMPTY_HASH,
        }
    }

    pub(super) fn block_keeper_mut(&mut self) -> &mut BlockKeeper {
        &mut self.block_keeper
    }

    pub(super) fn create_block_storage_view(&self) -> BlockStorageView {
        self.block_keeper.create_block_storage_view()
    }

    pub(super) fn known_peers(&self) -> Vec<PeerId> {
        self.network.known_peers()
    }

    pub(super) fn last_completed_block(&self) -> BlockHash {
        self.last_completed_block
    }

    pub(super) fn stage_external_block(&mut self, block_file: BlockFile) -> Result<(), String> {
        if self.block_keeper.block_can_be_added(&block_file) {
            self.block_keeper
                .add_external_block(block_file)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub(super) fn ensure_uncommitted_block(&self, block_hash: &BlockHash) -> Result<(), String> {
        self.block_keeper
            .get_uncommited_block(block_hash)
            .map(|_| ())
            .ok_or_else(|| format!("Block ${block_hash} is not found"))
    }

    pub(super) fn execute_all(
        &mut self,
        consensus: &mut ConsensusEngine,
        effects: Vec<ConsensusEffect>,
    ) -> Result<(), String> {
        let mut pending_effects = VecDeque::from(effects);
        while let Some(effect) = pending_effects.pop_front() {
            match effect {
                ConsensusEffect::StageClientTransaction(client_tx) => {
                    if let Some(block_hash) = self.stage_client_transaction(client_tx)? {
                        pending_effects.extend(consensus.on_block_created(block_hash)?);
                    }
                }
                ConsensusEffect::StageRaftEntries(entries) => {
                    self.stage_raft_entries(consensus, entries)?;
                }
                ConsensusEffect::BroadcastClientTransaction(client_tx) => {
                    self.broadcast_client_transaction(client_tx)?;
                }
                ConsensusEffect::ProposeBlock(block_hash) => {
                    if let Some(known_peers) = self.broadcast_block_proposal(block_hash)? {
                        pending_effects
                            .extend(consensus.on_local_block_proposed(block_hash, known_peers)?);
                    }
                }
                ConsensusEffect::SendRaftAppendEntries {
                    to,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                } => self.send_raft_append_entries(
                    to,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )?,
                ConsensusEffect::CommitBlock(block_hash) => {
                    debug!("Approved block {}", block_hash);
                    self.block_keeper.commit_block(&block_hash)?;
                    self.last_completed_block = block_hash;
                }
                ConsensusEffect::RollbackBlock(block_hash) => {
                    debug!("Rejected block {}", block_hash);
                    self.block_keeper.rollback_block(&block_hash)?;
                    self.last_completed_block = block_hash;
                }
                ConsensusEffect::Broadcast(body) => {
                    self.network.broadcast_peer_message(&body, self.peer_id);
                }
                ConsensusEffect::Send { to, body } => {
                    self.network.send_peer_message(Message {
                        from: self.peer_id,
                        to,
                        body,
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) fn stage_client_transaction(
        &mut self,
        client_tx: SignedTransaction,
    ) -> Result<Option<BlockHash>, String> {
        client_tx.verify()?;
        self.transaction_processor
            .process_transaction(client_tx.clone())
            .map_err(|e| e.to_string())?;
        match self.block_keeper.add_transaction(client_tx) {
            BlockStatus::NewBlockCreated { block_hash } => Ok(Some(block_hash)),
            BlockStatus::AddedToMempool => Ok(None),
        }
    }

    fn broadcast_client_transaction(&self, client_tx: SignedTransaction) -> Result<(), String> {
        let verified_tx = VerifiedTransaction::new(client_tx, &self.signing_key);
        self.network
            .broadcast_peer_message(&MessageBody::Synchronization(verified_tx), self.peer_id);
        Ok(())
    }

    fn stage_raft_entries(
        &mut self,
        consensus: &mut ConsensusEngine,
        entries: Vec<RaftLogEntry>,
    ) -> Result<(), String> {
        for entry in entries {
            let pending_block = consensus.take_pending_raft_block(&entry.block_hash);
            if self
                .block_keeper
                .get_uncommited_block(&entry.block_hash)
                .is_some()
            {
                continue;
            }

            let Some(block_file) = pending_block else {
                return Err(format!(
                    "Validated Raft block {} for log index {} is not found",
                    entry.block_hash, entry.index
                ));
            };

            if self.block_keeper.block_can_be_added(&block_file) {
                self.block_keeper
                    .add_external_block(block_file)
                    .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    }

    fn broadcast_block_proposal(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<Option<Vec<PeerId>>, String> {
        let known_peers = self.network.known_peers();
        if known_peers.is_empty() {
            self.block_keeper.commit_block(&block_hash)?;
            return Ok(None);
        }

        if let Some(block_file) = self.block_keeper.get_uncommited_block(&block_hash) {
            let block_as_bytes =
                serde_json::to_vec(block_file).expect("Failed to serialize block file");
            let signature: Signature = self.signing_key.sign(&block_as_bytes);
            self.network.broadcast_peer_message(
                &MessageBody::BlockProposal {
                    block_hash,
                    block_file: block_as_bytes,
                    signature,
                    public_key: self.public_key,
                },
                self.peer_id,
            )
        }
        Ok(Some(known_peers))
    }

    fn send_raft_append_entries(
        &mut self,
        to: PeerId,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> Result<(), String> {
        let mut replicated_entries = Vec::with_capacity(entries.len());
        for entry in entries {
            let block_file = if let Some(block_file) =
                self.block_keeper.get_uncommited_block(&entry.block_hash)
            {
                block_file.clone()
            } else {
                let block_file = self.block_keeper.read_block_by_index(entry.index)?;
                if block_file.hash != entry.block_hash {
                    return Err(format!(
                        "Block at Raft log index {} has hash {}, expected {}",
                        entry.index, block_file.hash, entry.block_hash
                    ));
                }
                block_file
            };
            let block_as_bytes =
                serde_json::to_vec(&block_file).expect("Failed to serialize block file");
            let signature: Signature = self.signing_key.sign(&block_as_bytes);
            replicated_entries.push(RaftReplicatedBlock {
                entry,
                block_file: block_as_bytes,
                signature,
                public_key: self.public_key,
            });
        }

        self.network.send_peer_message(Message {
            from: self.peer_id,
            to,
            body: MessageBody::RaftAppendEntries {
                term,
                leader_id: self.peer_id,
                prev_log_index,
                prev_log_term,
                entries: replicated_entries,
                leader_commit,
            },
        });
        Ok(())
    }
}
