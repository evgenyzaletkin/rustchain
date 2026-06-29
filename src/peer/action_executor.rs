use super::{Message, MessageBody, PeerId, RaftReplicatedBlock};
use crate::network::NetworkInterface;
use crate::peer::consensus::{ConsensusAction, ConsensusInput, RaftLogEntry};
use crate::storage::{BlockFile, BlockHash, BlockKeeper, BlockStatus};
use crate::transactions::{SignedTransaction, TransactionProcessor, VerifiedTransaction};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;

pub(super) enum ActionResult {
    NewBlockCreated {
        block_hash: BlockHash,
    },
    LocalBlockProposed {
        block_hash: BlockHash,
        known_peers: Vec<PeerId>,
    },
}

impl ActionResult {
    pub(super) fn into_consensus_input(self) -> ConsensusInput {
        match self {
            Self::NewBlockCreated { block_hash } => ConsensusInput::NewBlockCreated { block_hash },
            Self::LocalBlockProposed {
                block_hash,
                known_peers,
            } => ConsensusInput::LocalBlockProposed {
                block_hash,
                known_peers,
            },
        }
    }
}

pub(super) struct ConsensusActionExecutor<'a, Network: NetworkInterface> {
    pub(super) peer_id: PeerId,
    pub(super) transaction_processor: &'a mut TransactionProcessor,
    pub(super) block_keeper: &'a mut BlockKeeper,
    pub(super) pending_raft_blocks: &'a mut HashMap<BlockHash, BlockFile>,
    pub(super) signing_key: &'a SigningKey,
    pub(super) public_key: VerifyingKey,
    pub(super) network: &'a Arc<Network>,
    pub(super) last_completed_block: &'a mut BlockHash,
}

impl<Network: NetworkInterface> ConsensusActionExecutor<'_, Network> {
    pub(super) fn execute(&mut self, action: ConsensusAction) -> Result<Vec<ActionResult>, String> {
        match action {
            ConsensusAction::StageClientTransaction(client_tx) => {
                self.stage_client_transaction(client_tx)
            }
            ConsensusAction::StageRaftEntries(entries) => {
                self.stage_raft_entries(entries)?;
                Ok(Vec::new())
            }
            ConsensusAction::BroadcastClientTransaction(client_tx) => {
                self.broadcast_client_transaction(client_tx)?;
                Ok(Vec::new())
            }
            ConsensusAction::ProposeBlock(block_hash) => self.broadcast_block_proposal(block_hash),
            ConsensusAction::SendRaftAppendEntries {
                to,
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.send_raft_append_entries(
                    to,
                    term,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )?;
                Ok(Vec::new())
            }
            ConsensusAction::CommitBlock(block_hash) => {
                debug!("Approved block {}", block_hash);
                self.block_keeper.commit_block(&block_hash)?;
                *self.last_completed_block = block_hash;
                Ok(Vec::new())
            }
            ConsensusAction::RollbackBlock(block_hash) => {
                debug!("Rejected block {}", block_hash);
                self.block_keeper.rollback_block(&block_hash)?;
                *self.last_completed_block = block_hash;
                Ok(Vec::new())
            }
            ConsensusAction::Broadcast(body) => {
                self.network.broadcast_peer_message(&body, self.peer_id);
                Ok(Vec::new())
            }
            ConsensusAction::Send { to, body } => {
                self.network.send_peer_message(Message {
                    from: self.peer_id,
                    to,
                    body,
                });
                Ok(Vec::new())
            }
            ConsensusAction::Reject(message) => Err(message),
        }
    }

    fn stage_client_transaction(
        &mut self,
        client_tx: SignedTransaction,
    ) -> Result<Vec<ActionResult>, String> {
        client_tx.verify()?;
        self.transaction_processor
            .process_transaction(client_tx.clone())
            .map_err(|e| e.to_string())?;
        match self.block_keeper.add_transaction(client_tx) {
            BlockStatus::NewBlockCreated { block_hash } => {
                Ok(vec![ActionResult::NewBlockCreated { block_hash }])
            }
            BlockStatus::AddedToMempool => Ok(Vec::new()),
        }
    }

    fn broadcast_client_transaction(&self, client_tx: SignedTransaction) -> Result<(), String> {
        let verified_tx = VerifiedTransaction::new(client_tx, self.signing_key);
        self.network
            .broadcast_peer_message(&MessageBody::Synchronization(verified_tx), self.peer_id);
        Ok(())
    }

    fn stage_raft_entries(&mut self, entries: Vec<RaftLogEntry>) -> Result<(), String> {
        for entry in entries {
            if self
                .block_keeper
                .get_uncommited_block(&entry.block_hash)
                .is_some()
            {
                continue;
            }

            let Some(block_file) = self.pending_raft_blocks.remove(&entry.block_hash) else {
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
    ) -> Result<Vec<ActionResult>, String> {
        let known_peers = self.network.known_peers();
        if known_peers.is_empty() {
            self.block_keeper.commit_block(&block_hash)?;
            return Ok(Vec::new());
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
        Ok(vec![ActionResult::LocalBlockProposed {
            block_hash,
            known_peers,
        }])
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
