#[allow(dead_code)]
pub mod raft;
pub(crate) mod raft_log_store;
mod voting;

use crate::peer::consensus::raft::RaftConsensus;
use crate::peer::consensus::raft_log_store::AnyRaftLogStore;
use crate::peer::{MessageBody, PeerId, RaftReplicatedBlock};
use crate::storage::{BlockFile, BlockHash};
use crate::transactions::SignedTransaction;
use serde::Serialize;
use std::collections::HashMap;
use std::time::Instant;

pub use raft::{RaftLogEntry, RaftRoleState};
#[allow(unused_imports)]
pub use voting::{ConsensusOutcome, VotingConsensus};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "mode", rename_all = "lowercase")]
pub enum ConsensusState {
    Voting,
    Raft {
        role: RaftRoleState,
        term: u64,
        leader_id: Option<PeerId>,
        commit_index: u64,
        last_log_index: u64,
    },
}

pub enum ConsensusEngine {
    Voting {
        peer_id: PeerId,
        votings: HashMap<BlockHash, VotingConsensus>,
    },
    Raft(RaftConsensus),
}

pub enum ConsensusEffect {
    StageClientTransaction(SignedTransaction),
    StageRaftEntries(Vec<RaftLogEntry>),
    BroadcastClientTransaction(SignedTransaction),
    ProposeBlock(BlockHash),
    SendRaftAppendEntries {
        to: PeerId,
        term: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    },
    CommitBlock(BlockHash),
    RollbackBlock(BlockHash),
    Broadcast(MessageBody),
    Send {
        to: PeerId,
        body: MessageBody,
    },
}

impl ConsensusEngine {
    pub fn new_voting(peer_id: PeerId) -> Self {
        Self::Voting {
            peer_id,
            votings: HashMap::new(),
        }
    }

    pub fn new_raft(peer_id: PeerId) -> Self {
        Self::Raft(RaftConsensus::new(peer_id))
    }

    pub(crate) fn new_raft_with_storage(
        peer_id: PeerId,
        raft_log_store: AnyRaftLogStore,
        commit_index: u64,
    ) -> Result<Self, String> {
        Ok(Self::Raft(RaftConsensus::new_with_storage(
            peer_id,
            raft_log_store,
            commit_index,
        )?))
    }

    pub fn requires_tick(&self) -> bool {
        matches!(self, Self::Raft(_))
    }

    pub fn state(&self) -> ConsensusState {
        match self {
            Self::Voting { .. } => ConsensusState::Voting,
            Self::Raft(raft) => raft.state(),
        }
    }

    pub fn on_client_transaction(
        &mut self,
        client_tx: SignedTransaction,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(voting::on_client_transaction(client_tx)),
            Self::Raft(raft) => raft.on_client_transaction(client_tx),
        }
    }

    pub fn on_block_created(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(voting::on_block_created(block_hash)),
            Self::Raft(raft) => raft.on_block_created(block_hash),
        }
    }

    pub fn on_local_block_proposed(
        &mut self,
        block_hash: BlockHash,
        known_peers: Vec<PeerId>,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { peer_id, votings } => Ok(voting::on_local_block_proposed(
                *peer_id,
                votings,
                block_hash,
                &known_peers,
            )),
            Self::Raft(_) => Ok(Vec::new()),
        }
    }

    pub fn on_block_proposal_validated(
        &mut self,
        block_hash: BlockHash,
        proposer: PeerId,
        valid: bool,
        known_peers: Vec<PeerId>,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { peer_id, votings } => Ok(voting::on_block_proposal_validated(
                *peer_id,
                votings,
                block_hash,
                proposer,
                valid,
                &known_peers,
            )),
            Self::Raft(_) => Ok(Vec::new()),
        }
    }

    pub fn on_block_vote(
        &mut self,
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
        known_peers: Vec<PeerId>,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { peer_id, votings } => Ok(voting::on_block_vote(
                *peer_id,
                votings,
                block_hash,
                from,
                approve,
                &known_peers,
            )),
            Self::Raft(_) => Ok(Vec::new()),
        }
    }

    pub fn on_tick(
        &mut self,
        now: Instant,
        known_peers: Vec<PeerId>,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(Vec::new()),
            Self::Raft(raft) => raft.on_tick(now, &known_peers),
        }
    }

    pub fn on_request_vote(
        &mut self,
        term: u64,
        candidate_id: PeerId,
        last_log_index: u64,
        last_log_term: u64,
        from: PeerId,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(Vec::new()),
            Self::Raft(raft) => {
                raft.on_request_vote(term, candidate_id, last_log_index, last_log_term, from)
            }
        }
    }

    pub fn on_request_vote_response(
        &mut self,
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(Vec::new()),
            Self::Raft(raft) => raft.on_request_vote_response(term, voter_id, vote_granted),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_append_entries(
        &mut self,
        term: u64,
        leader_id: PeerId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftReplicatedBlock>,
        leader_commit: u64,
        from: PeerId,
        now: Instant,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(Vec::new()),
            Self::Raft(raft) => raft.on_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                from,
                now,
            ),
        }
    }

    pub fn on_append_entries_response(
        &mut self,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self {
            Self::Voting { .. } => Ok(Vec::new()),
            Self::Raft(raft) => raft.on_append_entries_response(term, from, success, match_index),
        }
    }

    pub(crate) fn take_pending_raft_block(&mut self, block_hash: &BlockHash) -> Option<BlockFile> {
        match self {
            Self::Voting { .. } => None,
            Self::Raft(raft) => raft.take_pending_block(block_hash),
        }
    }

    /// Dispatches a message that requires no validation beyond what consensus itself
    /// performs. `Peer` routes here for every message kind it doesn't need to validate
    /// or stage via `PeerEffects` first (client transactions, synchronization, and raw
    /// block proposals are handled by `Peer` directly and never reach this method).
    pub fn on_message(
        &mut self,
        from: PeerId,
        body: MessageBody,
        now: Instant,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match body {
            MessageBody::RaftRequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(term, candidate_id, last_log_index, last_log_term, from),
            MessageBody::RaftRequestVoteResponse { term, vote_granted } => {
                self.on_request_vote_response(term, from, vote_granted)
            }
            MessageBody::RaftAppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                from,
                now,
            ),
            MessageBody::RaftAppendEntriesResponse {
                term,
                success,
                match_index,
            } => self.on_append_entries_response(term, from, success, match_index),
            other => Err(format!(
                "{other} must be validated by Peer before reaching consensus"
            )),
        }
    }
}

#[cfg(test)]
mod tests;
