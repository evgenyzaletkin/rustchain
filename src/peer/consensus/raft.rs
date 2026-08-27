use crate::config::{
    DEFAULT_RAFT_ELECTION_TIMEOUT, DEFAULT_RAFT_ELECTION_TIMEOUT_JITTER,
    DEFAULT_RAFT_HEARTBEAT_INTERVAL,
};
use crate::crypto::KeyManager;
use crate::peer::MessageBody;
use crate::peer::PeerId;
use crate::peer::RaftReplicatedBlock;
use crate::peer::consensus::raft_log_store::{AnyRaftLogStore, InMemoryRaftLogStore};
use crate::peer::consensus::{ConsensusEffect, ConsensusState};
use crate::storage::{BlockFile, BlockHash, EMPTY_HASH};
use crate::transactions::SignedTransaction;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = DEFAULT_RAFT_HEARTBEAT_INTERVAL;
pub const DEFAULT_ELECTION_TIMEOUT: Duration = DEFAULT_RAFT_ELECTION_TIMEOUT;
pub const DEFAULT_ELECTION_TIMEOUT_JITTER: Duration = DEFAULT_RAFT_ELECTION_TIMEOUT_JITTER;
const MAX_APPEND_ENTRIES: usize = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoteResponse {
    Granted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub block_hash: BlockHash,
}

impl RaftLogEntry {
    pub fn noop(term: u64, index: u64) -> Self {
        Self {
            term,
            index,
            block_hash: EMPTY_HASH,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.block_hash == EMPTY_HASH
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftRoleState {
    Follower,
    Candidate,
    Leader,
}

struct AppendEntriesRequest {
    term: u64,
    leader_id: PeerId,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<RaftReplicatedBlock>,
    leader_commit: u64,
    from: PeerId,
    now: Instant,
}

pub struct RaftConsensus {
    peer_id: PeerId,
    participants: HashSet<PeerId>,
    current_term: u64,
    voted_for: Option<PeerId>,
    leader_id: Option<PeerId>,
    role: RaftRole,
    votes_received: HashSet<PeerId>,
    heartbeat_interval: Duration,
    election_timeout_base: Duration,
    election_timeout_jitter: Duration,
    current_election_timeout: Duration,
    last_heartbeat_received_at: Instant,
    last_heartbeat_sent_at: Option<Instant>,
    log: Vec<RaftLogEntry>,
    raft_log_store: AnyRaftLogStore,
    commit_index: u64,
    match_indexes: HashMap<PeerId, u64>,
    pending_blocks: HashMap<BlockHash, BlockFile>,
}

impl RaftConsensus {
    pub fn new(peer_id: PeerId) -> Self {
        Self::new_with_storage(
            peer_id,
            AnyRaftLogStore::InMemory(InMemoryRaftLogStore::new()),
            0,
        )
        .expect("In-memory Raft log store must be readable")
    }

    pub(crate) fn new_with_storage(
        peer_id: PeerId,
        raft_log_store: AnyRaftLogStore,
        commit_index: u64,
    ) -> Result<Self, String> {
        let persisted_state = raft_log_store.load()?;
        let commit_index = persisted_state.commit_index.unwrap_or(commit_index);
        Ok(Self::new_with_log(
            peer_id,
            persisted_state.log,
            commit_index,
            raft_log_store,
        ))
    }

    pub(super) fn state(&self) -> ConsensusState {
        let role = match self.role {
            RaftRole::Follower => RaftRoleState::Follower,
            RaftRole::Candidate => RaftRoleState::Candidate,
            RaftRole::Leader => RaftRoleState::Leader,
        };
        ConsensusState::Raft {
            role,
            term: self.current_term,
            leader_id: self.leader_id,
            commit_index: self.commit_index,
            last_log_index: self.last_log_index(),
        }
    }

    fn new_with_log(
        peer_id: PeerId,
        log: Vec<RaftLogEntry>,
        commit_index: u64,
        raft_log_store: AnyRaftLogStore,
    ) -> Self {
        let participants = HashSet::from([peer_id]);
        let commit_index = commit_index.min(log.last().map(|entry| entry.index).unwrap_or(0));
        Self {
            peer_id,
            participants,
            current_term: 0,
            voted_for: None,
            leader_id: None,
            role: RaftRole::Follower,
            votes_received: HashSet::new(),
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            election_timeout_base: DEFAULT_ELECTION_TIMEOUT,
            election_timeout_jitter: DEFAULT_ELECTION_TIMEOUT_JITTER,
            current_election_timeout: election::random_election_timeout(
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_ELECTION_TIMEOUT_JITTER,
            ),
            last_heartbeat_received_at: Instant::now(),
            last_heartbeat_sent_at: None,
            log,
            raft_log_store,
            commit_index,
            match_indexes: HashMap::new(),
            pending_blocks: HashMap::new(),
        }
    }
    pub(super) fn on_client_transaction(
        &mut self,
        client_tx: SignedTransaction,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self.role {
            RaftRole::Leader => {
                self.ensure_no_uncommitted_block()?;
                Ok(vec![ConsensusEffect::StageClientTransaction(client_tx)])
            }
            RaftRole::Follower | RaftRole::Candidate => self.forward_client_transaction(client_tx),
        }
    }

    pub(super) fn on_block_created(
        &mut self,
        block_hash: BlockHash,
    ) -> Result<Vec<ConsensusEffect>, String> {
        match self.role {
            RaftRole::Leader => self.append_local_block(block_hash),
            RaftRole::Follower | RaftRole::Candidate => Ok(Vec::new()),
        }
    }

    pub(super) fn on_tick(
        &mut self,
        now: Instant,
        known_peers: &[PeerId],
    ) -> Result<Vec<ConsensusEffect>, String> {
        self.update_participants(known_peers);
        if self.role == RaftRole::Leader {
            if self.heartbeat_due(now) {
                self.last_heartbeat_sent_at = Some(now);
                return Ok(self.append_entries_effects_for_followers());
            }
            return Ok(Vec::new());
        }

        if now.duration_since(self.last_heartbeat_received_at) >= self.current_election_timeout {
            let became_leader = self.start_election_at(now);
            let mut effects = vec![ConsensusEffect::Broadcast(MessageBody::RaftRequestVote {
                term: self.current_term,
                candidate_id: self.peer_id,
                last_log_index: self.last_log_index(),
                last_log_term: self.last_log_term(),
            })];
            if became_leader {
                effects.extend(self.append_leader_noop()?);
            }
            return Ok(effects);
        }
        Ok(Vec::new())
    }

    pub(super) fn on_request_vote(
        &mut self,
        term: u64,
        candidate_id: PeerId,
        last_log_index: u64,
        last_log_term: u64,
        from: PeerId,
    ) -> Result<Vec<ConsensusEffect>, String> {
        let response = self.request_vote(term, candidate_id, last_log_index, last_log_term);
        Ok(vec![ConsensusEffect::Send {
            to: from,
            body: MessageBody::RaftRequestVoteResponse {
                term: self.current_term,
                vote_granted: response == VoteResponse::Granted,
            },
        }])
    }

    pub(super) fn on_request_vote_response(
        &mut self,
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    ) -> Result<Vec<ConsensusEffect>, String> {
        if self.role == RaftRole::Candidate {
            if self.receive_vote(term, voter_id, vote_granted) {
                return self.append_leader_noop();
            }
        } else {
            self.step_down_on_newer_term(term);
        }
        Ok(Vec::new())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn on_append_entries(
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
        self.handle_append_entries(AppendEntriesRequest {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
            from,
            now,
        })
    }

    fn validate_replicated_entries(
        entries: Vec<RaftReplicatedBlock>,
    ) -> Result<(Vec<RaftLogEntry>, HashMap<BlockHash, BlockFile>), String> {
        let mut blocks = HashMap::with_capacity(entries.len());
        let mut log_entries = Vec::with_capacity(entries.len());
        for replicated_block in entries {
            if replicated_block.entry.is_noop() {
                if !replicated_block.block_file.is_empty() {
                    return Err(format!(
                        "Raft no-op entry at index {} contains a block payload",
                        replicated_block.entry.index
                    ));
                }
                KeyManager::verify_message(
                    &replicated_block.public_key,
                    &replicated_block.signature,
                    &replicated_block.block_file,
                )
                .map_err(|e| e.to_string())?;
            } else {
                let block_file = BlockFile::verify_block_vec(
                    replicated_block.entry.block_hash,
                    &replicated_block.block_file,
                    replicated_block.signature,
                    replicated_block.public_key,
                )
                .map_err(|e| e.to_string())?;
                blocks.insert(replicated_block.entry.block_hash, block_file);
            }
            log_entries.push(replicated_block.entry);
        }
        Ok((log_entries, blocks))
    }

    pub(super) fn on_append_entries_response(
        &mut self,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Result<Vec<ConsensusEffect>, String> {
        if self.role == RaftRole::Leader {
            return self.handle_leader_append_entries_response(term, from, success, match_index);
        }
        self.step_down_on_newer_term(term);
        Ok(Vec::new())
    }
}

impl RaftConsensus {
    fn forward_client_transaction(
        &self,
        client_tx: SignedTransaction,
    ) -> Result<Vec<ConsensusEffect>, String> {
        let leader_id = self
            .leader_id
            .ok_or_else(|| "Raft leader is unknown".to_string())?;
        Ok(vec![ConsensusEffect::Send {
            to: leader_id,
            body: MessageBody::ClientTransaction(client_tx),
        }])
    }

    pub(super) fn take_pending_block(&mut self, block_hash: &BlockHash) -> Option<BlockFile> {
        self.pending_blocks.remove(block_hash)
    }
}

mod election;
mod replication;
#[cfg(test)]
#[path = "raft/tests.rs"]
mod tests;
