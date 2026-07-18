use crate::config::{
    DEFAULT_RAFT_ELECTION_TIMEOUT, DEFAULT_RAFT_ELECTION_TIMEOUT_JITTER,
    DEFAULT_RAFT_HEARTBEAT_INTERVAL,
};
use crate::peer::MessageBody;
use crate::peer::PeerId;
use crate::peer::consensus::raft_log_store::{InMemoryRaftLogStore, RaftLogStorage};
use crate::peer::consensus::{
    ConsensusAction, ConsensusInput, ConsensusState, RaftLogEntry, RaftRoleState,
};
use crate::storage::BlockHash;
use crate::transactions::SignedTransaction;
use rand::Rng;
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

impl RaftRole {
    fn handle_client_transaction(
        self,
        raft: &mut RaftConsensus,
        client_tx: SignedTransaction,
    ) -> Vec<ConsensusAction> {
        match self {
            Self::Leader => LeaderRole::handle_client_transaction(raft, client_tx),
            Self::Follower => FollowerRole::handle_client_transaction(raft, client_tx),
            Self::Candidate => CandidateRole::handle_client_transaction(raft, client_tx),
        }
    }

    fn handle_new_block_created(
        self,
        raft: &mut RaftConsensus,
        block_hash: BlockHash,
    ) -> Vec<ConsensusAction> {
        match self {
            Self::Leader => LeaderRole::handle_new_block_created(raft, block_hash),
            Self::Follower | Self::Candidate => Vec::new(),
        }
    }

    fn handle_tick(self, raft: &mut RaftConsensus, now: Instant) -> Vec<ConsensusAction> {
        match self {
            Self::Leader => LeaderRole::handle_tick(raft, now),
            Self::Follower => FollowerRole::handle_tick(raft, now),
            Self::Candidate => CandidateRole::handle_tick(raft, now),
        }
    }

    fn tick_outcome(self, raft: &mut RaftConsensus, now: Instant) -> TickOutcome {
        match self {
            Self::Leader => LeaderRole::tick_outcome(raft, now),
            Self::Follower => FollowerRole::tick_outcome(raft, now),
            Self::Candidate => CandidateRole::tick_outcome(raft, now),
        }
    }

    fn handle_request_vote_response(
        self,
        raft: &mut RaftConsensus,
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    ) -> Vec<ConsensusAction> {
        match self {
            Self::Leader => LeaderRole::handle_request_vote_response(raft, term),
            Self::Follower => FollowerRole::handle_response_with_term(raft, term),
            Self::Candidate => {
                CandidateRole::handle_request_vote_response(raft, term, voter_id, vote_granted)
            }
        }
    }

    fn handle_append_entries_response(
        self,
        raft: &mut RaftConsensus,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Vec<ConsensusAction> {
        match self {
            Self::Leader => {
                LeaderRole::handle_append_entries_response(raft, term, from, success, match_index)
            }
            Self::Follower => FollowerRole::handle_response_with_term(raft, term),
            Self::Candidate => CandidateRole::handle_append_entries_response(raft, term),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VoteResponse {
    Granted,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickOutcome {
    None,
    ElectionStarted,
    HeartbeatDue,
}

struct AppendEntriesRequest {
    term: u64,
    leader_id: PeerId,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: Vec<RaftLogEntry>,
    leader_commit: u64,
    from: PeerId,
    now: Instant,
}

struct LeaderRole;
struct FollowerRole;
struct CandidateRole;

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
    raft_log_store: Box<dyn RaftLogStorage>,
    commit_index: u64,
    match_indexes: HashMap<PeerId, u64>,
}

impl RaftConsensus {
    pub fn new(peer_id: PeerId) -> Self {
        Self::new_with_storage(peer_id, Box::new(InMemoryRaftLogStore::new()), 0)
            .expect("In-memory Raft log store must be readable")
    }

    pub(crate) fn new_with_storage(
        peer_id: PeerId,
        raft_log_store: Box<dyn RaftLogStorage>,
        commit_index: u64,
    ) -> Result<Self, String> {
        let log = raft_log_store.load()?;
        Ok(Self::new_with_log(
            peer_id,
            log,
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
        raft_log_store: Box<dyn RaftLogStorage>,
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
            current_election_timeout: random_election_timeout(
                DEFAULT_ELECTION_TIMEOUT,
                DEFAULT_ELECTION_TIMEOUT_JITTER,
            ),
            last_heartbeat_received_at: Instant::now(),
            last_heartbeat_sent_at: None,
            log,
            raft_log_store,
            commit_index,
            match_indexes: HashMap::new(),
        }
    }
    pub fn handle_raft_input(&mut self, input: ConsensusInput) -> Vec<ConsensusAction> {
        let role = self.role;
        match input {
            ConsensusInput::ClientTransactionReceived(client_tx) => {
                role.handle_client_transaction(self, client_tx)
            }
            ConsensusInput::NewBlockCreated { block_hash } => {
                role.handle_new_block_created(self, block_hash)
            }
            ConsensusInput::Tick {
                now, known_peers, ..
            } => {
                self.update_participants(&known_peers);
                role.handle_tick(self, now)
            }
            ConsensusInput::RaftRequestVote {
                term,
                candidate_id,
                from,
            } => self.handle_request_vote(term, candidate_id, from),
            ConsensusInput::RaftRequestVoteResponse {
                term,
                voter_id,
                vote_granted,
            } => role.handle_request_vote_response(self, term, voter_id, vote_granted),
            ConsensusInput::RaftAppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                from,
                now,
            } => self.handle_append_entries(AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                from,
                now,
            }),
            ConsensusInput::RaftAppendEntriesResponse {
                term,
                from,
                success,
                match_index,
            } => role.handle_append_entries_response(self, term, from, success, match_index),
            ConsensusInput::LocalBlockProposed { .. }
            | ConsensusInput::BlockProposalValidated { .. }
            | ConsensusInput::BlockVoteReceived { .. } => Vec::new(),
        }
    }

    fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: PeerId,
        from: PeerId,
    ) -> Vec<ConsensusAction> {
        let response = self.request_vote(term, candidate_id);
        vec![ConsensusAction::Send {
            to: from,
            body: MessageBody::RaftRequestVoteResponse {
                term: self.current_term,
                vote_granted: response == VoteResponse::Granted,
            },
        }]
    }

    fn handle_append_entries(&mut self, request: AppendEntriesRequest) -> Vec<ConsensusAction> {
        if !self.receive_append_entries_at(
            request.term,
            request.leader_id,
            request.from,
            request.now,
        ) {
            return vec![self.append_entries_response(
                request.from,
                false,
                self.matching_index_for(
                    request.prev_log_index,
                    request.prev_log_term,
                    &request.entries,
                ),
            )];
        }

        let match_index = self.matching_index_for(
            request.prev_log_index,
            request.prev_log_term,
            &request.entries,
        );
        let accepted_match_index = request
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(request.prev_log_index);
        let log_changed = accepted_match_index > request.prev_log_index;
        let Some(mut outputs) = self.append_entries(
            request.prev_log_index,
            request.prev_log_term,
            request.entries,
            request.leader_commit,
        ) else {
            return vec![self.append_entries_response(request.from, false, match_index)];
        };

        outputs.insert(
            0,
            self.append_entries_response(request.from, true, accepted_match_index),
        );
        if log_changed && let Err(err) = self.persist_log() {
            return vec![ConsensusAction::Reject(err)];
        }
        outputs
    }

    fn append_entries_response(
        &self,
        to: PeerId,
        success: bool,
        match_index: u64,
    ) -> ConsensusAction {
        ConsensusAction::Send {
            to,
            body: MessageBody::RaftAppendEntriesResponse {
                term: self.current_term,
                success,
                match_index,
            },
        }
    }

    fn update_participants(&mut self, known_peers: &[PeerId]) {
        let mut participants = HashSet::from_iter(known_peers.iter().copied());
        participants.insert(self.peer_id);
        self.participants = participants;
        self.votes_received
            .retain(|peer_id| self.participants.contains(peer_id));
        self.match_indexes
            .retain(|peer_id, _| self.participants.contains(peer_id));
    }

    fn start_election_at(&mut self, now: Instant) {
        self.current_term += 1;
        self.role = RaftRole::Candidate;
        self.voted_for = Some(self.peer_id);
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.peer_id);
        self.last_heartbeat_received_at = now;
        self.last_heartbeat_sent_at = None;
        self.become_leader_if_majority();
    }

    fn tick(&mut self, now: Instant) -> TickOutcome {
        self.role.tick_outcome(self, now)
    }

    fn request_vote(&mut self, term: u64, candidate_id: PeerId) -> VoteResponse {
        if term < self.current_term {
            return VoteResponse::Rejected;
        }

        if term > self.current_term {
            self.step_down(term);
        }

        match self.voted_for {
            Some(voted_for) if voted_for != candidate_id => VoteResponse::Rejected,
            _ => {
                self.voted_for = Some(candidate_id);
                VoteResponse::Granted
            }
        }
    }

    fn receive_vote(&mut self, term: u64, voter_id: PeerId, granted: bool) {
        if term > self.current_term {
            self.step_down(term);
            return;
        }

        if self.role != RaftRole::Candidate || term != self.current_term || !granted {
            return;
        }

        if self.participants.contains(&voter_id) {
            self.votes_received.insert(voter_id);
            self.become_leader_if_majority();
        }
    }

    fn receive_append_entries_at(
        &mut self,
        term: u64,
        leader_id: PeerId,
        from: PeerId,
        now: Instant,
    ) -> bool {
        if from != leader_id || !self.participants.contains(&leader_id) {
            return false;
        }

        if term < self.current_term {
            return false;
        }

        if self
            .leader_id
            .is_some_and(|current_leader_id| current_leader_id != leader_id)
            && !self.leader_timed_out(now)
        {
            return false;
        }

        if term > self.current_term {
            self.step_down(term);
        }

        let leader_changed = self.leader_id != Some(leader_id);
        self.role = RaftRole::Follower;
        self.leader_id = Some(leader_id);
        self.votes_received.clear();
        self.last_heartbeat_received_at = now;
        if leader_changed {
            self.reset_election_timeout();
        }
        true
    }

    fn step_down(&mut self, term: u64) {
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_received.clear();
    }

    fn become_leader_if_majority(&mut self) {
        if self.votes_received.len() >= self.majority() {
            self.role = RaftRole::Leader;
            self.leader_id = Some(self.peer_id);
            self.last_heartbeat_sent_at = None;
            self.match_indexes.clear();
            for participant in &self.participants {
                let match_index = if *participant == self.peer_id {
                    self.last_log_index()
                } else {
                    0
                };
                self.match_indexes.insert(*participant, match_index);
            }
        }
    }

    fn majority(&self) -> usize {
        self.participants.len() / 2 + 1
    }

    fn leader_timed_out(&self, now: Instant) -> bool {
        now.duration_since(self.last_heartbeat_received_at) >= self.current_election_timeout
    }

    fn reset_election_timeout(&mut self) {
        self.current_election_timeout =
            random_election_timeout(self.election_timeout_base, self.election_timeout_jitter);
    }

    fn append_entries(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> Option<Vec<ConsensusAction>> {
        if self.term_at(prev_log_index) != Some(prev_log_term) {
            return None;
        }

        if !entries_are_contiguous_after(prev_log_index, &entries) {
            return None;
        }

        let previous_commit_index = self.commit_index;
        let entries_to_stage: Vec<_> = entries
            .iter()
            .copied()
            .filter(|entry| entry.index > previous_commit_index)
            .collect();

        for entry in entries {
            if let Some(existing) = self.log_entry(entry.index) {
                if existing.term != entry.term {
                    self.log.truncate((entry.index - 1) as usize);
                    self.log.push(entry);
                }
            } else if entry.index == self.last_log_index() + 1 {
                self.log.push(entry);
            }
        }

        self.commit_index = self
            .commit_index
            .max(leader_commit.min(self.last_log_index()));
        let mut outputs = Vec::new();
        if !entries_to_stage.is_empty() {
            outputs.push(ConsensusAction::StageRaftEntries(entries_to_stage));
        }
        outputs.extend(
            self.log
                .iter()
                .filter(|entry| {
                    entry.index > previous_commit_index && entry.index <= self.commit_index
                })
                .map(|entry| ConsensusAction::CommitBlock(entry.block_hash)),
        );
        Some(outputs)
    }

    fn last_log_index(&self) -> u64 {
        self.log.last().map(|entry| entry.index).unwrap_or(0)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log_entry(index).map(|entry| entry.term)
    }

    fn log_entry(&self, index: u64) -> Option<&RaftLogEntry> {
        self.log.iter().find(|entry| entry.index == index)
    }

    fn matching_index_for(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[RaftLogEntry],
    ) -> u64 {
        let previous_match = if self.term_at(prev_log_index) == Some(prev_log_term) {
            prev_log_index
        } else {
            0
        };

        entries
            .iter()
            .filter(|entry| self.term_at(entry.index) == Some(entry.term))
            .map(|entry| entry.index)
            .max()
            .unwrap_or(previous_match)
    }

    fn persist_log(&mut self) -> Result<(), String> {
        self.raft_log_store.save(&self.log)
    }
}

impl LeaderRole {
    fn handle_client_transaction(
        _raft: &RaftConsensus,
        client_tx: SignedTransaction,
    ) -> Vec<ConsensusAction> {
        vec![ConsensusAction::StageClientTransaction(client_tx)]
    }

    fn handle_new_block_created(
        raft: &mut RaftConsensus,
        block_hash: BlockHash,
    ) -> Vec<ConsensusAction> {
        Self::append_local_block(raft, block_hash)
    }

    fn handle_tick(raft: &mut RaftConsensus, now: Instant) -> Vec<ConsensusAction> {
        if Self::heartbeat_due(raft, now) {
            raft.last_heartbeat_sent_at = Some(now);
            return Self::append_entries_actions_for_followers(raft);
        }
        Vec::new()
    }

    fn tick_outcome(raft: &mut RaftConsensus, now: Instant) -> TickOutcome {
        if Self::heartbeat_due(raft, now) {
            raft.last_heartbeat_sent_at = Some(now);
            return TickOutcome::HeartbeatDue;
        }
        TickOutcome::None
    }

    fn heartbeat_due(raft: &RaftConsensus, now: Instant) -> bool {
        raft.last_heartbeat_sent_at
            .is_none_or(|last_sent_at| now.duration_since(last_sent_at) >= raft.heartbeat_interval)
    }

    fn handle_append_entries_response(
        raft: &mut RaftConsensus,
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    ) -> Vec<ConsensusAction> {
        if term > raft.current_term {
            raft.step_down(term);
            return Vec::new();
        }

        if term != raft.current_term {
            return Vec::new();
        }

        if !raft.participants.contains(&from) {
            return Vec::new();
        }

        if success {
            let current_match_index = raft.match_indexes.entry(from).or_insert(0);
            *current_match_index = (*current_match_index).max(match_index);
            return Self::advance_commit_index(raft);
        }

        let match_index = match_index.min(raft.last_log_index());
        raft.match_indexes.insert(from, match_index);
        if match_index >= raft.last_log_index() {
            return Vec::new();
        }

        Self::append_entries_action_for(raft, from)
            .into_iter()
            .collect()
    }

    fn handle_request_vote_response(raft: &mut RaftConsensus, term: u64) -> Vec<ConsensusAction> {
        step_down_on_newer_term(raft, term);
        Vec::new()
    }

    fn append_local_block(raft: &mut RaftConsensus, block_hash: BlockHash) -> Vec<ConsensusAction> {
        let entry = RaftLogEntry {
            term: raft.current_term,
            index: raft.last_log_index() + 1,
            block_hash,
        };
        raft.log.push(entry);
        raft.match_indexes.insert(raft.peer_id, entry.index);
        if let Err(err) = raft.persist_log() {
            return vec![ConsensusAction::Reject(err)];
        }
        let mut outputs = Self::append_entries_actions_for_followers(raft);
        outputs.extend(Self::advance_commit_index(raft));
        outputs
    }

    fn advance_commit_index(raft: &mut RaftConsensus) -> Vec<ConsensusAction> {
        let previous_commit_index = raft.commit_index;
        for entry in raft.log.iter().rev() {
            if entry.index <= raft.commit_index || entry.term != raft.current_term {
                continue;
            }

            let replicated_count = raft
                .participants
                .iter()
                .filter(|peer_id| {
                    raft.match_indexes
                        .get(peer_id)
                        .is_some_and(|match_index| *match_index >= entry.index)
                })
                .count();
            if replicated_count >= raft.majority() {
                raft.commit_index = entry.index;
                break;
            }
        }

        raft.log
            .iter()
            .filter(|entry| entry.index > previous_commit_index && entry.index <= raft.commit_index)
            .map(|entry| ConsensusAction::CommitBlock(entry.block_hash))
            .collect()
    }

    fn append_entries_actions_for_followers(raft: &RaftConsensus) -> Vec<ConsensusAction> {
        raft.participants
            .iter()
            .copied()
            .filter(|peer_id| *peer_id != raft.peer_id)
            .filter_map(|peer_id| Self::append_entries_action_for(raft, peer_id))
            .collect()
    }

    fn append_entries_action_for(raft: &RaftConsensus, peer_id: PeerId) -> Option<ConsensusAction> {
        let peer_match_index = raft.match_indexes.get(&peer_id).copied().unwrap_or(0);
        let prev_log_index = peer_match_index.min(raft.last_log_index());
        let prev_log_term = raft.term_at(prev_log_index)?;
        let entries = raft
            .log
            .iter()
            .filter(|entry| entry.index > prev_log_index)
            .take(MAX_APPEND_ENTRIES)
            .copied()
            .collect();

        Some(ConsensusAction::SendRaftAppendEntries {
            to: peer_id,
            term: raft.current_term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: raft.commit_index,
        })
    }
}

impl FollowerRole {
    fn handle_client_transaction(
        raft: &RaftConsensus,
        client_tx: SignedTransaction,
    ) -> Vec<ConsensusAction> {
        handle_non_leader_client_transaction(raft, client_tx)
    }

    fn handle_tick(raft: &mut RaftConsensus, now: Instant) -> Vec<ConsensusAction> {
        handle_election_tick(raft, now)
    }

    fn tick_outcome(raft: &mut RaftConsensus, now: Instant) -> TickOutcome {
        election_tick_outcome(raft, now)
    }

    fn handle_response_with_term(raft: &mut RaftConsensus, term: u64) -> Vec<ConsensusAction> {
        step_down_on_newer_term(raft, term);
        Vec::new()
    }
}

impl CandidateRole {
    fn handle_client_transaction(
        raft: &RaftConsensus,
        client_tx: SignedTransaction,
    ) -> Vec<ConsensusAction> {
        handle_non_leader_client_transaction(raft, client_tx)
    }

    fn handle_tick(raft: &mut RaftConsensus, now: Instant) -> Vec<ConsensusAction> {
        handle_election_tick(raft, now)
    }

    fn tick_outcome(raft: &mut RaftConsensus, now: Instant) -> TickOutcome {
        election_tick_outcome(raft, now)
    }

    fn handle_request_vote_response(
        raft: &mut RaftConsensus,
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    ) -> Vec<ConsensusAction> {
        raft.receive_vote(term, voter_id, vote_granted);
        Vec::new()
    }

    fn handle_append_entries_response(raft: &mut RaftConsensus, term: u64) -> Vec<ConsensusAction> {
        step_down_on_newer_term(raft, term);
        Vec::new()
    }

    fn request_vote_broadcast(raft: &RaftConsensus) -> ConsensusAction {
        ConsensusAction::Broadcast(MessageBody::RaftRequestVote {
            term: raft.current_term,
            candidate_id: raft.peer_id,
        })
    }
}

fn handle_non_leader_client_transaction(
    raft: &RaftConsensus,
    client_tx: SignedTransaction,
) -> Vec<ConsensusAction> {
    if let Some(leader_id) = raft.leader_id {
        return vec![ConsensusAction::Send {
            to: leader_id,
            body: MessageBody::ClientTransaction(client_tx),
        }];
    }
    vec![ConsensusAction::Reject(
        "Raft leader is unknown".to_string(),
    )]
}

fn handle_election_tick(raft: &mut RaftConsensus, now: Instant) -> Vec<ConsensusAction> {
    if election_tick_outcome(raft, now) == TickOutcome::ElectionStarted {
        return vec![CandidateRole::request_vote_broadcast(raft)];
    }
    Vec::new()
}

fn election_tick_outcome(raft: &mut RaftConsensus, now: Instant) -> TickOutcome {
    if now.duration_since(raft.last_heartbeat_received_at) >= raft.current_election_timeout {
        raft.start_election_at(now);
        return TickOutcome::ElectionStarted;
    }
    TickOutcome::None
}

fn step_down_on_newer_term(raft: &mut RaftConsensus, term: u64) {
    if term > raft.current_term {
        raft.step_down(term);
    }
}

fn entries_are_contiguous_after(prev_log_index: u64, entries: &[RaftLogEntry]) -> bool {
    entries
        .iter()
        .enumerate()
        .all(|(offset, entry)| entry.index == prev_log_index + offset as u64 + 1)
}

fn random_election_timeout(base: Duration, jitter: Duration) -> Duration {
    if jitter.is_zero() {
        return base;
    }

    let jitter_millis = jitter.as_millis();
    let jitter = if jitter_millis > u64::MAX as u128 {
        u64::MAX
    } else {
        jitter_millis as u64
    };
    let randomized_jitter = rand::rng().random_range(0..=jitter);
    base + Duration::from_millis(randomized_jitter)
}

pub fn handle_raft_input(raft: &mut RaftConsensus, input: ConsensusInput) -> Vec<ConsensusAction> {
    raft.handle_raft_input(input)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER, DEFAULT_HEARTBEAT_INTERVAL,
        RaftConsensus, RaftRole, TickOutcome, VoteResponse,
    };
    use crate::peer::PeerId;
    use crate::peer::consensus::RaftLogEntry;
    use crate::storage::BlockHash;
    use std::time::{Duration, Instant};

    fn create_consensus(now: Instant) -> RaftConsensus {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);
        consensus.receive_append_entries_at(0, PeerId::from(2), PeerId::from(2), now);
        consensus
    }

    fn hash(value: u8) -> BlockHash {
        BlockHash::new([value; 32])
    }

    fn log_entry(index: u64, term: u64) -> RaftLogEntry {
        RaftLogEntry {
            term,
            index,
            block_hash: hash(index as u8),
        }
    }

    #[test]
    fn starts_election_as_candidate_and_votes_for_self() {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);

        consensus.start_election_at(Instant::now());

        assert_eq!(consensus.current_term, 1);
        assert_eq!(consensus.role, RaftRole::Candidate);
        assert_eq!(consensus.voted_for, Some(PeerId::from(1)));
        assert_eq!(consensus.leader_id, None);
    }

    #[test]
    fn becomes_leader_after_majority_vote() {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);

        consensus.start_election_at(Instant::now());
        consensus.receive_vote(1, PeerId::from(2), true);

        assert_eq!(consensus.role, RaftRole::Leader);
        assert_eq!(consensus.leader_id, Some(PeerId::from(1)));
    }

    #[test]
    fn grants_one_vote_per_term() {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);

        assert_eq!(
            consensus.request_vote(1, PeerId::from(2)),
            VoteResponse::Granted
        );
        assert_eq!(
            consensus.request_vote(1, PeerId::from(3)),
            VoteResponse::Rejected
        );
        assert_eq!(consensus.voted_for, Some(PeerId::from(2)));
    }

    #[test]
    fn rejects_stale_term_vote_requests() {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);

        consensus.request_vote(2, PeerId::from(2));

        assert_eq!(
            consensus.request_vote(1, PeerId::from(3)),
            VoteResponse::Rejected
        );
        assert_eq!(consensus.current_term, 2);
    }

    #[test]
    fn newer_term_steps_candidate_down() {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);

        consensus.start_election_at(Instant::now());
        consensus.receive_vote(2, PeerId::from(2), true);

        assert_eq!(consensus.current_term, 2);
        assert_eq!(consensus.role, RaftRole::Follower);
        assert_eq!(consensus.voted_for, None);
        assert_eq!(consensus.leader_id, None);
    }

    #[test]
    fn append_entries_records_current_leader() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert!(consensus.receive_append_entries_at(1, PeerId::from(2), PeerId::from(2), now));

        assert_eq!(consensus.current_term, 1);
        assert_eq!(consensus.role, RaftRole::Follower);
        assert_eq!(consensus.leader_id, Some(PeerId::from(2)));
    }

    #[test]
    fn append_entries_rejects_impersonated_leader() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert!(!consensus.receive_append_entries_at(1, PeerId::from(2), PeerId::from(3), now));
        assert_eq!(consensus.leader_id, Some(PeerId::from(2)));
    }

    #[test]
    fn append_entries_rejects_non_participant_leader() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert!(!consensus.receive_append_entries_at(1, PeerId::from(99), PeerId::from(99), now));
        assert_eq!(consensus.leader_id, Some(PeerId::from(2)));
    }

    #[test]
    fn append_entries_rejects_different_leader_before_timeout() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert!(!consensus.receive_append_entries_at(
            1,
            PeerId::from(3),
            PeerId::from(3),
            now + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1)
        ));
        assert_eq!(consensus.leader_id, Some(PeerId::from(2)));
    }

    #[test]
    fn append_entries_accepts_different_leader_after_timeout() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert!(consensus.receive_append_entries_at(
            1,
            PeerId::from(3),
            PeerId::from(3),
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1)
        ));
        assert_eq!(consensus.leader_id, Some(PeerId::from(3)));
    }

    #[test]
    fn append_entries_from_same_leader_does_not_reset_election_timeout() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);
        let election_timeout = consensus.current_election_timeout;

        assert!(consensus.receive_append_entries_at(
            0,
            PeerId::from(2),
            PeerId::from(2),
            now + Duration::from_secs(1)
        ));

        assert_eq!(consensus.current_election_timeout, election_timeout);
    }

    #[test]
    fn randomized_election_timeout_is_within_expected_bounds() {
        for _ in 0..20 {
            let consensus = RaftConsensus::new(PeerId::from(1));

            assert!(consensus.current_election_timeout >= DEFAULT_ELECTION_TIMEOUT);
            assert!(
                consensus.current_election_timeout
                    <= DEFAULT_ELECTION_TIMEOUT + DEFAULT_ELECTION_TIMEOUT_JITTER
            );
        }
    }

    #[test]
    fn append_entries_resets_heartbeat_timer() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);
        let heartbeat_at = now + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1);

        assert!(consensus.receive_append_entries_at(
            1,
            PeerId::from(2),
            PeerId::from(2),
            heartbeat_at
        ));
        assert_eq!(
            consensus.tick(heartbeat_at + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1)),
            TickOutcome::None
        );
        assert_eq!(
            consensus.tick(
                heartbeat_at
                    + DEFAULT_ELECTION_TIMEOUT
                    + DEFAULT_ELECTION_TIMEOUT_JITTER
                    + Duration::from_secs(1)
            ),
            TickOutcome::ElectionStarted
        );
    }

    #[test]
    fn append_entries_rejects_non_contiguous_entries() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert!(
            consensus
                .append_entries(0, 0, vec![log_entry(1, 1), log_entry(3, 1)], 0)
                .is_none()
        );
        assert_eq!(consensus.last_log_index(), 0);
    }

    #[test]
    fn matching_index_returns_highest_entry_with_same_term() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);
        consensus.log = vec![log_entry(1, 1), log_entry(2, 1), log_entry(3, 2)];

        assert_eq!(
            consensus.matching_index_for(
                0,
                0,
                &[log_entry(1, 1), log_entry(2, 1), log_entry(3, 3)]
            ),
            2
        );
    }

    #[test]
    fn follower_tick_before_timeout_does_nothing() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert_eq!(
            consensus.tick(now + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1)),
            TickOutcome::None
        );
        assert_eq!(consensus.role, RaftRole::Follower);
    }

    #[test]
    fn follower_tick_after_timeout_starts_election() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        assert_eq!(
            consensus.tick(
                now + DEFAULT_ELECTION_TIMEOUT
                    + DEFAULT_ELECTION_TIMEOUT_JITTER
                    + Duration::from_secs(1)
            ),
            TickOutcome::ElectionStarted
        );
        assert_eq!(consensus.current_term, 1);
        assert_eq!(consensus.role, RaftRole::Candidate);
    }

    #[test]
    fn candidate_tick_after_timeout_starts_new_term() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        consensus.start_election_at(now);
        assert_eq!(
            consensus.tick(
                now + DEFAULT_ELECTION_TIMEOUT
                    + DEFAULT_ELECTION_TIMEOUT_JITTER
                    + Duration::from_secs(1)
            ),
            TickOutcome::ElectionStarted
        );

        assert_eq!(consensus.current_term, 2);
        assert_eq!(consensus.role, RaftRole::Candidate);
    }

    #[test]
    fn leader_tick_emits_heartbeat_when_due() {
        let now = Instant::now();
        let mut consensus = create_consensus(now);

        consensus.start_election_at(now);
        consensus.receive_vote(1, PeerId::from(2), true);

        assert_eq!(consensus.tick(now), TickOutcome::HeartbeatDue);
        assert_eq!(
            consensus.tick(now + DEFAULT_HEARTBEAT_INTERVAL - Duration::from_secs(1)),
            TickOutcome::None
        );
        assert_eq!(
            consensus.tick(now + DEFAULT_HEARTBEAT_INTERVAL),
            TickOutcome::HeartbeatDue
        );
    }
}
