use crate::config::{
    DEFAULT_RAFT_ELECTION_TIMEOUT, DEFAULT_RAFT_ELECTION_TIMEOUT_JITTER,
    DEFAULT_RAFT_HEARTBEAT_INTERVAL,
};
use crate::consensus::{ConsensusInput, ConsensusOutput};
use crate::peer::MessageBody;
use crate::peer::PeerId;
use rand::Rng;
use std::collections::HashSet;
use std::time::{Duration, Instant};

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = DEFAULT_RAFT_HEARTBEAT_INTERVAL;
pub const DEFAULT_ELECTION_TIMEOUT: Duration = DEFAULT_RAFT_ELECTION_TIMEOUT;
pub const DEFAULT_ELECTION_TIMEOUT_JITTER: Duration = DEFAULT_RAFT_ELECTION_TIMEOUT_JITTER;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickOutcome {
    None,
    ElectionStarted,
    HeartbeatDue,
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
}

impl RaftConsensus {
    pub fn new(peer_id: PeerId) -> Self {
        let participants = HashSet::from([peer_id]);
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
        }
    }

    fn accepts_block_proposals(&self) -> bool {
        self.role == RaftRole::Leader
    }

    fn update_participants(&mut self, known_peers: &[PeerId]) {
        let mut participants = HashSet::from_iter(known_peers.iter().copied());
        participants.insert(self.peer_id);
        self.participants = participants;
        self.votes_received
            .retain(|peer_id| self.participants.contains(peer_id));
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
        if self.role == RaftRole::Leader {
            if self.last_heartbeat_sent_at.is_none_or(|last_sent_at| {
                now.duration_since(last_sent_at) >= self.heartbeat_interval
            }) {
                self.last_heartbeat_sent_at = Some(now);
                return TickOutcome::HeartbeatDue;
            }
            return TickOutcome::None;
        }

        if now.duration_since(self.last_heartbeat_received_at) >= self.current_election_timeout {
            self.start_election_at(now);
            return TickOutcome::ElectionStarted;
        }

        TickOutcome::None
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

pub fn handle_raft_input(raft: &mut RaftConsensus, input: ConsensusInput) -> Vec<ConsensusOutput> {
    match input {
        ConsensusInput::ClientTransactionReceived(client_tx) => {
            if raft.role == RaftRole::Leader {
                vec![ConsensusOutput::ApplyClientTransaction(client_tx)]
            } else if let Some(leader_id) = raft.leader_id {
                vec![ConsensusOutput::Send {
                    to: leader_id,
                    body: MessageBody::ClientTransaction(client_tx),
                }]
            } else {
                vec![ConsensusOutput::Reject(
                    "Raft leader is unknown".to_string(),
                )]
            }
        }
        ConsensusInput::NewBlockCreated { block_hash } => {
            if raft.accepts_block_proposals() {
                vec![ConsensusOutput::ProposeBlock(block_hash)]
            } else {
                Vec::new()
            }
        }
        ConsensusInput::Tick {
            now, known_peers, ..
        } => {
            raft.update_participants(&known_peers);
            match raft.tick(now) {
                TickOutcome::ElectionStarted => {
                    vec![ConsensusOutput::Broadcast(MessageBody::RaftRequestVote {
                        term: raft.current_term,
                        candidate_id: raft.peer_id,
                    })]
                }
                TickOutcome::HeartbeatDue => {
                    vec![ConsensusOutput::Broadcast(MessageBody::RaftAppendEntries {
                        term: raft.current_term,
                        leader_id: raft.peer_id,
                    })]
                }
                TickOutcome::None => Vec::new(),
            }
        }
        ConsensusInput::RaftRequestVote {
            term,
            candidate_id,
            from,
        } => {
            let response = raft.request_vote(term, candidate_id);
            vec![ConsensusOutput::Send {
                to: from,
                body: MessageBody::RaftRequestVoteResponse {
                    term: raft.current_term,
                    vote_granted: response == VoteResponse::Granted,
                },
            }]
        }
        ConsensusInput::RaftRequestVoteResponse {
            term,
            voter_id,
            vote_granted,
        } => {
            raft.receive_vote(term, voter_id, vote_granted);
            Vec::new()
        }
        ConsensusInput::RaftAppendEntries {
            term,
            leader_id,
            from,
            now,
        } => {
            raft.receive_append_entries_at(term, leader_id, from, now);
            Vec::new()
        }
        ConsensusInput::LocalBlockProposed { .. }
        | ConsensusInput::BlockProposalValidated { .. }
        | ConsensusInput::BlockVoteReceived { .. } => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER, DEFAULT_HEARTBEAT_INTERVAL,
        RaftConsensus, RaftRole, TickOutcome, VoteResponse,
    };
    use crate::peer::PeerId;
    use std::time::{Duration, Instant};

    fn create_consensus(now: Instant) -> RaftConsensus {
        let mut consensus = RaftConsensus::new(PeerId::from(1));
        consensus.update_participants(&[PeerId::from(2), PeerId::from(3)]);
        consensus.receive_append_entries_at(0, PeerId::from(2), PeerId::from(2), now);
        consensus
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
