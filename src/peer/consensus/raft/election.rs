use super::{RaftConsensus, RaftRole, VoteResponse};
use crate::peer::PeerId;
use rand::Rng;
use std::collections::HashSet;
use std::time::{Duration, Instant};

impl RaftConsensus {
    pub(super) fn update_participants(&mut self, known_peers: &[PeerId]) {
        let mut participants = HashSet::from_iter(known_peers.iter().copied());
        participants.insert(self.peer_id);
        self.participants = participants;
        self.votes_received
            .retain(|peer_id| self.participants.contains(peer_id));
        self.match_indexes
            .retain(|peer_id, _| self.participants.contains(peer_id));
    }

    pub(super) fn start_election_at(&mut self, now: Instant) {
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

    pub(super) fn request_vote(&mut self, term: u64, candidate_id: PeerId) -> VoteResponse {
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

    pub(super) fn receive_vote(&mut self, term: u64, voter_id: PeerId, granted: bool) {
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

    pub(super) fn receive_append_entries_at(
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

    pub(super) fn step_down(&mut self, term: u64) {
        self.current_term = term;
        self.role = RaftRole::Follower;
        self.voted_for = None;
        self.leader_id = None;
        self.votes_received.clear();
    }

    pub(super) fn step_down_on_newer_term(&mut self, term: u64) {
        if term > self.current_term {
            self.step_down(term);
        }
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

    pub(super) fn majority(&self) -> usize {
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

pub(super) fn random_election_timeout(base: Duration, jitter: Duration) -> Duration {
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
