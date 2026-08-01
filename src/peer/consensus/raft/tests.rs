use super::{
    DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER, DEFAULT_HEARTBEAT_INTERVAL,
    RaftConsensus, RaftRole, VoteResponse,
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
        now + DEFAULT_ELECTION_TIMEOUT + DEFAULT_ELECTION_TIMEOUT_JITTER + Duration::from_secs(1)
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

    assert!(consensus.receive_append_entries_at(1, PeerId::from(2), PeerId::from(2), heartbeat_at));
    assert!(
        consensus
            .on_tick(
                heartbeat_at + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1),
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        consensus
            .on_tick(
                heartbeat_at
                    + DEFAULT_ELECTION_TIMEOUT
                    + DEFAULT_ELECTION_TIMEOUT_JITTER
                    + Duration::from_secs(1),
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .len(),
        1
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
        consensus.matching_index_for(0, 0, &[log_entry(1, 1), log_entry(2, 1), log_entry(3, 3)]),
        2
    );
}

#[test]
fn follower_tick_before_timeout_does_nothing() {
    let now = Instant::now();
    let mut consensus = create_consensus(now);

    assert!(
        consensus
            .on_tick(
                now + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1),
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(consensus.role, RaftRole::Follower);
}

#[test]
fn follower_tick_after_timeout_starts_election() {
    let now = Instant::now();
    let mut consensus = create_consensus(now);

    assert_eq!(
        consensus
            .on_tick(
                now + DEFAULT_ELECTION_TIMEOUT
                    + DEFAULT_ELECTION_TIMEOUT_JITTER
                    + Duration::from_secs(1),
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .len(),
        1
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
        consensus
            .on_tick(
                now + DEFAULT_ELECTION_TIMEOUT
                    + DEFAULT_ELECTION_TIMEOUT_JITTER
                    + Duration::from_secs(1),
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .len(),
        1
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

    assert_eq!(
        consensus
            .on_tick(now, &[PeerId::from(2), PeerId::from(3)])
            .unwrap()
            .len(),
        2
    );
    assert!(
        consensus
            .on_tick(
                now + DEFAULT_HEARTBEAT_INTERVAL - Duration::from_secs(1),
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        consensus
            .on_tick(
                now + DEFAULT_HEARTBEAT_INTERVAL,
                &[PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .len(),
        2
    );
}
