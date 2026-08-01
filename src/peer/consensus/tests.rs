use super::{ConsensusEffect, ConsensusEngine, ConsensusState, RaftLogEntry, ValidatedRaftBlock};
use crate::crypto::KeyManager;
use crate::peer::consensus::raft::{
    DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER, DEFAULT_HEARTBEAT_INTERVAL,
};
use crate::peer::consensus::raft_log_store::{FileRaftLogStore, RaftLogStorage};
use crate::peer::{MessageBody, PeerId};
use crate::storage::{BlockFile, BlockHash, EMPTY_HASH};
use crate::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};
use k256::ecdsa::SigningKey;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn hash(value: u8) -> BlockHash {
    BlockHash::new([value; 32])
}

fn create_raft_consensus() -> ConsensusEngine {
    ConsensusEngine::new_raft(PeerId::from(1))
}

fn elect_raft_leader(consensus: &mut ConsensusEngine, now: Instant) {
    consensus
        .on_tick(
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            vec![PeerId::from(2), PeerId::from(3)],
        )
        .unwrap();
    consensus
        .on_request_vote_response(1, PeerId::from(2), true)
        .unwrap();
}

fn create_test_transaction(signing_key: &SigningKey) -> SignedTransaction {
    let transaction = Transaction {
        operation: Operation::AddCoin {
            asset_type: AssetType::USDT,
            amount: 10,
        },
        metadata: Metadata {
            timestamp_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            sequence_number: 1,
        },
    };

    SignedTransaction::new(transaction, signing_key)
}

#[test]
fn voting_consensus_reports_voting_state() {
    let consensus = ConsensusEngine::new_voting(PeerId::from(1));

    assert_eq!(consensus.state(), ConsensusState::Voting);
    assert_eq!(
        serde_json::to_value(consensus.state()).unwrap()["mode"],
        "voting"
    );
}

#[test]
fn voting_stages_and_broadcasts_client_transaction() {
    let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
    let transaction = create_test_transaction(&KeyManager::create_key());

    let outputs = consensus
        .on_client_transaction(transaction.clone())
        .unwrap();

    assert_eq!(outputs.len(), 2);
    assert!(matches!(
        &outputs[0],
        ConsensusEffect::StageClientTransaction(client_tx) if *client_tx == transaction
    ));
    assert!(matches!(
        &outputs[1],
        ConsensusEffect::BroadcastClientTransaction(client_tx) if *client_tx == transaction
    ));
}

#[test]
fn only_raft_requires_tick() {
    assert!(!ConsensusEngine::new_voting(PeerId::from(1)).requires_tick());
    assert!(ConsensusEngine::new_raft(PeerId::from(1)).requires_tick());
}

#[test]
fn new_block_created_proposes_for_voting_only_by_default() {
    let block_hash = hash(9);

    let mut voting = ConsensusEngine::new_voting(PeerId::from(1));
    let mut raft = ConsensusEngine::new_raft(PeerId::from(1));

    assert!(matches!(
        voting.on_block_created(block_hash).unwrap()[0],
        ConsensusEffect::ProposeBlock(hash) if hash == block_hash
    ));
    assert!(raft.on_block_created(block_hash).unwrap().is_empty());
}

#[test]
fn local_block_proposal_initializes_vote_without_side_effects() {
    let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));

    let actions = consensus
        .on_local_block_proposed(
            hash(1),
            vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)],
        )
        .unwrap();

    assert!(actions.is_empty());
}

#[test]
fn local_block_proposal_emits_commit_when_self_vote_reaches_threshold() {
    let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
    let block_hash = hash(8);

    let actions = consensus
        .on_local_block_proposed(block_hash, vec![PeerId::from(2), PeerId::from(3)])
        .unwrap();

    assert_eq!(actions.len(), 2);
    assert!(matches!(actions[0], ConsensusEffect::CommitBlock(hash) if hash == block_hash));
    assert!(matches!(
        actions[1],
        ConsensusEffect::Broadcast(MessageBody::BlockApproved { block_hash: hash })
            if hash == block_hash
    ));
}

#[test]
fn received_votes_emit_commit_and_broadcast_actions_at_threshold() {
    let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
    let block_hash = hash(2);
    let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];

    consensus
        .on_local_block_proposed(block_hash, known_peers.clone())
        .unwrap();
    assert!(
        consensus
            .on_block_vote(block_hash, PeerId::from(2), true, known_peers.clone(),)
            .unwrap()
            .is_empty()
    );

    let outputs = consensus
        .on_block_vote(block_hash, PeerId::from(3), true, known_peers)
        .unwrap();
    assert_eq!(outputs.len(), 2);
    assert!(matches!(outputs[0], ConsensusEffect::CommitBlock(hash) if hash == block_hash));
    assert!(matches!(
        outputs[1],
        ConsensusEffect::Broadcast(MessageBody::BlockApproved { block_hash: hash })
            if hash == block_hash
    ));
}

#[test]
fn duplicate_votes_do_not_emit_actions() {
    let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
    let block_hash = hash(3);
    let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];

    assert!(
        consensus
            .on_block_vote(block_hash, PeerId::from(2), true, known_peers.clone(),)
            .unwrap()
            .is_empty()
    );
    assert!(
        consensus
            .on_block_vote(block_hash, PeerId::from(2), true, known_peers,)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn validated_rejected_proposal_emits_rollback_and_broadcast_actions() {
    let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
    let block_hash = hash(4);

    let outputs = consensus
        .on_block_proposal_validated(
            block_hash,
            PeerId::from(2),
            false,
            vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)],
        )
        .unwrap();
    assert_eq!(outputs.len(), 2);
    assert!(matches!(outputs[0], ConsensusEffect::RollbackBlock(hash) if hash == block_hash));
    assert!(matches!(
        outputs[1],
        ConsensusEffect::Broadcast(MessageBody::BlockReject { block_hash: hash })
            if hash == block_hash
    ));
}

#[test]
fn raft_election_timeout_broadcasts_vote_request() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();

    assert!(
        consensus
            .on_tick(
                now + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1),
                vec![PeerId::from(2), PeerId::from(3)],
            )
            .unwrap()
            .is_empty()
    );

    let outputs = consensus
        .on_tick(
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            vec![PeerId::from(2), PeerId::from(3)],
        )
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        outputs[0],
        ConsensusEffect::Broadcast(MessageBody::RaftRequestVote {
            term: 1,
            candidate_id,
        }) if candidate_id == PeerId::from(1)
    ));
}

#[test]
fn raft_vote_request_sends_direct_response() {
    let mut consensus = ConsensusEngine::new_raft(PeerId::from(1));

    let outputs = consensus
        .on_request_vote(1, PeerId::from(2), PeerId::from(2))
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        &outputs[0],
        ConsensusEffect::Send {
            to,
            body: MessageBody::RaftRequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        } if *to == PeerId::from(2)
    ));
}

#[test]
fn raft_append_entries_records_leader() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let transaction = create_test_transaction(&KeyManager::create_key());

    assert!(
        consensus
            .on_tick(now, vec![PeerId::from(2), PeerId::from(3)])
            .unwrap()
            .is_empty()
    );
    let heartbeat_outputs = consensus
        .on_append_entries(
            1,
            PeerId::from(2),
            0,
            0,
            Vec::new(),
            0,
            PeerId::from(2),
            now,
        )
        .unwrap();
    assert_eq!(heartbeat_outputs.len(), 1);
    assert!(matches!(
        &heartbeat_outputs[0],
        ConsensusEffect::Send {
            to,
            body: MessageBody::RaftAppendEntriesResponse {
                term: 1,
                success: true,
                match_index: 0,
            },
        } if *to == PeerId::from(2)
    ));

    let outputs = consensus
        .on_client_transaction(transaction.clone())
        .unwrap();
    assert!(matches!(
        &outputs[0],
        ConsensusEffect::Send {
            to,
            body: MessageBody::ClientTransaction(client_tx),
        } if *to == PeerId::from(2) && *client_tx == transaction
    ));
}

#[test]
fn raft_append_entries_persists_accepted_entries() {
    let now = Instant::now();
    let dir = std::env::temp_dir().join(format!(
        "rustchain_raft_append_entries_persist_{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    let raft_log_store = FileRaftLogStore::new(&dir);
    let mut consensus =
        ConsensusEngine::new_raft_with_storage(PeerId::from(1), Box::new(raft_log_store), 0)
            .unwrap();
    let block_file = BlockFile::create(Vec::new(), EMPTY_HASH, 1);
    let block_hash = block_file.hash;

    consensus
        .on_tick(now, vec![PeerId::from(2), PeerId::from(3)])
        .unwrap();

    let outputs = consensus
        .on_append_entries(
            1,
            PeerId::from(2),
            0,
            0,
            vec![ValidatedRaftBlock {
                entry: RaftLogEntry {
                    term: 1,
                    index: 1,
                    block_hash,
                },
                block_file,
            }],
            0,
            PeerId::from(2),
            now,
        )
        .unwrap();

    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::StageRaftEntries(entries)
            if entries.len() == 1
                && entries[0].term == 1
                && entries[0].index == 1
                && entries[0].block_hash == block_hash
    )));
    assert_eq!(
        consensus.take_pending_raft_block(&block_hash).unwrap().hash,
        block_hash
    );

    let restored_log = FileRaftLogStore::new(&dir).load().unwrap();
    assert_eq!(
        restored_log,
        vec![RaftLogEntry {
            term: 1,
            index: 1,
            block_hash,
        }]
    );
    fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn raft_rejected_append_entries_does_not_retain_validated_block() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let block_file = BlockFile::create(Vec::new(), EMPTY_HASH, 2);
    let block_hash = block_file.hash;

    consensus
        .on_tick(now, vec![PeerId::from(2), PeerId::from(3)])
        .unwrap();
    let effects = consensus
        .on_append_entries(
            1,
            PeerId::from(2),
            1,
            1,
            vec![ValidatedRaftBlock {
                entry: RaftLogEntry {
                    term: 1,
                    index: 2,
                    block_hash,
                },
                block_file,
            }],
            0,
            PeerId::from(2),
            now,
        )
        .unwrap();

    assert!(matches!(
        &effects[0],
        ConsensusEffect::Send {
            body: MessageBody::RaftAppendEntriesResponse { success: false, .. },
            ..
        }
    ));
    assert!(consensus.take_pending_raft_block(&block_hash).is_none());
}

#[test]
fn raft_follower_forwards_client_transaction_to_known_leader() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let transaction = create_test_transaction(&KeyManager::create_key());

    consensus
        .on_tick(now, vec![PeerId::from(2), PeerId::from(3)])
        .unwrap();
    consensus
        .on_append_entries(
            1,
            PeerId::from(2),
            0,
            0,
            Vec::new(),
            0,
            PeerId::from(2),
            now,
        )
        .unwrap();
    let outputs = consensus
        .on_client_transaction(transaction.clone())
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        &outputs[0],
        ConsensusEffect::Send {
            to,
            body: MessageBody::ClientTransaction(client_tx),
        } if *to == PeerId::from(2) && *client_tx == transaction
    ));
}

#[test]
fn raft_leader_stages_client_transaction_without_broadcasting_it() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let transaction = create_test_transaction(&KeyManager::create_key());

    consensus
        .on_tick(
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            vec![PeerId::from(2), PeerId::from(3)],
        )
        .unwrap();

    let Err(error) = consensus.on_client_transaction(transaction.clone()) else {
        panic!("transaction must be rejected while the Raft leader is unknown");
    };
    assert_eq!(error, "Raft leader is unknown");

    consensus
        .on_request_vote_response(1, PeerId::from(2), true)
        .unwrap();
    let outputs = consensus
        .on_client_transaction(transaction.clone())
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        &outputs[0],
        ConsensusEffect::StageClientTransaction(client_tx) if *client_tx == transaction
    ));
}

#[test]
fn raft_follower_rejects_client_transaction_without_known_leader() {
    let mut consensus = ConsensusEngine::new_raft(PeerId::from(1));
    let transaction = create_test_transaction(&KeyManager::create_key());

    let Err(error) = consensus.on_client_transaction(transaction) else {
        panic!("transaction must be rejected while the Raft leader is unknown");
    };
    assert_eq!(error, "Raft leader is unknown");
}

#[test]
fn raft_leader_appends_new_block_to_log() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let block_hash = hash(7);

    consensus
        .on_tick(
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            vec![PeerId::from(2), PeerId::from(3)],
        )
        .unwrap();
    consensus
        .on_request_vote_response(1, PeerId::from(2), true)
        .unwrap();

    let outputs = consensus.on_block_created(block_hash).unwrap();

    assert_eq!(outputs.len(), 2);
    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
        } if *to == PeerId::from(2)
            && entries.len() == 1
            && entries[0].term == 1
            && entries[0].index == 1
            && entries[0].block_hash == block_hash
    )));
    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            term: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 0,
        } if *to == PeerId::from(3)
            && entries.len() == 1
            && entries[0].term == 1
            && entries[0].index == 1
            && entries[0].block_hash == block_hash
    )));
}

#[test]
fn raft_leader_commits_after_majority_append_response() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let block_hash = hash(8);

    elect_raft_leader(&mut consensus, now);
    consensus.on_block_created(block_hash).unwrap();

    let outputs = consensus
        .on_append_entries_response(1, PeerId::from(2), true, 1)
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        outputs[0],
        ConsensusEffect::CommitBlock(hash) if hash == block_hash
    ));
}

#[test]
fn raft_leader_sends_entries_after_each_followers_match_index() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let first_block_hash = hash(10);
    let second_block_hash = hash(11);

    elect_raft_leader(&mut consensus, now);
    consensus.on_block_created(first_block_hash).unwrap();
    consensus
        .on_append_entries_response(1, PeerId::from(2), true, 1)
        .unwrap();

    let outputs = consensus.on_block_created(second_block_hash).unwrap();

    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            prev_log_index: 1,
            prev_log_term: 1,
            entries,
            ..
        } if *to == PeerId::from(2)
            && entries.len() == 1
            && entries[0].index == 2
            && entries[0].block_hash == second_block_hash
    )));
    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            ..
        } if *to == PeerId::from(3)
            && entries.len() == 2
            && entries[0].index == 1
            && entries[0].block_hash == first_block_hash
            && entries[1].index == 2
            && entries[1].block_hash == second_block_hash
    )));
}

#[test]
fn raft_leader_limits_append_entries_batch_size() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();

    elect_raft_leader(&mut consensus, now);

    let mut outputs = Vec::new();
    for block_number in 1..=6 {
        outputs = consensus.on_block_created(hash(block_number)).unwrap();
    }

    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            ..
        } if *to == PeerId::from(3)
            && entries.len() == 5
            && entries[0].index == 1
            && entries[4].index == 5
    )));
}

#[test]
fn raft_leader_sends_missing_entries_on_tick() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();

    elect_raft_leader(&mut consensus, now);

    for block_number in 1..=6 {
        consensus.on_block_created(hash(block_number)).unwrap();
    }
    consensus
        .on_append_entries_response(1, PeerId::from(2), true, 4)
        .unwrap();

    let outputs = consensus
        .on_tick(
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + DEFAULT_HEARTBEAT_INTERVAL
                + Duration::from_secs(2),
            vec![PeerId::from(2), PeerId::from(3)],
        )
        .unwrap();

    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            prev_log_index: 4,
            entries,
            ..
        } if *to == PeerId::from(2)
            && entries.len() == 2
            && entries[0].index == 5
            && entries[1].index == 6
    )));
}

#[test]
fn raft_leader_sends_empty_append_entries_on_tick_when_follower_is_caught_up() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let block_hash = hash(12);

    elect_raft_leader(&mut consensus, now);
    consensus.on_block_created(block_hash).unwrap();
    consensus
        .on_append_entries_response(1, PeerId::from(2), true, 1)
        .unwrap();

    let outputs = consensus
        .on_tick(
            now + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + DEFAULT_HEARTBEAT_INTERVAL
                + Duration::from_secs(2),
            vec![PeerId::from(2), PeerId::from(3)],
        )
        .unwrap();

    assert!(outputs.iter().any(|action| matches!(
        action,
        ConsensusEffect::SendRaftAppendEntries {
            to,
            prev_log_index: 1,
            entries,
            ..
        } if *to == PeerId::from(2) && entries.is_empty()
    )));
}

#[test]
fn raft_leader_retries_after_failed_append_response_match_index() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let first_block_hash = hash(12);
    let second_block_hash = hash(13);

    elect_raft_leader(&mut consensus, now);
    consensus.on_block_created(first_block_hash).unwrap();
    consensus
        .on_append_entries_response(1, PeerId::from(2), true, 1)
        .unwrap();
    consensus.on_block_created(second_block_hash).unwrap();

    let outputs = consensus
        .on_append_entries_response(1, PeerId::from(2), false, 1)
        .unwrap();

    assert_eq!(outputs.len(), 1);
    assert!(matches!(
        &outputs[0],
        ConsensusEffect::SendRaftAppendEntries {
            to,
            prev_log_index: 1,
            prev_log_term: 1,
            entries,
            ..
        } if *to == PeerId::from(2)
            && entries.len() == 1
            && entries[0].block_hash == second_block_hash
    ));
}

#[test]
fn raft_leader_does_not_retry_empty_append_entries_after_failed_response() {
    let now = Instant::now();
    let mut consensus = create_raft_consensus();
    let block_hash = hash(14);

    elect_raft_leader(&mut consensus, now);
    consensus.on_block_created(block_hash).unwrap();
    consensus
        .on_append_entries_response(1, PeerId::from(2), true, 1)
        .unwrap();

    let outputs = consensus
        .on_append_entries_response(1, PeerId::from(2), false, 1)
        .unwrap();

    assert!(outputs.is_empty());
}
