#[allow(dead_code)]
pub mod raft;
pub(crate) mod raft_log_store;
mod voting;

use crate::peer::consensus::raft::RaftConsensus;
use crate::peer::consensus::raft_log_store::RaftLogStorage;
use crate::peer::{MessageBody, PeerId};
use crate::storage::BlockHash;
use crate::transactions::SignedTransaction;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

#[allow(unused_imports)]
pub use voting::{ConsensusOutcome, VotingConsensus};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub block_hash: BlockHash,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RaftRoleState {
    Follower,
    Candidate,
    Leader,
}

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

pub enum ConsensusInput {
    ClientTransactionReceived(SignedTransaction),
    NewBlockCreated {
        block_hash: BlockHash,
    },
    LocalBlockProposed {
        block_hash: BlockHash,
        known_peers: Vec<PeerId>,
    },
    BlockProposalValidated {
        block_hash: BlockHash,
        proposer: PeerId,
        valid: bool,
        known_peers: Vec<PeerId>,
    },
    BlockVoteReceived {
        block_hash: BlockHash,
        from: PeerId,
        approve: bool,
        known_peers: Vec<PeerId>,
    },
    Tick {
        now: Instant,
        known_peers: Vec<PeerId>,
    },
    RaftRequestVote {
        term: u64,
        candidate_id: PeerId,
        from: PeerId,
    },
    RaftRequestVoteResponse {
        term: u64,
        voter_id: PeerId,
        vote_granted: bool,
    },
    RaftAppendEntries {
        term: u64,
        leader_id: PeerId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
        from: PeerId,
        now: Instant,
    },
    RaftAppendEntriesResponse {
        term: u64,
        from: PeerId,
        success: bool,
        match_index: u64,
    },
}

pub enum ConsensusAction {
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
    Reject(String),
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
        raft_log_store: Box<dyn RaftLogStorage>,
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

    pub fn handle_input(&mut self, input: ConsensusInput) -> Vec<ConsensusAction> {
        match self {
            Self::Voting { peer_id, votings } => {
                voting::handle_voting_input(*peer_id, votings, input)
            }
            Self::Raft(raft) => raft::handle_raft_input(raft, input),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsensusAction, ConsensusEngine, ConsensusInput, ConsensusState, RaftLogEntry};
    use crate::crypto::KeyManager;
    use crate::peer::consensus::raft::{
        DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER, DEFAULT_HEARTBEAT_INTERVAL,
    };
    use crate::peer::consensus::raft_log_store::{FileRaftLogStore, RaftLogStorage};
    use crate::peer::{MessageBody, PeerId};
    use crate::storage::BlockHash;
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

        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));

        assert_eq!(outputs.len(), 2);
        assert!(matches!(
            &outputs[0],
            ConsensusAction::StageClientTransaction(client_tx) if *client_tx == transaction
        ));
        assert!(matches!(
            &outputs[1],
            ConsensusAction::BroadcastClientTransaction(client_tx) if *client_tx == transaction
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
            voting.handle_input(ConsensusInput::NewBlockCreated { block_hash })[0],
            ConsensusAction::ProposeBlock(hash) if hash == block_hash
        ));
        assert!(
            raft.handle_input(ConsensusInput::NewBlockCreated { block_hash })
                .is_empty()
        );
    }

    #[test]
    fn local_block_proposal_initializes_vote_without_side_effects() {
        let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));

        let actions = consensus.handle_input(ConsensusInput::LocalBlockProposed {
            block_hash: hash(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)],
        });

        assert!(actions.is_empty());
    }

    #[test]
    fn local_block_proposal_emits_commit_when_self_vote_reaches_threshold() {
        let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
        let block_hash = hash(8);

        let actions = consensus.handle_input(ConsensusInput::LocalBlockProposed {
            block_hash,
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });

        assert_eq!(actions.len(), 2);
        assert!(matches!(actions[0], ConsensusAction::CommitBlock(hash) if hash == block_hash));
        assert!(matches!(
            actions[1],
            ConsensusAction::Broadcast(MessageBody::BlockApproved { block_hash: hash })
                if hash == block_hash
        ));
    }

    #[test]
    fn received_votes_emit_commit_and_broadcast_actions_at_threshold() {
        let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
        let block_hash = hash(2);
        let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];

        consensus.handle_input(ConsensusInput::LocalBlockProposed {
            block_hash,
            known_peers: known_peers.clone(),
        });
        assert!(
            consensus
                .handle_input(ConsensusInput::BlockVoteReceived {
                    block_hash,
                    from: PeerId::from(2),
                    approve: true,
                    known_peers: known_peers.clone(),
                })
                .is_empty()
        );

        let outputs = consensus.handle_input(ConsensusInput::BlockVoteReceived {
            block_hash,
            from: PeerId::from(3),
            approve: true,
            known_peers,
        });
        assert_eq!(outputs.len(), 2);
        assert!(matches!(outputs[0], ConsensusAction::CommitBlock(hash) if hash == block_hash));
        assert!(matches!(
            outputs[1],
            ConsensusAction::Broadcast(MessageBody::BlockApproved { block_hash: hash })
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
                .handle_input(ConsensusInput::BlockVoteReceived {
                    block_hash,
                    from: PeerId::from(2),
                    approve: true,
                    known_peers: known_peers.clone(),
                })
                .is_empty()
        );
        assert!(
            consensus
                .handle_input(ConsensusInput::BlockVoteReceived {
                    block_hash,
                    from: PeerId::from(2),
                    approve: true,
                    known_peers,
                })
                .is_empty()
        );
    }

    #[test]
    fn validated_rejected_proposal_emits_rollback_and_broadcast_actions() {
        let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
        let block_hash = hash(4);

        let outputs = consensus.handle_input(ConsensusInput::BlockProposalValidated {
            block_hash,
            proposer: PeerId::from(2),
            valid: false,
            known_peers: vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)],
        });
        assert_eq!(outputs.len(), 2);
        assert!(matches!(outputs[0], ConsensusAction::RollbackBlock(hash) if hash == block_hash));
        assert!(matches!(
            outputs[1],
            ConsensusAction::Broadcast(MessageBody::BlockReject { block_hash: hash })
                if hash == block_hash
        ));
    }

    #[test]
    fn raft_election_timeout_broadcasts_vote_request() {
        let now = Instant::now();
        let mut consensus = create_raft_consensus();

        assert!(
            consensus
                .handle_input(ConsensusInput::Tick {
                    now: now + DEFAULT_ELECTION_TIMEOUT - Duration::from_secs(1),
                    known_peers: vec![PeerId::from(2), PeerId::from(3)],
                })
                .is_empty()
        );

        let outputs = consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            outputs[0],
            ConsensusAction::Broadcast(MessageBody::RaftRequestVote {
                term: 1,
                candidate_id,
            }) if candidate_id == PeerId::from(1)
        ));
    }

    #[test]
    fn raft_vote_request_sends_direct_response() {
        let mut consensus = ConsensusEngine::new_raft(PeerId::from(1));

        let outputs = consensus.handle_input(ConsensusInput::RaftRequestVote {
            term: 1,
            candidate_id: PeerId::from(2),
            from: PeerId::from(2),
        });
        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusAction::Send {
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
                .handle_input(ConsensusInput::Tick {
                    now,
                    known_peers: vec![PeerId::from(2), PeerId::from(3)],
                })
                .is_empty()
        );
        let heartbeat_outputs = consensus.handle_input(ConsensusInput::RaftAppendEntries {
            term: 1,
            leader_id: PeerId::from(2),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
            from: PeerId::from(2),
            now,
        });
        assert_eq!(heartbeat_outputs.len(), 1);
        assert!(matches!(
            &heartbeat_outputs[0],
            ConsensusAction::Send {
                to,
                body: MessageBody::RaftAppendEntriesResponse {
                    term: 1,
                    success: true,
                    match_index: 0,
                },
            } if *to == PeerId::from(2)
        ));

        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));
        assert!(matches!(
            &outputs[0],
            ConsensusAction::Send {
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
        let block_hash = hash(21);

        consensus.handle_input(ConsensusInput::Tick {
            now,
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });

        let outputs = consensus.handle_input(ConsensusInput::RaftAppendEntries {
            term: 1,
            leader_id: PeerId::from(2),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 1,
                block_hash,
            }],
            leader_commit: 0,
            from: PeerId::from(2),
            now,
        });

        assert!(outputs.iter().any(|action| matches!(
            action,
            ConsensusAction::StageRaftEntries(entries)
                if entries.len() == 1
                    && entries[0].term == 1
                    && entries[0].index == 1
                    && entries[0].block_hash == block_hash
        )));

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
    fn raft_follower_forwards_client_transaction_to_known_leader() {
        let now = Instant::now();
        let mut consensus = create_raft_consensus();
        let transaction = create_test_transaction(&KeyManager::create_key());

        consensus.handle_input(ConsensusInput::Tick {
            now,
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftAppendEntries {
            term: 1,
            leader_id: PeerId::from(2),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
            from: PeerId::from(2),
            now,
        });
        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusAction::Send {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });

        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));
        assert!(matches!(
            &outputs[0],
            ConsensusAction::Reject(message) if message == "Raft leader is unknown"
        ));

        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });
        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusAction::StageClientTransaction(client_tx) if *client_tx == transaction
        ));
    }

    #[test]
    fn raft_follower_rejects_client_transaction_without_known_leader() {
        let mut consensus = ConsensusEngine::new_raft(PeerId::from(1));
        let transaction = create_test_transaction(&KeyManager::create_key());

        let outputs =
            consensus.handle_input(ConsensusInput::ClientTransactionReceived(transaction));

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusAction::Reject(message) if message == "Raft leader is unknown"
        ));
    }

    #[test]
    fn raft_leader_appends_new_block_to_log() {
        let now = Instant::now();
        let mut consensus = create_raft_consensus();
        let block_hash = hash(7);

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });

        let outputs = consensus.handle_input(ConsensusInput::NewBlockCreated { block_hash });

        assert_eq!(outputs.len(), 2);
        assert!(outputs.iter().any(|action| matches!(
            action,
            ConsensusAction::SendRaftAppendEntries {
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
            ConsensusAction::SendRaftAppendEntries {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });
        consensus.handle_input(ConsensusInput::NewBlockCreated { block_hash });

        let outputs = consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: true,
            match_index: 1,
        });

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            outputs[0],
            ConsensusAction::CommitBlock(hash) if hash == block_hash
        ));
    }

    #[test]
    fn raft_leader_sends_entries_after_each_followers_match_index() {
        let now = Instant::now();
        let mut consensus = create_raft_consensus();
        let first_block_hash = hash(10);
        let second_block_hash = hash(11);

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });
        consensus.handle_input(ConsensusInput::NewBlockCreated {
            block_hash: first_block_hash,
        });
        consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: true,
            match_index: 1,
        });

        let outputs = consensus.handle_input(ConsensusInput::NewBlockCreated {
            block_hash: second_block_hash,
        });

        assert!(outputs.iter().any(|action| matches!(
            action,
            ConsensusAction::SendRaftAppendEntries {
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
            ConsensusAction::SendRaftAppendEntries {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });

        let mut outputs = Vec::new();
        for block_number in 1..=6 {
            outputs = consensus.handle_input(ConsensusInput::NewBlockCreated {
                block_hash: hash(block_number),
            });
        }

        assert!(outputs.iter().any(|action| matches!(
            action,
            ConsensusAction::SendRaftAppendEntries {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });

        for block_number in 1..=6 {
            consensus.handle_input(ConsensusInput::NewBlockCreated {
                block_hash: hash(block_number),
            });
        }
        consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: true,
            match_index: 4,
        });

        let outputs = consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + DEFAULT_HEARTBEAT_INTERVAL
                + Duration::from_secs(2),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });

        assert!(outputs.iter().any(|action| matches!(
            action,
            ConsensusAction::SendRaftAppendEntries {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });
        consensus.handle_input(ConsensusInput::NewBlockCreated { block_hash });
        consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: true,
            match_index: 1,
        });

        let outputs = consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + DEFAULT_HEARTBEAT_INTERVAL
                + Duration::from_secs(2),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });

        assert!(outputs.iter().any(|action| matches!(
            action,
            ConsensusAction::SendRaftAppendEntries {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });
        consensus.handle_input(ConsensusInput::NewBlockCreated {
            block_hash: first_block_hash,
        });
        consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: true,
            match_index: 1,
        });
        consensus.handle_input(ConsensusInput::NewBlockCreated {
            block_hash: second_block_hash,
        });

        let outputs = consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: false,
            match_index: 1,
        });

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusAction::SendRaftAppendEntries {
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

        consensus.handle_input(ConsensusInput::Tick {
            now: now
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_secs(1),
            known_peers: vec![PeerId::from(2), PeerId::from(3)],
        });
        consensus.handle_input(ConsensusInput::RaftRequestVoteResponse {
            term: 1,
            voter_id: PeerId::from(2),
            vote_granted: true,
        });
        consensus.handle_input(ConsensusInput::NewBlockCreated { block_hash });
        consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: true,
            match_index: 1,
        });

        let outputs = consensus.handle_input(ConsensusInput::RaftAppendEntriesResponse {
            term: 1,
            from: PeerId::from(2),
            success: false,
            match_index: 1,
        });

        assert!(outputs.is_empty());
    }
}
