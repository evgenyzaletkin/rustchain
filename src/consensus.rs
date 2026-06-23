#[allow(dead_code)]
pub mod raft;
mod voting;

use crate::consensus::raft::RaftConsensus;
use crate::peer::{MessageBody, PeerId};
use crate::storage::BlockHash;
use crate::transactions::SignedTransaction;
use std::collections::HashMap;
use std::time::Instant;

#[allow(unused_imports)]
pub use voting::{ConsensusOutcome, VotingConsensus};

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
        from: PeerId,
        now: Instant,
    },
}

pub enum ConsensusOutput {
    ApplyClientTransaction(SignedTransaction),
    ProposeBlock(BlockHash),
    CommitBlock(BlockHash),
    RollbackBlock(BlockHash),
    Broadcast(MessageBody),
    Send { to: PeerId, body: MessageBody },
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

    pub fn requires_tick(&self) -> bool {
        matches!(self, Self::Raft(_))
    }

    pub fn handle_input(&mut self, input: ConsensusInput) -> Vec<ConsensusOutput> {
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
    use super::{ConsensusEngine, ConsensusInput, ConsensusOutput};
    use crate::consensus::raft::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER};
    use crate::crypto::KeyManager;
    use crate::peer::{MessageBody, PeerId};
    use crate::storage::BlockHash;
    use crate::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};
    use k256::ecdsa::SigningKey;
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
    fn voting_applies_client_transaction() {
        let mut consensus = ConsensusEngine::new_voting(PeerId::from(1));
        let transaction = create_test_transaction(&KeyManager::create_key());

        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusOutput::ApplyClientTransaction(client_tx) if *client_tx == transaction
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
            ConsensusOutput::ProposeBlock(hash) if hash == block_hash
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
        assert!(matches!(outputs[0], ConsensusOutput::CommitBlock(hash) if hash == block_hash));
        assert!(matches!(
            outputs[1],
            ConsensusOutput::Broadcast(MessageBody::BlockApproved { block_hash: hash })
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
        assert!(matches!(outputs[0], ConsensusOutput::RollbackBlock(hash) if hash == block_hash));
        assert!(matches!(
            outputs[1],
            ConsensusOutput::Broadcast(MessageBody::BlockReject { block_hash: hash })
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
            ConsensusOutput::Broadcast(MessageBody::RaftRequestVote {
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
            ConsensusOutput::Send {
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
        assert!(
            consensus
                .handle_input(ConsensusInput::RaftAppendEntries {
                    term: 1,
                    leader_id: PeerId::from(2),
                    from: PeerId::from(2),
                    now,
                })
                .is_empty()
        );

        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));
        assert!(matches!(
            &outputs[0],
            ConsensusOutput::Send {
                to,
                body: MessageBody::ClientTransaction(client_tx),
            } if *to == PeerId::from(2) && *client_tx == transaction
        ));
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
            from: PeerId::from(2),
            now,
        });
        let outputs = consensus.handle_input(ConsensusInput::ClientTransactionReceived(
            transaction.clone(),
        ));

        assert_eq!(outputs.len(), 1);
        assert!(matches!(
            &outputs[0],
            ConsensusOutput::Send {
                to,
                body: MessageBody::ClientTransaction(client_tx),
            } if *to == PeerId::from(2) && *client_tx == transaction
        ));
    }

    #[test]
    fn raft_leader_applies_client_transaction() {
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
            ConsensusOutput::Reject(message) if message == "Raft leader is unknown"
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
            ConsensusOutput::ApplyClientTransaction(client_tx) if *client_tx == transaction
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
            ConsensusOutput::Reject(message) if message == "Raft leader is unknown"
        ));
    }
}
