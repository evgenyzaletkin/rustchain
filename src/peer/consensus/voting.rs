use crate::peer::MessageBody;
use crate::peer::PeerId;
use crate::peer::consensus::{ConsensusAction, ConsensusInput};
use crate::storage::BlockHash;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConsensusOutcome {
    Pending,
    Approved,
    Rejected,
}

pub struct VotingConsensus {
    participants: HashSet<PeerId>,
    approvals: HashSet<PeerId>,
    rejections: HashSet<PeerId>,
    outcome: Option<ConsensusOutcome>,
}

impl VotingConsensus {
    pub fn new(peer_id: PeerId, known_peers: &[PeerId]) -> VotingConsensus {
        let mut participants: HashSet<PeerId> = HashSet::from_iter(known_peers.iter().copied());
        participants.insert(peer_id);
        VotingConsensus {
            approvals: HashSet::with_capacity(participants.len()),
            rejections: HashSet::with_capacity(participants.len()),
            participants,
            outcome: None,
        }
    }

    pub fn outcome(&self) -> Option<ConsensusOutcome> {
        self.outcome
    }

    pub fn already_voted(&self, peer_id: &PeerId) -> bool {
        self.approvals.contains(peer_id) || self.rejections.contains(peer_id)
    }

    pub fn make_vote(&mut self, peer_id: PeerId, approve: bool) -> ConsensusOutcome {
        if let Some(outcome) = self.outcome {
            return outcome;
        }

        if self.participants.contains(&peer_id) {
            if approve {
                self.approvals.insert(peer_id);
            } else {
                self.rejections.insert(peer_id);
            }

            let total_peers = self.participants.len();
            let f = (total_peers - 1) / 3;
            if self.approvals.len() >= 2 * f + 1 {
                self.outcome = Some(ConsensusOutcome::Approved);
                return ConsensusOutcome::Approved;
            } else if self.rejections.len() >= f {
                self.outcome = Some(ConsensusOutcome::Rejected);
                return ConsensusOutcome::Rejected;
            }
        }

        ConsensusOutcome::Pending
    }
}

pub fn handle_voting_input(
    peer_id: PeerId,
    votings: &mut HashMap<BlockHash, VotingConsensus>,
    input: ConsensusInput,
) -> Vec<ConsensusAction> {
    match input {
        ConsensusInput::ClientTransactionReceived(client_tx) => {
            vec![
                ConsensusAction::StageClientTransaction(client_tx.clone()),
                ConsensusAction::BroadcastClientTransaction(client_tx),
            ]
        }
        ConsensusInput::NewBlockCreated { block_hash } => {
            vec![ConsensusAction::ProposeBlock(block_hash)]
        }
        ConsensusInput::LocalBlockProposed {
            block_hash,
            known_peers,
        } => {
            let mut consensus = VotingConsensus::new(peer_id, &known_peers);
            consensus.make_vote(peer_id, true);
            votings.insert(block_hash, consensus);
            Vec::new()
        }
        ConsensusInput::BlockProposalValidated {
            block_hash,
            proposer,
            valid,
            known_peers,
        } => {
            let mut actions =
                handle_voting_vote(peer_id, votings, &known_peers, block_hash, proposer, true);
            if actions.is_empty() {
                actions.extend(handle_voting_vote(
                    peer_id,
                    votings,
                    &known_peers,
                    block_hash,
                    peer_id,
                    valid,
                ));
            }
            actions
        }
        ConsensusInput::BlockVoteReceived {
            block_hash,
            from,
            approve,
            known_peers,
        } => handle_voting_vote(peer_id, votings, &known_peers, block_hash, from, approve),
        ConsensusInput::Tick { .. }
        | ConsensusInput::RaftRequestVote { .. }
        | ConsensusInput::RaftRequestVoteResponse { .. }
        | ConsensusInput::RaftAppendEntriesResponse { .. }
        | ConsensusInput::RaftAppendEntries { .. } => Vec::new(),
    }
}

fn handle_voting_vote(
    peer_id: PeerId,
    votings: &mut HashMap<BlockHash, VotingConsensus>,
    known_peers: &[PeerId],
    block_hash: BlockHash,
    from: PeerId,
    approve: bool,
) -> Vec<ConsensusAction> {
    let consensus = votings
        .entry(block_hash)
        .or_insert_with(|| VotingConsensus::new(peer_id, known_peers));

    if consensus.already_voted(&from) || consensus.outcome().is_some() {
        return Vec::new();
    }

    match consensus.make_vote(from, approve) {
        ConsensusOutcome::Approved => vec![
            ConsensusAction::CommitBlock(block_hash),
            ConsensusAction::Broadcast(MessageBody::BlockApproved { block_hash }),
        ],
        ConsensusOutcome::Rejected => vec![
            ConsensusAction::RollbackBlock(block_hash),
            ConsensusAction::Broadcast(MessageBody::BlockReject { block_hash }),
        ],
        ConsensusOutcome::Pending => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{ConsensusOutcome, VotingConsensus};
    use crate::peer::PeerId;

    #[test]
    fn approves_when_threshold_is_reached() {
        let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];
        let mut consensus = VotingConsensus::new(PeerId::from(1), &known_peers);

        assert_eq!(
            consensus.make_vote(PeerId::from(1), true),
            ConsensusOutcome::Pending
        );
        assert_eq!(
            consensus.make_vote(PeerId::from(2), true),
            ConsensusOutcome::Pending
        );
        assert_eq!(
            consensus.make_vote(PeerId::from(3), true),
            ConsensusOutcome::Approved
        );
        assert_eq!(consensus.outcome(), Some(ConsensusOutcome::Approved));
    }

    #[test]
    fn rejects_when_threshold_is_reached() {
        let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];
        let mut consensus = VotingConsensus::new(PeerId::from(1), &known_peers);

        assert_eq!(
            consensus.make_vote(PeerId::from(2), false),
            ConsensusOutcome::Rejected
        );
        assert_eq!(consensus.outcome(), Some(ConsensusOutcome::Rejected));
    }

    #[test]
    fn ignores_votes_from_non_participants() {
        let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];
        let mut consensus = VotingConsensus::new(PeerId::from(1), &known_peers);

        assert_eq!(
            consensus.make_vote(PeerId::from(99), true),
            ConsensusOutcome::Pending
        );
        assert_eq!(consensus.outcome(), None);
    }

    #[test]
    fn tracks_duplicate_votes_without_changing_state() {
        let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];
        let mut consensus = VotingConsensus::new(PeerId::from(1), &known_peers);

        assert_eq!(
            consensus.make_vote(PeerId::from(1), true),
            ConsensusOutcome::Pending
        );
        assert!(consensus.already_voted(&PeerId::from(1)));
        assert_eq!(
            consensus.make_vote(PeerId::from(1), true),
            ConsensusOutcome::Pending
        );
        assert_eq!(
            consensus.make_vote(PeerId::from(2), true),
            ConsensusOutcome::Pending
        );
    }

    #[test]
    fn returns_final_outcome_after_consensus_is_reached() {
        let known_peers = vec![PeerId::from(2), PeerId::from(3), PeerId::from(4)];
        let mut consensus = VotingConsensus::new(PeerId::from(1), &known_peers);

        assert_eq!(
            consensus.make_vote(PeerId::from(2), false),
            ConsensusOutcome::Rejected
        );
        assert_eq!(
            consensus.make_vote(PeerId::from(3), true),
            ConsensusOutcome::Rejected
        );
    }
}
