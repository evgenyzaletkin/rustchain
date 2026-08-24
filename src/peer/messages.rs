use crate::peer::consensus::RaftLogEntry;
use crate::storage::BlockHash;
use crate::transactions::{SignedTransaction, VerifiedTransaction};
use derive_more::{Constructor, Display, From};
use k256::ecdsa::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(
    Clone,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    Hash,
    Copy,
    Debug,
    Display,
    From,
    Constructor,
    Serialize,
    Deserialize,
)]
pub struct PeerId(u32);

impl FromStr for PeerId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self).map_err(|e| e.to_string())
    }
}

impl TryInto<u16> for PeerId {
    type Error = String;

    fn try_into(self) -> Result<u16, Self::Error> {
        u16::try_from(self.0).map_err(|_| format!("PeerId is too big: {}", self.0))
    }
}

pub type TxPayload = Vec<u8>;

#[derive(Clone, Serialize, Deserialize)]
pub struct RaftReplicatedBlock {
    pub entry: RaftLogEntry,
    pub block_file: Vec<u8>,
    pub signature: Signature,
    pub public_key: VerifyingKey,
}

#[derive(Display, Clone, Serialize, Deserialize)]
pub enum MessageBody {
    // Ping,
    // Pong,
    #[display("ClientTransaction")]
    ClientTransaction(SignedTransaction),
    #[display("Synchronization")]
    Synchronization(VerifiedTransaction),
    #[display("BlockProposal")]
    BlockProposal {
        block_hash: BlockHash,
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
    },
    #[display("BlockReject")]
    BlockReject { block_hash: BlockHash },
    #[display("BlockApproved")]
    BlockApproved { block_hash: BlockHash },
    #[display("RaftRequestVote")]
    RaftRequestVote {
        term: u64,
        candidate_id: PeerId,
        last_log_index: u64,
        last_log_term: u64,
    },
    #[display("RaftRequestVoteResponse")]
    RaftRequestVoteResponse { term: u64, vote_granted: bool },
    #[display("RaftAppendEntries")]
    RaftAppendEntries {
        term: u64,
        leader_id: PeerId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftReplicatedBlock>,
        leader_commit: u64,
    },
    #[display("RaftAppendEntriesResponse")]
    RaftAppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

#[derive(Display, Serialize, Deserialize, Clone)]
#[display("{from} -> {to}: {body} ")]
pub struct Message {
    pub from: PeerId,
    pub to: PeerId,
    pub body: MessageBody,
}

#[cfg(test)]
mod tests {
    use super::{Message, MessageBody, PeerId};
    use crate::storage::BlockHash;
    use serde_json::json;

    #[test]
    fn raft_message_wire_shape_is_stable() {
        let message = Message {
            from: PeerId::from(1),
            to: PeerId::from(2),
            body: MessageBody::RaftRequestVote {
                term: 7,
                candidate_id: PeerId::from(1),
                last_log_index: 5,
                last_log_term: 6,
            },
        };

        let value = serde_json::to_value(&message).unwrap();
        assert_eq!(
            value,
            json!({
                "from": 1,
                "to": 2,
                "body": {"RaftRequestVote": {
                    "term": 7,
                    "candidate_id": 1,
                    "last_log_index": 5,
                    "last_log_term": 6
                }}
            })
        );
    }

    #[test]
    fn voting_message_wire_shape_is_stable() {
        let body = MessageBody::BlockApproved {
            block_hash: BlockHash::new([3; 32]),
        };

        let value = serde_json::to_value(&body).unwrap();
        assert_eq!(value, json!({"BlockApproved": {"block_hash": vec![3; 32]}}));
    }
}
