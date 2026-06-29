use crate::peer::consensus::RaftLogEntry;
use crate::storage::BlockHash;
use crate::transactions::{SignedTransaction, VerifiedTransaction};
use derive_more::{Constructor, Display, From};
use k256::ecdsa::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(
    Clone, Eq, PartialEq, Hash, Copy, Debug, Display, From, Constructor, Serialize, Deserialize,
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
        if self.0 > u16::MAX as u32 {
            Err(format!("PeerId is too big: {}", self.0))
        } else {
            Ok(self.0 as u16)
        }
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
    RaftRequestVote { term: u64, candidate_id: PeerId },
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
