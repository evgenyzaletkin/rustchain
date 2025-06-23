pub(crate) use crate::crypto::KeyManager;
use crate::crypto::Signable;
use crate::transactions::{SignedTransaction};
use derive_more::Display;
use k256::ecdsa::{Signature, VerifyingKey, signature};
use k256::sha2::{Digest, Sha256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fmt, fs, mem};

pub const DEFAULT_PATH_TO_BLOCKS: &str = "data";
pub const DEFAULT_MEMPOOL_SIZE: usize = 5;
pub const EMPTY_HASH: BlockHash = BlockHash([0; 32]);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct BlockHash([u8; 32]);

impl BlockHash {
    pub fn new(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl TryFrom<[u8; 32]> for BlockHash {
    type Error = ();

    fn try_from(hash: [u8; 32]) -> Result<Self, Self::Error> {
        Ok(Self(hash))
    }
}

impl Display for BlockHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockFile {
    current_hash: BlockHash,
    previous_hash: BlockHash,
    transactions: Vec<SignedTransaction>,
}

impl From<&Vec<u8>> for BlockFile {
    fn from(block_file_vec: &Vec<u8>) -> Self {
        serde_json::from_slice(&block_file_vec).unwrap()
    }
}

impl Signable for BlockFile {}

pub enum BlockStatus {
    AddedToMempool,
    NewBlockCreated { block_hash: BlockHash },
}

#[derive(Debug, Display)]
pub enum BlockVerificationError {
    SignatureError(signature::Error),
    DeserializationError(serde_json::Error),
    InvalidBlockHash,
    InvalidPreviousHash,
}

impl From<signature::Error> for BlockVerificationError {
    fn from(err: signature::Error) -> Self {
        BlockVerificationError::SignatureError(err)
    }
}

impl From<serde_json::Error> for BlockVerificationError {
    fn from(err: serde_json::Error) -> Self {
        BlockVerificationError::DeserializationError(err)
    }
}

pub struct BlockKeeper {
    path_to_blocks: PathBuf,
    mempool_size: usize,
    mempool: HashMap<String, SignedTransaction>,
    pending_transactions: HashMap<String, SignedTransaction>,
    uncommited_blocks: HashMap<BlockHash, BlockFile>,
    last_commited_index: u32,
    last_commited_hash: BlockHash,
    previous_hash: BlockHash,
}

impl BlockKeeper {
    const BLOCK_PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(\d+)\.block$").expect("invalid regex"));

    pub fn new(path_to_blocks: PathBuf, mempool_size: usize) -> Self {
        let mut keeper = Self {
            path_to_blocks,
            mempool_size,
            mempool: HashMap::with_capacity(mempool_size),
            pending_transactions: HashMap::new(),
            uncommited_blocks: HashMap::new(),
            last_commited_index: 0,
            last_commited_hash: EMPTY_HASH,
            previous_hash: EMPTY_HASH,
        };
        let sorted_blocks = keeper.list_all_blocks();
        let (last_commited_index, last_commited_hash) = match sorted_blocks.last() {
            None => (0, EMPTY_HASH),
            Some(block_file_name) => {
                let block_index = Self::BLOCK_PATTERN
                    .captures(&block_file_name)
                    .map(|c| c[1].parse::<u32>().unwrap())
                    .expect("Failed to parse block index");
                let block_file = keeper.read_block_from_disk(&block_file_name);
                (block_index, block_file.previous_hash)
            }
        };
        keeper.last_commited_index = last_commited_index;
        keeper.last_commited_hash = last_commited_hash.clone();
        keeper.previous_hash = last_commited_hash;
        keeper
    }

    pub fn add_transaction(&mut self, transaction: SignedTransaction) -> BlockStatus {
        self.mempool.insert(transaction.tx_id(), transaction);
        if self.mempool.len() >= self.mempool_size {
            let transactions =
                mem::replace(&mut self.mempool, HashMap::with_capacity(self.mempool_size));
            BlockStatus::NewBlockCreated {
                block_hash: self.create_new_block(transactions),
            }
        } else {
            BlockStatus::AddedToMempool
        }
    }

    fn create_new_block(
        &mut self,
        transactions_map: HashMap<String, SignedTransaction>,
    ) -> BlockHash {
        let mut transactions: Vec<SignedTransaction> = transactions_map.values().cloned().collect();
        transactions.sort_by(|a, b| a.tx_id().cmp(&b.tx_id()));
        let current_hash = Self::calculate_hash(&transactions, &self.previous_hash);
        println!("Creating new block: {}, transactions: {:?}", current_hash, &transactions);
        let block_file = BlockFile {
            transactions,
            current_hash: current_hash.clone(),
            previous_hash: self.last_commited_hash.clone(),
        };
        self.previous_hash = current_hash.clone();
        self.uncommited_blocks
            .insert(current_hash.clone(), block_file);
        for (tx_id, transaction) in transactions_map {
            self.pending_transactions
                .insert(tx_id, transaction);
        }
        current_hash
    }

    // Assume the block is already verified
    pub fn add_block_from_proposal(&mut self, block_file: BlockFile) -> Result<(), String> {
        if self
            .uncommited_blocks
            .contains_key(&block_file.current_hash)
        {
            return Err(format!(
                "Block with hash {} already exists",
                block_file.current_hash
            ));
        }
        for transaction in block_file.transactions {
            if let Some(transaction) = self.mempool.remove(&transaction.tx_id()) {
                self.pending_transactions
                    .insert(transaction.tx_id(), transaction);
            }
        }
        Ok(())
    }

    pub fn commit_block(&mut self, block_hash: &BlockHash) -> Result<(), String> {
        let Some(block_file) = self.uncommited_blocks.get(block_hash) else {
            return Err(format!("Block with hash {} not found", block_hash));
        };
        let block_index = self.last_commited_index + 1;
        let block_filename = self.block_filename_for_index(self.last_commited_index + 1);
        let block_path = self.path_to_blocks.join(block_filename);
        let json = serde_json::to_string(&block_file).expect("Failed to serialize block file");
        match fs::write(block_path, json) {
            Ok(_) => {
                self.last_commited_index = block_index;
                self.last_commited_hash = block_hash.clone();
                for transaction in &block_file.transactions {
                    self.pending_transactions.remove(&transaction.tx_id());
                }
                self.uncommited_blocks.remove(block_hash);
                Ok(())
            }
            Err(e) => Err(format!("Failed to write block file: {}", e)),
        }
    }

    pub fn rollback_block(&mut self, block_hash: &BlockHash) -> Result<(), String> {
        let Some(block_file) = self.uncommited_blocks.remove(block_hash) else {
            return Err(format!("Block with hash {} not found", block_hash));
        };
        for transaction in block_file.transactions {
            if let Some(transaction) = self.pending_transactions.remove(&transaction.tx_id()) {
                self.add_transaction(transaction);
            }
        }
        Ok(())
    }

    pub fn get_uncommited_block(&self, block_hash: &BlockHash) -> Option<&BlockFile> {
        self.uncommited_blocks.get(block_hash)
    }

    fn calculate_hash(
        transactions: &Vec<SignedTransaction>,
        previous_hash: &BlockHash,
    ) -> BlockHash {
        let mut hasher = Sha256::new();
        hasher.update(previous_hash.0);
        hasher.update(serde_json::to_string(transactions).unwrap().as_bytes());
        BlockHash(hasher.finalize().into())
    }

    fn block_filename_for_index(&self, index: u32) -> String {
        format!("{:05}.block", index)
    }

    pub fn read_transactions_from_disk(&self, block_filename: &str) -> Vec<SignedTransaction> {
        self.read_block_from_disk(block_filename).transactions
    }

    fn read_block_from_disk(&self, block_filename: &str) -> BlockFile {
        let block_path = self.path_to_blocks.join(block_filename);
        fs::read_to_string(block_path)
            .ok()
            .and_then(|s| serde_json::from_str::<BlockFile>(&s).ok())
            .expect("Failed to read block file")
    }

    pub fn list_all_blocks(&self) -> SortedVec<String> {
        fs::read_dir(&self.path_to_blocks)
            .ok()
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter_map(|entry| entry.file_name().to_str().map(String::from))
                    .filter(|filename| Self::BLOCK_PATTERN.is_match(&filename))
                    .fold(SortedVec::new(), |mut sorted_vec, filename| {
                        sorted_vec.push(filename);
                        sorted_vec
                    })
            })
            .unwrap_or(SortedVec::new())
    }

    pub fn verify_block(
        &self,
        block_hash: BlockHash,
        block_file_vec: &Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
    ) -> Result<BlockFile, BlockVerificationError> {
        KeyManager::verify_message(&public_key, &signature, block_file_vec)?;
        let block_file: BlockFile = block_file_vec.into();
        if block_hash != block_file.current_hash {
            return Err(BlockVerificationError::InvalidBlockHash);
        }
        let recalculated_hash =
            Self::calculate_hash(&block_file.transactions, &block_file.previous_hash);
        if recalculated_hash != block_file.current_hash {
            println!(
                "recalculated_hash: {} is different from received hash: {}",
                recalculated_hash, block_file.current_hash
            );
            return Err(BlockVerificationError::InvalidBlockHash);
        }
        if block_file.previous_hash != self.last_commited_hash {
            return Err(BlockVerificationError::InvalidPreviousHash);
        }
        // verify transactions
        Ok(block_file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};

    #[test]
    fn block_save_test() {
        let path_to_blocks = PathBuf::from("target/test/data/peer_1");
        fs::remove_dir_all(&path_to_blocks).expect("Failed to remove directory");
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");

        let client_key = KeyManager::create_key();

        let transaction = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        let client_transaction = SignedTransaction::new(transaction, &client_key);

        let mut block_keeper = BlockKeeper {
            path_to_blocks,
            mempool_size: 1,
            mempool: HashMap::with_capacity(1),
            pending_transactions: HashMap::with_capacity(1),
            uncommited_blocks: HashMap::new(),
            last_commited_index: 0,
            last_commited_hash: EMPTY_HASH,
            previous_hash: EMPTY_HASH,
        };

        if let BlockStatus::NewBlockCreated { block_hash } =
            block_keeper.add_transaction(client_transaction.clone())
        {
            block_keeper
                .commit_block(&block_hash)
                .expect("Failed to commit block");
            let block_file = block_keeper.read_block_from_disk("00001.block");
            assert_eq!(block_file.transactions.len(), 1);
            assert!(block_file.transactions.contains(&client_transaction));
        } else {
            panic!("New block not created");
        }
    }
}
