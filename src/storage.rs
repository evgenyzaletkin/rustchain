pub(crate) use crate::crypto::KeyManager;
use crate::crypto::Signable;
use crate::transactions::Transaction;
use derive_more::Display;
use k256::ecdsa::signature::Verifier;
use k256::ecdsa::{Signature, VerifyingKey, signature};
use k256::sha2::{Digest, Sha256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fs, mem};

pub const DEFAULT_PATH_TO_BLOCKS: &str = "data";
pub const DEFAULT_MEMPOOL_SIZE: usize = 100;
pub const EMPTY_HASH: BlockHash = [0; 32];

pub struct BlockKeeper {
    path_to_blocks: PathBuf,
    mempool_size: usize,
    mempool: Vec<Transaction>,
    uncommited_blocks: HashMap<BlockHash, BlockFile>,
    last_commited_index: u32,
    last_commited_hash: BlockHash,
    previous_hash: BlockHash,
}

pub type BlockHash = [u8; 32];

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockFile {
    current_hash: BlockHash,
    previous_hash: BlockHash,
    transactions: Vec<Transaction>,
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

impl BlockKeeper {
    const BLOCK_PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(\d+)\.block$").expect("invalid regex"));

    pub fn new(path_to_blocks: PathBuf, mempool_size: usize) -> Self {
        let mut keeper = Self {
            path_to_blocks,
            mempool_size,
            mempool: Vec::with_capacity(mempool_size),
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

    pub fn add_transaction(&mut self, transaction: Transaction) -> BlockStatus {
        self.mempool.push(transaction);
        if (self.mempool.len() >= self.mempool_size) {
            let transactions =
                mem::replace(&mut self.mempool, Vec::with_capacity(self.mempool_size));
            BlockStatus::NewBlockCreated {
                block_hash: self.create_new_block(transactions),
            }
        } else {
            BlockStatus::AddedToMempool
        }
    }

    fn create_new_block(&mut self, transactions: Vec<Transaction>) -> BlockHash {
        let current_hash = Self::calculate_hash(&self.mempool, &self.previous_hash);
        let block_file = BlockFile {
            transactions,
            current_hash: current_hash.clone(),
            previous_hash: self.previous_hash.clone(),
        };
        self.previous_hash = current_hash.clone();
        self.uncommited_blocks
            .insert(current_hash.clone(), block_file);
        current_hash
    }

    pub fn commit_block(&mut self, block_hash: &BlockHash) -> Result<(), String> {
        let Some(block_file) = self.uncommited_blocks.get(block_hash) else {
            return Err(format!(
                "Block with hash {} not found",
                hex::encode(block_hash)
            ));
        };
        let block_index = self.last_commited_index + 1;
        let block_filename = self.block_filename_for_index(self.last_commited_index + 1);
        let block_path = self.path_to_blocks.join(block_filename);
        let json = serde_json::to_string(&block_file).expect("Failed to serialize block file");
        match fs::write(block_path, json) {
            Ok(_) => {
                self.last_commited_index = block_index;
                self.last_commited_hash = block_hash.clone();
                self.uncommited_blocks.remove(block_hash);
                Ok(())
            }
            Err(e) => Err(format!("Failed to write block file: {}", e)),
        }
    }

    pub fn get_uncommited_block(&self, block_hash: &BlockHash) -> Option<&BlockFile> {
        self.uncommited_blocks.get(block_hash)
    }

    fn calculate_hash(
        transactions: &Vec<Transaction>,
        previous_hash: &BlockHash,
    ) -> BlockHash {
        let mut hasher = Sha256::new();
        hasher.update(previous_hash);
        hasher.update(serde_json::to_string(transactions).unwrap().as_bytes());
        hasher.finalize().into()
    }

    fn block_filename_for_index(&self, index: u32) -> String {
        format!("{:05}.block", index)
    }

    pub fn read_transactions_from_disk(&self, block_filename: &str) -> Vec<Transaction> {
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
        block_file: Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
    ) -> Result<BlockFile, BlockVerificationError> {
        KeyManager::verify_message(&public_key, &signature, &block_file)?;
        let block_file: BlockFile = serde_json::from_slice::<BlockFile>(&block_file)?;
        if (block_hash != block_file.current_hash) {
            return Err(BlockVerificationError::InvalidBlockHash);
        }
        let recalculated_hash =
            Self::calculate_hash(&block_file.transactions, &block_file.previous_hash);
        if (recalculated_hash != block_file.current_hash) {
            println!(
                "recalculated_hash: {} is different from received hash: {}",
                hex::encode(recalculated_hash),
                hex::encode(block_file.current_hash)
            );
            return Err(BlockVerificationError::InvalidBlockHash);
        }
        if (block_file.previous_hash != self.previous_hash) {
            return Err(BlockVerificationError::InvalidPreviousHash);
        }
        // verify transactions
        Ok((block_file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions::{AssetType, Metadata, Operation};
    use k256::ecdsa::VerifyingKey;
    #[test]
    fn block_save_test() {
        let path_to_blocks = PathBuf::from("target/test/data/peer_1");
        fs::remove_dir_all(&path_to_blocks).expect("Failed to remove directory");
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");

        let client_key = KeyManager::create_key();
        let client_public_key = VerifyingKey::from(client_key.clone());
        let operation = Operation::AddCoin {
            amount: 10,
            asset_type: AssetType::BTC,
        };
        let signature = KeyManager::sign_message(&client_key, &operation);

        let mut block_keeper = BlockKeeper {
            path_to_blocks,
            mempool_size: 1,
            mempool: Vec::with_capacity(1),
            uncommited_blocks: HashMap::new(),
            last_commited_index: 0,
            last_commited_hash: EMPTY_HASH,
            previous_hash: EMPTY_HASH,
        };

        let transaction = Transaction {
            operation: operation,
            signature,
            public_key: client_public_key,
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };
        if let BlockStatus::NewBlockCreated { block_hash } =
            block_keeper.add_transaction(transaction.clone())
        {
            block_keeper
                .commit_block(&block_hash)
                .expect("Failed to commit block");
            let block_file = block_keeper.read_block_from_disk("00001.block");
            assert_eq!(block_file.transactions.len(), 1);
            assert!(block_file.transactions.contains(&transaction));
        } else {
            panic!("New block not created");
        }
    }
}
