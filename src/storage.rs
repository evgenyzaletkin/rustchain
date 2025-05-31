use crate::transactions::Transaction;
use k256::ecdsa::SigningKey;
use k256::sha2::{Digest, Sha256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fs, mem};
use k256::ecdsa::VerifyingKey;

pub const DEFAULT_PATH_TO_BLOCKS: &str = "data";
pub const DEFAULT_MEMPOOL_SIZE: usize = 100;
const EMPTY_HASH: &str = "0";

pub struct BlockKeeper {
    path_to_blocks: PathBuf,
    mempool_size: usize,
    mempool: Vec<Transaction>,
    uncommited_blocks: HashMap<String, BlockFile>,
    last_commited_index: u32,
    last_commited_hash: String,
    previous_hash: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BlockFile {
    current_hash: String,
    previous_hash: String,
    transactions: Vec<Transaction>,
}

pub enum BlockStatus {
    AddedToMempool,
    NewBlockCreated { block_hash: String },
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
            last_commited_hash: EMPTY_HASH.to_string(),
            previous_hash: EMPTY_HASH.to_string(),
        };
        let sorted_blocks = keeper.list_all_blocks_sorted();
        let (last_commited_index, last_commited_hash) = match sorted_blocks.last() {
            None => (0, EMPTY_HASH.to_string()),
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

    fn create_new_block(&mut self, transactions: Vec<Transaction>) -> String {
        let current_hash = self.calculate_hash(&self.mempool, &self.previous_hash);
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

    pub fn commit_block(&mut self, block_hash: &str) -> Result<(), String> {
        if let Some(block_file) = self.uncommited_blocks.get(block_hash) {
            let block_index = self.last_commited_index + 1;
            let block_filename = self.block_filename_for_index(self.last_commited_index + 1);
            let block_path = self.path_to_blocks.join(block_filename);
            let json = serde_json::to_string(&block_file).expect("Failed to serialize block file");
            match fs::write(block_path, json) {
                Ok(_) => {
                    self.last_commited_index = block_index;
                    self.last_commited_hash = block_hash.to_string();
                    self.uncommited_blocks.remove(block_hash);
                    Ok(())
                }
                Err(e) => Err(format!("Failed to write block file: {}", e)),
            }
        } else {
            Err(format!("Block with hash {} not found", block_hash))
        }
    }

    pub fn get_uncommited_block(&self, block_hash: &str) -> Option<&BlockFile> {
        self.uncommited_blocks.get(block_hash)
    }

    fn calculate_hash(&self, transactions: &Vec<Transaction>, previous_hash: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(previous_hash.as_bytes());
        hasher.update(serde_json::to_string(transactions).unwrap().as_bytes());
        hex::encode(hasher.finalize())
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

    pub fn list_all_blocks_sorted(&self) -> Vec<String> {
        let mut vec = fs::read_dir(&self.path_to_blocks)
            .ok()
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter_map(|entry| entry.file_name().to_str().map(String::from))
                    .filter(|filename| Self::BLOCK_PATTERN.is_match(&filename))
                    .collect()
            })
            .unwrap_or(Vec::new());
        vec.sort();
        vec
    }
}

pub struct KeyManager {}

const KEY_FILE_NAME: &str = "private_key.bin";

impl KeyManager {
    pub fn get_or_create_key(key_dir: &PathBuf) -> SigningKey {
        fs::create_dir_all(&key_dir)
            .expect(format!("Failed to create directory {key_dir:?}").as_str());
        let key_file_path = key_dir.join(KEY_FILE_NAME);
        if let Some(key_bytes) = fs::read(&key_file_path).ok() {
            let key_array: [u8; 32] = key_bytes.try_into().expect("Invalid key length");
            SigningKey::from_bytes(&key_array.into()).expect("Invalid key data")
        } else {
            let key = SigningKey::random(&mut rand::rng());
            let key_file = key.to_bytes();
            fs::write(key_file_path, key_file).expect("Failed to write key file");
            key
        }
    }
    
    pub fn key_to_hex_string(key: &VerifyingKey) -> String {
        hex::encode(key.to_encoded_point(false).as_bytes())
    }
    
    pub fn hex_string_to_key(key_hex: &str) -> VerifyingKey {
        let key_bytes = hex::decode(key_hex).expect("Invalid key hex string");
        VerifyingKey::from_sec1_bytes(&key_bytes).expect("Invalid key data")   
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions::{AssetType, Metadata, Operation};
    #[test]
    fn block_save_test() {
        let path_to_blocks = PathBuf::from("target/test/data/peer_1");
        fs::remove_dir_all(&path_to_blocks).expect("Failed to remove directory");
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");
        let mut block_keeper = BlockKeeper {
            path_to_blocks,
            mempool_size: 1,
            mempool: Vec::with_capacity(1),
            uncommited_blocks: HashMap::new(),
            last_commited_index: 0,
            last_commited_hash: "0".to_string(),
            previous_hash: "0".to_string(),
        };

        let transaction = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            signature: "signature1".to_string(),
            public_key: "public_key1".to_string(),
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
