use crate::PeerId;
use k256::sha2::{Digest, Sha256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fs, mem};

const DEFAULT_MEMPOOL_SIZE: usize = 100;
const DEFAULT_PATH_TO_BLOCKS: &str = "data";

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Metadata {
    pub timestamp_nanos: u32,
    pub sequence_number: u32,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum AssetType {
    BTC,
    USDT,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub enum Operation {
    AddCoin {
        amount: u32,
        asset_type: AssetType,
    },
    Send {
        recipient: String,
        amount: u32,
        asset_type: AssetType,
    },
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Transaction {
    pub operation: Operation,
    pub signature: String,
    pub public_key: String,
    pub metadata: Metadata,
}

pub struct Account {
    pub asset_type: AssetType,
    pub balance: u32,
}

#[derive(Serialize, Deserialize)]
struct BlockFile {
    current_hash: String,
    previous_hash: String,
    transactions: Vec<Transaction>,
}

pub struct TransactionProcessor {
    block_keeper: BlockKeeper,
    accounts: HashMap<String, Account>,
}

impl TransactionProcessor {
    pub fn new(peer_id: PeerId) -> Self {
        let path_to_blocks =
            PathBuf::from(DEFAULT_PATH_TO_BLOCKS).join(format!("peer_{}", peer_id));
        Self {
            block_keeper: BlockKeeper::new(path_to_blocks, DEFAULT_MEMPOOL_SIZE),
            accounts: HashMap::new(),
        }
    }

    pub fn with_block_keeper(block_keeper: BlockKeeper) -> Self {
        Self {
            block_keeper,
            accounts: HashMap::new(),
        }
    }

    pub fn process_transaction(&mut self, transaction: Transaction) {
        self.block_keeper.add_transaction(transaction.clone());
        match transaction.operation {
            Operation::AddCoin { asset_type, amount } => {
                self.add_coin(transaction.public_key.clone(), asset_type.clone(), amount);
            }
            Operation::Send {
                recipient,
                amount,
                asset_type,
            } => self.send_coins(transaction.public_key, recipient, asset_type, amount),
        }
    }

    fn add_coin(&mut self, id: String, asset_type: AssetType, amount: u32) {
        self.accounts
            .entry(id)
            .and_modify(|account| {
                account.balance += amount;
            })
            .or_insert_with_key(|_| Account {
                asset_type,
                balance: amount,
            });
    }

    fn send_coins(&mut self, id: String, to: String, asset_type: AssetType, amount: u32) {
        self.accounts
            .entry(id)
            .and_modify(|account| account.balance -= amount)
            .or_insert_with_key(|k| panic!("Account {} not found", k));
        self.add_coin(to, asset_type.clone(), amount);
    }

    pub fn get_account(&self, id: &str) -> Option<&Account> {
        self.accounts.get(id)
    }

    pub fn read_state(&mut self) {
        let block_names = self.block_keeper.list_all_blocks();
        for block_name in block_names {
            let transactions = self.block_keeper.read_transactions_from_disk(&block_name);
            for transaction in transactions {
                self.process_transaction(transaction);
            }
        }
    }
}

pub struct BlockKeeper {
    path_to_blocks: PathBuf,
    mempool_size: usize,
    mempool: Vec<Transaction>,
}

impl BlockKeeper {
    const BLOCK_PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(\d+)\.block$").expect("invalid regex"));

    pub fn new(path_to_blocks: PathBuf, mempool_size: usize) -> Self {
        Self {
            path_to_blocks,
            mempool_size,
            mempool: Vec::with_capacity(mempool_size),
        }
    }

    fn add_transaction(&mut self, transaction: Transaction) {
        self.mempool.push(transaction);
        if (self.mempool.capacity() >= self.mempool_size) {
            let transactions = mem::take(&mut self.mempool);
            self.save_mempool_to_disk(transactions);
        }
    }

    fn save_mempool_to_disk(&self, transactions: Vec<Transaction>) {
        let mut all_blocks = self.list_all_blocks();
        all_blocks.sort();
        let option = all_blocks.iter().max();
        let (block_index, previous_hash) = match option {
            None => (1, "0".to_string()),
            Some(s) => (
                Self::BLOCK_PATTERN
                    .captures(s)
                    .map(|c| c[1].parse::<u32>().unwrap() + 1)
                    .expect("Failed to parse block index"),
                self.read_block_from_disk(&s).current_hash,
            ),
        };

        let current_hash = self.calculate_hash(&self.mempool, &previous_hash);
        let block_file = BlockFile {
            transactions,
            current_hash,
            previous_hash,
        };
        let block_filename = self.block_filename_for_index(block_index);
        let block_path = self.path_to_blocks.join(block_filename);
        let json = serde_json::to_string(&block_file).expect("Failed to serialize block file");
        fs::write(block_path, json).expect("Failed to write block file");
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

    fn read_transactions_from_disk(&self, block_filename: &str) -> Vec<Transaction> {
        self.read_block_from_disk(block_filename).transactions
    }

    fn read_block_from_disk(&self, block_filename: &str) -> BlockFile {
        let block_path = self.path_to_blocks.join(block_filename);
        fs::read_to_string(block_path)
            .ok()
            .and_then(|s| serde_json::from_str::<BlockFile>(&s).ok())
            .expect("Failed to read block file")
    }

    pub fn list_all_blocks(&self) -> Vec<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn block_save_test() {
        let path_to_blocks = PathBuf::from("target/test/data/peer_1");
        fs::remove_dir_all(&path_to_blocks).expect("Failed to remove directory");
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");
        let mut block_keeper = BlockKeeper {
            path_to_blocks,
            mempool_size: 1,
            mempool: Vec::with_capacity(1),
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
        let vec = block_keeper.list_all_blocks();
        assert!(vec.is_empty());
        block_keeper.add_transaction(transaction.clone());
        let vec = block_keeper.list_all_blocks();
        assert_eq!(vec.len(), 1);
        assert_eq!(vec[0], "00001.block");
        let block_file = block_keeper.read_block_from_disk(&vec[0]);
        assert_eq!(block_file.transactions.len(), 1);
        assert!(block_file.transactions.contains(&transaction));
    }
}
