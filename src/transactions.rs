use crate::PeerId;
use k256::sha2::{Digest, Sha256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Metadata {
    timestamp_nanos: u32,
    sequence_number: u32,
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
    operation: Operation,
    signature: String,
    public_key: String,
    metadata: Metadata,
}

#[derive(Serialize, Deserialize)]
struct BlockFile {
    current_hash: String,
    previous_hash: String,
    transactions: Vec<Transaction>,
}

pub struct TransactionProcessor {
    mempool: Vec<Transaction>,
    block_keeper: BlockKeeper,
}

impl TransactionProcessor {
    const MEMPOOL_SIZE: usize = 100;
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            mempool: Vec::with_capacity(Self::MEMPOOL_SIZE),
            block_keeper: BlockKeeper::new(peer_id),
        }
    }

    pub fn process_transaction(&mut self, transaction: Transaction) {
        if (self.mempool.capacity() >= Self::MEMPOOL_SIZE) {
            self.mempool.clear();
        }
        self.mempool.push(transaction);
    }
}

struct BlockKeeper {
    path_to_blocks: PathBuf,
}

impl BlockKeeper {
    const BLOCK_PATTERN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(\d+)\.block$").expect("invalid regex"));

    fn new(peer_id: PeerId) -> Self {
        let path_to_blocks = PathBuf::from("data").join(format!("peer_{}", peer_id));
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");
        Self { path_to_blocks }
    }

    fn save_mempool_to_disk(&self, transactions: Vec<Transaction>) {
        let all_blocks = self.list_all_blocks();
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

        let current_hash = self.calculate_hash(&transactions, &previous_hash);
        let block_file = BlockFile {
            transactions,
            current_hash,
            previous_hash,
        };
        let block_filename = format!("{}.block", block_index);
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

    fn read_block_from_disk(&self, block_filename: &str) -> BlockFile {
        let block_path = self.path_to_blocks.join(block_filename);
        fs::read_to_string(block_path)
            .ok()
            .and_then(|s| serde_json::from_str::<BlockFile>(&s).ok())
            .expect("Failed to read block file")
    }

    fn list_all_blocks(&self) -> Vec<String> {
        fs::read_dir(&self.path_to_blocks)
            .ok()
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .filter_map(|entry| entry.file_name().to_str().map(String::from))
                    .filter(|filename| Self::BLOCK_PATTERN.is_match(&filename))
                    .collect()
            })
            .unwrap_or(Vec::new())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn it_works() {
        let path_to_blocks = PathBuf::from("target/test/data");
        match fs::remove_dir_all(&path_to_blocks) {
            Ok(_) => {}
            Err(e) => println!("{}", e)
        }
        fs::create_dir_all(&path_to_blocks).expect("Failed to create directory");
        let block_keeper: BlockKeeper = BlockKeeper { path_to_blocks };
        let transaction1 = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            signature: "signature1".to_string(),
            public_key: "public_key2".to_string(),
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        let transaction2 = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            signature: "signature2".to_string(),
            public_key: "public_key2".to_string(),
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        block_keeper.save_mempool_to_disk(vec![transaction1.clone(), transaction2.clone()]);

        let block_file_1 = block_keeper.read_block_from_disk("1.block");
        assert_eq!(block_file_1.transactions.len(), 2);
        assert!(block_file_1.transactions.contains(&transaction1));
        assert!(block_file_1.transactions.contains(&transaction2));

        let transaction3 = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            signature: "signature2".to_string(),
            public_key: "public_key2".to_string(),
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        block_keeper.save_mempool_to_disk(vec![transaction3.clone()]);
        let block_file_2 = block_keeper.read_block_from_disk("2.block");
        assert_eq!(block_file_2.transactions.len(), 1);
        assert!(block_file_2.transactions.contains(&transaction3));

        assert_eq!(block_file_1.current_hash, block_file_2.previous_hash);
    }
}
