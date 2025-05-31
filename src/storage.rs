use crate::transactions::Transaction;
use k256::sha2::{Digest, Sha256};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{fs, mem};

pub struct BlockKeeper {
    path_to_blocks: PathBuf,
    mempool_size: usize,
    mempool: Vec<Transaction>,
}

#[derive(Serialize, Deserialize)]
struct BlockFile {
    current_hash: String,
    previous_hash: String,
    transactions: Vec<Transaction>,
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

    pub fn add_transaction(&mut self, transaction: Transaction) {
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
