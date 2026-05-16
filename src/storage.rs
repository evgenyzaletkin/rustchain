use crate::crypto::KeyManager;
use crate::crypto::Signable;
use crate::transactions::SignedTransaction;
use derive_more::Display;
use k256::ecdsa::{Signature, VerifyingKey, signature};
use k256::sha2::{Digest, Sha256};
use log::{info};
use regex::Regex;
use serde::{Deserialize, Serialize};
use sorted_vec::SortedVec;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, RwLock};
use std::{fmt, fs, mem};

pub const DEFAULT_PATH_TO_BLOCKS: &str = "data";
pub const DEFAULT_MEMPOOL_SIZE: usize = 5;
pub const EMPTY_HASH: BlockHash = BlockHash([0; 32]);
const BLOCK_PATTERN: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(\d+)\.block$").unwrap());

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
    pub index: u32,
    pub hash: BlockHash,
    previous_hash: BlockHash,
    transactions: Vec<SignedTransaction>,
}

impl From<&Vec<u8>> for BlockFile {
    fn from(block_file_vec: &Vec<u8>) -> Self {
        serde_json::from_slice(&block_file_vec).unwrap()
    }
}

impl BlockFile {
    pub fn create(
        transactions: Vec<SignedTransaction>,
        previous_hash: BlockHash,
        index: u32,
    ) -> Self {
        Self {
            index,
            hash: Self::calculate_hash(&transactions, &previous_hash),
            previous_hash,
            transactions,
        }
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

    fn read_from_disk(block_path: &PathBuf) -> Result<Self, String> {
        let block_contents = fs::read_to_string(block_path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                format!("Block file not found: {}", block_path.display())
            } else {
                format!("Failed to read block file {}: {}", block_path.display(), e)
            }
        })?;
        serde_json::from_str::<BlockFile>(&block_contents).map_err(|e| {
            format!(
                "Failed to deserialize block file {}: {}",
                block_path.display(),
                e
            )
        })
    }

    fn read_from_disk_by_index(
        path_to_blocks: &PathBuf,
        index: u32,
    ) -> Result<Self, String> {
        Self::read_from_disk(&path_to_blocks.join(BlockFile::block_filename_for_index(index)))
    }

    fn block_filename_for_index(index: u32) -> String {
        format!("{:05}.block", index)
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
    AlreadyAdded,
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

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct BlockStorageState {
    pub block_height: u32,
    pub last_commited_hash: BlockHash,
}

impl From<&BlockFile> for BlockStorageState {
    fn from(value: &BlockFile) -> Self {
        BlockStorageState {
            block_height: value.index,
            last_commited_hash: value.hash,
        }
    }
}

pub struct BlockKeeper {
    path_to_blocks: PathBuf,
    mempool_size: usize,
    mempool: HashMap<String, SignedTransaction>,
    pending_transactions: HashMap<String, SignedTransaction>,
    uncommited_blocks: HashMap<BlockHash, BlockFile>,
    block_storage_state: Arc<RwLock<BlockStorageState>>,
    previous_hash: BlockHash,
}

impl BlockKeeper {
    pub fn new(path_to_blocks: PathBuf, mempool_size: usize) -> Self {
        let sorted_blocks = list_all_blocks(&path_to_blocks);
        let (last_commited_index, last_commited_hash) = match sorted_blocks.last() {
            None => (0, EMPTY_HASH),
            Some(block_file_name) => {
                let block_index = BLOCK_PATTERN
                    .captures(&block_file_name)
                    .map(|c| c[1].parse::<u32>().unwrap())
                    .expect("Failed to parse block index");
                match BlockFile::read_from_disk(&path_to_blocks.join(block_file_name)) {
                    Ok(block_file) => (block_index, block_file.hash),
                    Err(e) => {
                        eprintln!("Failed to read latest block state: {e}");
                        (0, EMPTY_HASH)
                    }
                }
            }
        };
        let block_storage_state = Arc::new(RwLock::new(BlockStorageState {
            block_height: last_commited_index,
            last_commited_hash,
        }));
        let mut keeper = Self {
            path_to_blocks,
            mempool_size,
            mempool: HashMap::with_capacity(mempool_size),
            pending_transactions: HashMap::new(),
            uncommited_blocks: HashMap::new(),
            block_storage_state,
            previous_hash: EMPTY_HASH,
        };
        keeper.previous_hash = last_commited_hash;
        keeper
    }

    pub fn create_block_storage_view(&self) -> BlockStorageView {
        BlockStorageView {
            storage_state: self.block_storage_state.clone(),
            path_to_blocks: self.path_to_blocks.clone(),
        }
    }

    pub fn get_block_storage_state(&self) -> Arc<RwLock<BlockStorageState>> {
        self.block_storage_state.clone()
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
        println!("Creating new block, transactions: {:?}", &transactions);
        let state = self.block_storage_state.read().unwrap();
        let block_file = BlockFile::create(
            transactions,
            state.last_commited_hash,
            state.block_height + 1,
        );
        let block_hash = block_file.hash.clone();
        self.previous_hash = block_hash.clone();
        self.uncommited_blocks
            .insert(block_hash.clone(), block_file);
        for (tx_id, transaction) in transactions_map {
            self.pending_transactions.insert(tx_id, transaction);
        }
        block_hash
    }

    // Assume the block is already verified
    pub fn add_external_block(
        &mut self,
        block_file: BlockFile,
    ) -> Result<(), BlockVerificationError> {
        self.verify_block(&block_file)?;
        for transaction in block_file.transactions.iter() {
            if let Some(transaction) = self.mempool.remove(&transaction.tx_id()) {
                self.pending_transactions
                    .insert(transaction.tx_id(), transaction);
            }
        }
        self.uncommited_blocks
            .insert(block_file.hash.clone(), block_file);
        Ok(())
    }

    pub fn commit_block(&mut self, block_hash: &BlockHash) -> Result<(), String> {
        let Some(block_file) = self.uncommited_blocks.get(block_hash) else {
            return Err(format!("Block with hash {} not found", block_hash));
        };
        let mut storage_state = self.block_storage_state.write().unwrap();
        let block_index = storage_state.block_height + 1;
        let block_filename = BlockFile::block_filename_for_index(block_index);
        let block_path = self.path_to_blocks.join(block_filename);
        let json = serde_json::to_string(&block_file).expect("Failed to serialize block file");
        match fs::write(block_path, json) {
            Ok(_) => {
                storage_state.block_height = block_index;
                storage_state.last_commited_hash = block_hash.clone();
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

    pub fn read_transactions_from_disk(
        &self,
        block_filename: &str,
    ) -> Result<Vec<SignedTransaction>, String> {
        Ok(BlockFile::read_from_disk(&self.path_to_blocks.join(block_filename))?.transactions)
    }

    pub fn list_all_blocks(&self) -> SortedVec<String> {
        list_all_blocks(&self.path_to_blocks)
    }

    pub fn block_can_be_added(&self, block_file: &BlockFile) -> bool {
        !self.uncommited_blocks.contains_key(&block_file.hash)
            && self.block_storage_state.read().unwrap().block_height == block_file.index - 1
    }

    pub fn verify_block_vec(
        &self,
        block_hash: BlockHash,
        block_file_vec: &Vec<u8>,
        signature: Signature,
        public_key: VerifyingKey,
    ) -> Result<BlockFile, BlockVerificationError> {
        KeyManager::verify_message(&public_key, &signature, block_file_vec)?;
        let block_file: BlockFile = block_file_vec.into();
        if block_hash != block_file.hash {
            return Err(BlockVerificationError::InvalidBlockHash);
        }
        self.verify_block(&block_file)?;
        Ok(block_file)
    }

    fn verify_block(&self, block_file: &BlockFile) -> Result<(), BlockVerificationError> {
        let calculated_hash =
            BlockFile::calculate_hash(&block_file.transactions, &block_file.previous_hash);
        if calculated_hash != block_file.hash {
            info!(
                "recalculated_hash: {} is different from received hash: {}",
                calculated_hash, block_file.hash
            );
            return Err(BlockVerificationError::InvalidBlockHash);
        }
        if block_file.previous_hash != self.block_storage_state.read().unwrap().last_commited_hash {
            return Err(BlockVerificationError::InvalidPreviousHash);
        }
        // verify transactions
        Ok(())
    }
}

pub struct BlockStorageView {
    storage_state: Arc<RwLock<BlockStorageState>>,
    path_to_blocks: PathBuf,
}

impl BlockStorageView {
    pub fn get_latest_state(&self) -> BlockStorageState {
        *self.storage_state.read().unwrap()
    }

    pub fn get_block(&self, index: u32) -> Result<BlockFile, String> {
        BlockFile::read_from_disk_by_index(&self.path_to_blocks, index)
    }
}

fn list_all_blocks(path_to_blocks: &PathBuf) -> SortedVec<String> {
    fs::read_dir(path_to_blocks)
        .ok()
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| entry.file_name().to_str().map(String::from))
                .filter(|filename| BLOCK_PATTERN.is_match(&filename))
                .fold(SortedVec::new(), |mut sorted_vec, filename| {
                    sorted_vec.push(filename);
                    sorted_vec
                })
        })
        .unwrap_or(SortedVec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};
    use k256::ecdsa::SigningKey;

    fn setup(path_to_blocks: &PathBuf) {
        // The dir might be absent
        let _ = fs::remove_dir_all(path_to_blocks);
        fs::create_dir_all(path_to_blocks).expect("Failed to create directory");
    }

    #[test]
    fn block_save_test() {
        let path_to_blocks = path_to_blocks("block_save");
        setup(&path_to_blocks);
        let client_key = KeyManager::create_key();
        let mut block_keeper = create_block_keeper(path_to_blocks, 1);

        let client_transaction = create_test_transaction(&client_key, 1);
        if let BlockStatus::NewBlockCreated { block_hash } =
            block_keeper.add_transaction(client_transaction.clone())
        {
            block_keeper.commit_block(&block_hash).unwrap();
            let block_file =
                BlockFile::read_from_disk_by_index(&block_keeper.path_to_blocks, 1).unwrap();
            assert_eq!(block_file.transactions.len(), 1);
            assert!(block_file.transactions.contains(&client_transaction));
            assert_eq!(block_file.index, 1);
        } else {
            panic!("New block not created");
        }
    }

    #[test]
    fn block_must_be_created_only_when_mempool_is_full() {
        let path_to_blocks = path_to_blocks("mempool");
        setup(&path_to_blocks);
        let client_key = KeyManager::create_key();
        let mut block_keeper = create_block_keeper(path_to_blocks, 2);
        let client_transaction = create_test_transaction(&client_key, 1);
        match block_keeper.add_transaction(client_transaction.clone()) {
            BlockStatus::AddedToMempool => {}
            _ => panic!("Block must be created only when mempool is full"),
        }
        let client_transaction = create_test_transaction(&client_key, 2);
        match block_keeper.add_transaction(client_transaction.clone()) {
            BlockStatus::AddedToMempool => panic!("Block must have been created"),
            _ => {}
        }
    }

    fn create_block_keeper(path_to_blocks: PathBuf, size: usize) -> BlockKeeper {
        BlockKeeper {
            path_to_blocks,
            mempool_size: size,
            mempool: HashMap::with_capacity(size),
            pending_transactions: HashMap::with_capacity(size),
            uncommited_blocks: HashMap::new(),
            block_storage_state: Arc::new(RwLock::new(BlockStorageState {
                block_height: 0,
                last_commited_hash: EMPTY_HASH,
            })),
            previous_hash: EMPTY_HASH,
        }
    }

    fn create_test_transaction(client_key: &SigningKey, sequence_number: u32) -> SignedTransaction {
        let transaction = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number,
            },
        };

        SignedTransaction::new(transaction.clone(), &client_key)
    }

    fn path_to_blocks(test_name: &str) -> PathBuf {
        PathBuf::from("target/test/data").join(test_name)
    }

    #[test]
    fn parse_block_state() {
        let json = r#"{
            "block_height": 1,
            "last_commited_hash": [
                201, 46, 232, 215, 209, 112, 226, 37,
                147, 150, 52, 152, 180, 126, 24, 76,
                9, 233, 50, 205, 231, 207, 132, 122,
                23, 141, 98, 78, 234, 112, 183, 142
            ]
         }"#;
        let block_state: BlockStorageState = serde_json::from_str(json).unwrap();
        assert_eq!(1, block_state.block_height)
    }

    #[test]
    fn missing_block_returns_read_error() {
        let path_to_blocks = path_to_blocks("missing_block");
        setup(&path_to_blocks);

        let result = BlockFile::read_from_disk_by_index(&path_to_blocks, 99);

        match result {
            Err(e) => assert!(e.starts_with("Block file not found")),
            Ok(_) => panic!("Expected missing block to return an error"),
        }
    }

    #[test]
    fn corrupt_block_returns_read_error() {
        let path_to_blocks = path_to_blocks("corrupt_block");
        setup(&path_to_blocks);
        fs::write(
            path_to_blocks.join(BlockFile::block_filename_for_index(1)),
            "{",
        )
        .expect("Failed to write corrupt block fixture");

        let result = BlockFile::read_from_disk_by_index(&path_to_blocks, 1);

        match result {
            Err(e) => assert!(e.starts_with("Failed to deserialize")),
            Ok(_) => panic!("Expected corrupt block to return an error"),
        }
    }
}
