use crate::storage::BlockKeeper;
use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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



pub struct TransactionProcessor {
    accounts: HashMap<String, Account>,
}

impl TransactionProcessor {
    
    pub fn new() -> Self {
        Self {
            accounts: HashMap::new(),
        }
    }

    pub fn process_transaction(&mut self, transaction: Transaction) {
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

    pub fn read_state(&mut self, block_keeper: &BlockKeeper) {
        let block_names = block_keeper.list_all_blocks();
        for block_name in block_names {
            let transactions = block_keeper.read_transactions_from_disk(&block_name);
            for transaction in transactions {
                self.process_transaction(transaction);
            }
        }
    }
}




