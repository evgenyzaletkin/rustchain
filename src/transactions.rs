use crate::crypto::Signable;
use crate::storage::{BlockKeeper, KeyManager};
use hex::FromHex;
use k256::ecdsa::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;


#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Metadata {
    pub timestamp_nanos: u128,
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
    pub signature: Signature,
    pub public_key: VerifyingKey,
    pub metadata: Metadata,
}

impl Transaction {
    pub fn tx_id(&self) -> String {
        hex::encode(self.signature.to_bytes())
    }
}

impl Signable for Transaction {}
impl Signable for Operation {}

pub struct Account {
    pub asset_type: AssetType,
    pub balance: u32,
}

#[derive(Default)]
pub struct TransactionProcessor {
    accounts: HashMap<String, Account>,
}

impl TransactionProcessor {

    pub fn process_transaction(&mut self, transaction: Transaction) {
        match transaction.operation {
            Operation::AddCoin { asset_type, amount } => {
                self.add_coin(
                    KeyManager::to_string_hex(&transaction.public_key),
                    asset_type.clone(),
                    amount,
                );
            }
            Operation::Send {
                recipient,
                amount,
                asset_type,
            } => self.send_coins(
                KeyManager::to_string_hex(&transaction.public_key),
                recipient,
                asset_type,
                amount,
            ),
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
