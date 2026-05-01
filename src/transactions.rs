use crate::crypto::{KeyManager, Signable};
use crate::storage::BlockKeeper;
use k256::ecdsa::signature::{Signer, Verifier};
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct Metadata {
    pub timestamp_nanos: u128,
    pub sequence_number: u32,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone, Debug)]
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
    pub metadata: Metadata,
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct SignedTransaction {
    pub transaction: Transaction,
    pub signature: Signature,
    pub public_key: VerifyingKey,
}

impl Debug for SignedTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SignedTransaction {}", self.tx_id())
    }
}

#[derive(Serialize, Deserialize, Eq, PartialEq, Clone)]
pub struct VerifiedTransaction {
    pub client_tx: SignedTransaction,
    pub peer_signature: Signature,
    pub peer_public_key: VerifyingKey,
}

impl Transaction {
    pub fn to_sign_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("Failed to serialize Transaction")
    }
}

impl SignedTransaction {
    pub fn new(transaction: Transaction, signing_key: &SigningKey) -> Self {
        let signature = signing_key.sign(&transaction.to_sign_bytes());
        let public_key = VerifyingKey::from(signing_key);

        Self {
            transaction,
            signature,
            public_key,
        }
    }

    pub fn verify(&self) -> Result<(), String> {
        self.public_key
            .verify(&self.transaction.to_sign_bytes(), &self.signature)
            .map_err(|e| format!("Invalid client signature: {}", e))
    }

    pub fn tx_id(&self) -> String {
        hex::encode(self.signature.to_bytes())
    }
}

impl VerifiedTransaction {
    pub fn new(client_tx: SignedTransaction, peer_key: &SigningKey) -> Self {
        let peer_signature = peer_key.sign(&serde_json::to_vec(&client_tx).unwrap());
        let peer_public_key = VerifyingKey::from(peer_key);

        Self {
            client_tx,
            peer_signature,
            peer_public_key,
        }
    }

    pub fn verify(&self) -> Result<(), String> {
        // First verify the client transaction
        self.client_tx.verify()?;

        // Then verify the peer signature
        let client_tx_bytes = serde_json::to_vec(&self.client_tx)
            .map_err(|e| format!("Failed to serialize client transaction: {}", e))?;

        self.peer_public_key
            .verify(&client_tx_bytes, &self.peer_signature)
            .map_err(|e| format!("Invalid peer signature: {}", e))
    }
}

impl Signable for Transaction {}
impl Signable for Operation {}

pub struct Account {
    pub asset_type: AssetType,
    pub balance: u32,
}

#[derive(Debug, Eq, PartialEq)]
pub enum TransactionValidationError {
    AccountNotFound {
        account_id: String,
    },
    InsufficientFunds {
        account_id: String,
        balance: u32,
        amount: u32,
    },
    AssetMismatch {
        account_id: String,
        expected: AssetType,
        actual: AssetType,
    },
}

impl Display for TransactionValidationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionValidationError::AccountNotFound { account_id } => {
                write!(f, "Account {} not found", account_id)
            }
            TransactionValidationError::InsufficientFunds {
                account_id,
                balance,
                amount,
            } => write!(
                f,
                "Account {} has insufficient funds: balance {}, requested {}",
                account_id, balance, amount
            ),
            TransactionValidationError::AssetMismatch {
                account_id,
                expected,
                actual,
            } => write!(
                f,
                "Account {} asset mismatch: expected {:?}, got {:?}",
                account_id, expected, actual
            ),
        }
    }
}

#[derive(Default)]
pub struct TransactionProcessor {
    accounts: HashMap<String, Account>,
}

impl TransactionProcessor {
    pub fn process_transaction(
        &mut self,
        client_tx: SignedTransaction,
    ) -> Result<(), TransactionValidationError> {
        match client_tx.transaction.operation {
            Operation::AddCoin { asset_type, amount } => self.add_coin(
                KeyManager::to_string_hex(&client_tx.public_key),
                asset_type.clone(),
                amount,
            ),
            Operation::Send {
                recipient,
                amount,
                asset_type,
            } => self.send_coins(
                KeyManager::to_string_hex(&client_tx.public_key),
                recipient,
                asset_type,
                amount,
            ),
        }
    }

    fn add_coin(
        &mut self,
        id: String,
        asset_type: AssetType,
        amount: u32,
    ) -> Result<(), TransactionValidationError> {
        if let Some(account) = self.accounts.get_mut(&id) {
            if account.asset_type != asset_type {
                return Err(TransactionValidationError::AssetMismatch {
                    account_id: id,
                    expected: account.asset_type.clone(),
                    actual: asset_type,
                });
            }
            account.balance += amount;
        } else {
            self.accounts.insert(
                id,
                Account {
                    asset_type,
                    balance: amount,
                },
            );
        }
        Ok(())
    }

    fn send_coins(
        &mut self,
        id: String,
        to: String,
        asset_type: AssetType,
        amount: u32,
    ) -> Result<(), TransactionValidationError> {
        let Some(sender_account) = self.accounts.get(&id) else {
            return Err(TransactionValidationError::AccountNotFound { account_id: id });
        };
        if sender_account.asset_type != asset_type {
            return Err(TransactionValidationError::AssetMismatch {
                account_id: id,
                expected: sender_account.asset_type.clone(),
                actual: asset_type,
            });
        }
        if sender_account.balance < amount {
            return Err(TransactionValidationError::InsufficientFunds {
                account_id: id,
                balance: sender_account.balance,
                amount,
            });
        }

        if let Some(recipient_account) = self.accounts.get(&to) {
            if recipient_account.asset_type != asset_type {
                return Err(TransactionValidationError::AssetMismatch {
                    account_id: to,
                    expected: recipient_account.asset_type.clone(),
                    actual: asset_type,
                });
            }
        }

        self.accounts.get_mut(&id).unwrap().balance -= amount;
        self.add_coin(to, asset_type, amount)
    }

    pub fn get_account(&self, id: &str) -> Option<&Account> {
        self.accounts.get(id)
    }

    pub fn read_state(
        &mut self,
        block_keeper: &BlockKeeper,
    ) -> Result<(), TransactionValidationError> {
        let block_names = block_keeper.list_all_blocks();
        for block_name in block_names {
            let transactions = block_keeper.read_transactions_from_disk(&block_name);
            for transaction in transactions {
                self.process_transaction(transaction)?;
            }
        }
        Ok(())
    }
}
