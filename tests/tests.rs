use k256::ecdsa::signature::Signer;

#[cfg(test)]
mod tests {
    use ::rustchain::transactions::{
        AssetType, Metadata, Operation, Transaction, TransactionProcessor,
    };
    use k256::ecdsa::SigningKey;
    use k256::ecdsa::signature::{Signer, Verifier};
    use k256::ecdsa::{Signature, VerifyingKey};
    use rand::rng;
    use rustchain::Peer;
    use rustchain::storage::{BlockKeeper, BlockStatus, KeyManager};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc;

    const TEST_DATA_PATH: &str = "target/test/data";

    #[test]
    fn account_state_should_be_restored_from_saved_blocks() {
        let path_to_blocks = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        recreate_dir(&path_to_blocks);
        let mut block_keeper = BlockKeeper::new(path_to_blocks.clone(), 3);

        let transaction1 = Transaction {
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
        let mut transaction2 = transaction1.clone();
        transaction2.signature = "signature2".to_string();
        transaction2.public_key = "public_key2".to_string();
        let transaction3 = transaction1.clone();

        block_keeper.add_transaction(transaction1);
        block_keeper.add_transaction(transaction2);
        if let BlockStatus::NewBlockCreated { block_hash } =
            block_keeper.add_transaction(transaction3)
        {
            block_keeper
                .commit_block(&block_hash)
                .expect("Block commit failed");
            let block_keeper = BlockKeeper::new(path_to_blocks, 3);
            let mut transaction_processor = TransactionProcessor::new();
            transaction_processor.read_state(&block_keeper);
            validate_accounts(&transaction_processor);
        } else {
            panic!("New block not created");
        }
    }

    fn validate_accounts(transaction_processor: &TransactionProcessor) {
        let acc1 = transaction_processor.get_account("public_key1").unwrap();
        assert!(acc1.asset_type == AssetType::BTC);
        assert_eq!(acc1.balance, 20);
        let acc2 = transaction_processor.get_account("public_key2").unwrap();
        assert!(acc2.asset_type == AssetType::BTC);
        assert_eq!(acc2.balance, 10);
    }

    #[test]
    fn sign_message_and_verify_it() {
        let signing_key: SigningKey = SigningKey::random(&mut rng()); // Serialize with `::to_bytes()`
        let message =
            b"ECDSA proves knowledge of a secret number in the context of a single message";
        let signature: Signature = signing_key.sign(message);
        let verifying_key = VerifyingKey::from(signing_key);
        assert!(verifying_key.verify(message, &signature).is_ok());
    }

    #[test]
    fn test_key_from_key_manager() {
        let key_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        recreate_dir(&key_dir);
        let signing_key: SigningKey = KeyManager::get_or_create_key(&key_dir);
        let transaction1 = Transaction {
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
        let tx_str = serde_json::to_string(&transaction1).expect("Failed to serialize transaction");
        let signature: Signature = signing_key.sign(tx_str.as_bytes());
        let public_key = VerifyingKey::from(signing_key);
        verify_signature(&signature, tx_str.as_bytes(), &public_key);
        //     The same key should be returned when requested from KeyManager
        let signing_key_from_key_manager: SigningKey = KeyManager::get_or_create_key(&key_dir);
        let public_key = VerifyingKey::from(signing_key_from_key_manager);
        verify_signature(&signature, tx_str.as_bytes(), &public_key);
    }

    fn verify_signature(signature: &Signature, message: &[u8], public_key: &VerifyingKey) {
        public_key
            .verify(message, signature)
            .expect("Failed to verify signature");
    }

    #[test]
    fn test_block_verification() {
        let (send1, recv1) = mpsc::channel();
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        BlockKeeper::new(peer_1_dir.clone(), 1);
        let peer_1 = Peer::create_with_storage(
            1,
            recv1,
            peer_1_dir.clone(),
            BlockKeeper::new(peer_1_dir.clone(), 1),
        );
    //     TBC
    }

    fn recreate_dir(path: &PathBuf) {
        fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("Failed to create directory");
    }
}
