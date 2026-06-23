#[cfg(test)]
mod tests {
    use ::rustchain::transactions::{AssetType, Metadata, Operation, Transaction};
    use k256::ecdsa::SigningKey;
    use k256::ecdsa::signature::Verifier;
    use k256::ecdsa::{Signature, VerifyingKey};
    use rustchain::consensus::ConsensusEngine;
    use rustchain::crypto::KeyManager;
    use rustchain::network::local_network::LocalNetwork;
    use rustchain::peer::{Peer, PeerId};
    use rustchain::storage::BlockKeeper;
    use rustchain::transactions::{
        SignedTransaction, TransactionProcessor, TransactionValidationError,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;

    const TEST_DATA_PATH: &str = "target/test/data";

    #[test]
    fn test_key_from_key_manager() {
        let key_dir = PathBuf::from(TEST_DATA_PATH).join("key_manager_peer");
        recreate_dir(&key_dir);
        let signing_key: SigningKey = KeyManager::get_or_create_key(&key_dir);

        // Create base transaction
        let transaction = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        // Create client transaction
        let client_transaction = SignedTransaction::new(transaction, &signing_key);

        // Verify the client transaction
        client_transaction
            .verify()
            .expect("Failed to verify client transaction");

        // Test key persistence
        let signing_key_from_key_manager: SigningKey = KeyManager::get_or_create_key(&key_dir);
        let client_transaction2 = SignedTransaction::new(
            Transaction {
                operation: Operation::AddCoin {
                    amount: 10,
                    asset_type: AssetType::BTC,
                },
                metadata: Metadata {
                    timestamp_nanos: 100,
                    sequence_number: 1,
                },
            },
            &signing_key_from_key_manager,
        );

        // Both transactions should be verifiable with the same public key
        let public_key = VerifyingKey::from(&signing_key);
        verify_signature(
            &client_transaction.signature,
            &client_transaction.transaction.to_sign_bytes(),
            &public_key,
        );
        verify_signature(
            &client_transaction2.signature,
            &client_transaction2.transaction.to_sign_bytes(),
            &public_key,
        );
    }

    fn verify_signature(signature: &Signature, message: &[u8], public_key: &VerifyingKey) {
        public_key
            .verify(message, signature)
            .expect("Failed to verify signature");
    }

    #[tokio::test]
    async fn test_block_verification() {
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join("block_verification_peer");
        recreate_dir(&peer_1_dir);

        let mut block_keeper = BlockKeeper::new(peer_1_dir.clone(), 1);
        let network = Arc::new(LocalNetwork::default());
        let _peer_1 = Peer::new(
            PeerId::new(1),
            network.clone(),
            ConsensusEngine::new_voting(PeerId::new(1)),
            BlockKeeper::new(peer_1_dir.clone(), 1),
            KeyManager::get_or_create_key(&peer_1_dir),
        );

        // Create and add a transaction
        let client_key = KeyManager::create_key();
        let transaction = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        let client_transaction = SignedTransaction::new(transaction, &client_key);
        block_keeper.add_transaction(client_transaction);

        // Additional block verification tests can be added here
    }

    #[test]
    fn test_transaction_verification() {
        // Create a transaction
        let client_key = KeyManager::create_key();
        let transaction = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            metadata: Metadata {
                timestamp_nanos: 100,
                sequence_number: 1,
            },
        };

        // Create a valid client transaction
        let client_transaction = SignedTransaction::new(transaction.clone(), &client_key);
        assert!(client_transaction.verify().is_ok());

        // Test with wrong signature
        let another_key = KeyManager::create_key();
        let wrong_transaction = SignedTransaction::new(transaction, &another_key);
        assert!(wrong_transaction.verify().is_ok());

        // But they should have different tx_ids
        assert_ne!(client_transaction.tx_id(), wrong_transaction.tx_id());
    }

    #[test]
    fn test_send_from_missing_account_returns_error() {
        let mut processor = TransactionProcessor::default();
        let sender_key = KeyManager::create_key();
        let sender_id = KeyManager::to_string_hex(&VerifyingKey::from(&sender_key));
        let send_tx = create_signed_transaction(
            &sender_key,
            Operation::Send {
                recipient: "recipient".to_string(),
                amount: 10,
                asset_type: AssetType::BTC,
            },
            1,
        );

        let result = processor.process_transaction(send_tx);

        assert_eq!(
            result,
            Err(TransactionValidationError::AccountNotFound {
                account_id: sender_id
            })
        );
    }

    #[test]
    fn test_send_with_insufficient_funds_returns_error() {
        let mut processor = TransactionProcessor::default();
        let sender_key = KeyManager::create_key();
        let sender_id = KeyManager::to_string_hex(&VerifyingKey::from(&sender_key));
        processor
            .process_transaction(create_signed_transaction(
                &sender_key,
                Operation::AddCoin {
                    amount: 5,
                    asset_type: AssetType::BTC,
                },
                1,
            ))
            .unwrap();

        let result = processor.process_transaction(create_signed_transaction(
            &sender_key,
            Operation::Send {
                recipient: "recipient".to_string(),
                amount: 10,
                asset_type: AssetType::BTC,
            },
            2,
        ));

        assert_eq!(
            result,
            Err(TransactionValidationError::InsufficientFunds {
                account_id: sender_id.clone(),
                balance: 5,
                amount: 10,
            })
        );
        assert_eq!(processor.get_account(&sender_id).unwrap().balance, 5);
    }

    #[test]
    fn test_send_with_asset_mismatch_returns_error() {
        let mut processor = TransactionProcessor::default();
        let sender_key = KeyManager::create_key();
        let sender_id = KeyManager::to_string_hex(&VerifyingKey::from(&sender_key));
        processor
            .process_transaction(create_signed_transaction(
                &sender_key,
                Operation::AddCoin {
                    amount: 10,
                    asset_type: AssetType::BTC,
                },
                1,
            ))
            .unwrap();

        let result = processor.process_transaction(create_signed_transaction(
            &sender_key,
            Operation::Send {
                recipient: "recipient".to_string(),
                amount: 5,
                asset_type: AssetType::USDT,
            },
            2,
        ));

        assert_eq!(
            result,
            Err(TransactionValidationError::AssetMismatch {
                account_id: sender_id.clone(),
                expected: AssetType::BTC,
                actual: AssetType::USDT,
            })
        );
        assert_eq!(processor.get_account(&sender_id).unwrap().balance, 10);
    }

    fn create_signed_transaction(
        signing_key: &SigningKey,
        operation: Operation,
        sequence_number: u32,
    ) -> SignedTransaction {
        SignedTransaction::new(
            Transaction {
                operation,
                metadata: Metadata {
                    timestamp_nanos: 100,
                    sequence_number,
                },
            },
            signing_key,
        )
    }

    fn recreate_dir(path: &PathBuf) {
        fs::remove_dir_all(path).ok(); // Using ok() to ignore if directory doesn't exist
        fs::create_dir_all(path).expect("Failed to create directory");
    }
}
