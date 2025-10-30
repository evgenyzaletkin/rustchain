#[cfg(test)]
mod tests {
    use ::rustchain::transactions::{AssetType, Metadata, Operation, Transaction};
    use k256::ecdsa::SigningKey;
    use k256::ecdsa::signature::Verifier;
    use k256::ecdsa::{Signature, VerifyingKey};
    use rustchain::crypto::KeyManager;
    use rustchain::network::local_network::LocalNetwork;
    use rustchain::peer::{Peer, PeerId};
    use rustchain::storage::BlockKeeper;
    use rustchain::transactions::SignedTransaction;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tokio::sync::mpsc;

    const TEST_DATA_PATH: &str = "target/test/data";

    #[test]
    fn test_key_from_key_manager() {
        let key_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
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
        let (send1, recv1) = mpsc::channel(1000);
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        recreate_dir(&peer_1_dir);

        let mut block_keeper = BlockKeeper::new(peer_1_dir.clone(), 1);
        let peer_1 = Peer::create_with_storage(
            PeerId::new(1),
            recv1,
            peer_1_dir.clone(),
            BlockKeeper::new(peer_1_dir.clone(), 1),
            Arc::new(LocalNetwork::default()),
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

    fn recreate_dir(path: &PathBuf) {
        fs::remove_dir_all(path).ok(); // Using ok() to ignore if directory doesn't exist
        fs::create_dir_all(path).expect("Failed to create directory");
    }
}
