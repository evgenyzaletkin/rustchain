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
    use rustchain::storage::{BlockKeeper};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use rustchain::crypto::KeyManager;

    const TEST_DATA_PATH: &str = "target/test/data";

    #[test]
    fn test_key_from_key_manager() {
        let key_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        recreate_dir(&key_dir);
        let signing_key: SigningKey = KeyManager::get_or_create_key(&key_dir);
        let operation = Operation::AddCoin {
            amount: 10,
            asset_type: AssetType::BTC,
        };
        let signature = KeyManager::sign_message(&signing_key, &operation);
        let public_key = VerifyingKey::from(&signing_key);
        let transaction1 = Transaction {
            operation: Operation::AddCoin {
                amount: 10,
                asset_type: AssetType::BTC,
            },
            signature,
            public_key,
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
