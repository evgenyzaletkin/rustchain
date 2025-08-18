#[cfg(test)]
mod tests {
    use k256::ecdsa::SigningKey;
    use rustchain::crypto::KeyManager;
    use rustchain::network::local_network::LocalNetwork;
    use rustchain::peer::{Message, MessageBody, Peer, PeerId};
    use rustchain::storage::BlockKeeper;
    use rustchain::transactions::{AssetType, Metadata, Operation, SignedTransaction, Transaction};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    const TEST_DATA_PATH: &str = "target/test/data";

    #[tokio::test]
    async fn test_client_transaction_mempool_size_2() {
        let mut network = LocalNetwork::default();
        let peer_id_2 = PeerId::from(2);
        network.add_known_peer(peer_id_2);
        let network = Arc::new(network);
        let mut peer = create_peer(PeerId::from(1), network.clone(), 2);

        let client_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        let client_transaction = MessageBody::ClientTransaction(transaction.clone());
        let client_msg = Message {
            from: PeerId::from(0),
            to: peer.id.clone(),
            body: client_transaction,
        };

        peer.handle_message(client_msg);

        let vec = network.get_broadcasted_messages();
        assert_eq!(vec.len(), 1);
        assert!(vec.iter().any(|msg_body| {
            matches!(msg_body, MessageBody::Synchronization(verified_transaction) if verified_transaction.client_tx == transaction)
        }));
    }

    #[tokio::test]
    async fn test_client_transaction_mempool_size_1() {
        let mut network = LocalNetwork::default();
        let peer_id_2 = PeerId::from(2);
        network.add_known_peer(peer_id_2);
        let network = Arc::new(network);
        let mut peer = create_peer(PeerId::from(1), network.clone(), 1);

        let client_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        let client_transaction = MessageBody::ClientTransaction(transaction.clone());
        let client_msg = Message {
            from: PeerId::from(0),
            to: peer.id.clone(),
            body: client_transaction,
        };

        peer.handle_message(client_msg);

        let broadcasted_messages = network.get_broadcasted_messages();
        assert_eq!(broadcasted_messages.len(), 2);
        assert!(broadcasted_messages.iter().any(|msg_body| {
            matches!(msg_body, MessageBody::Synchronization(verified_transaction) if verified_transaction.client_tx == transaction)
        }));
        assert!(
            broadcasted_messages.iter()
                .any(|msg_body| { matches!(msg_body, MessageBody::BlockProposal { .. }) })
        );
    }

    #[tokio::test]
    async fn test_block_approval() {
        let mut network = LocalNetwork::default();
        let peer_id_2 = PeerId::from(2);
        let peer_id_3 = PeerId::from(3);
        let peer_id_4 = PeerId::from(4);
        network.add_known_peer(peer_id_2);
        network.add_known_peer(peer_id_3);
        network.add_known_peer(peer_id_4);
        let network = Arc::new(network);
        let mut peer = create_peer(PeerId::from(1), network.clone(), 1);

        let client_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        let client_transaction = MessageBody::ClientTransaction(transaction.clone());
        let client_msg = Message {
            from: PeerId::from(0),
            to: peer.id.clone(),
            body: client_transaction,
        };

        peer.handle_message(client_msg);

        let mut broadcasted_messages = network.get_broadcasted_messages();
        assert_eq!(broadcasted_messages.len(), 2);
        assert!(match &broadcasted_messages[0] {
            MessageBody::Synchronization(verified_transaction) => verified_transaction.client_tx == transaction,
            _ => false
        });
        let block_hash = match &broadcasted_messages[1] {
            MessageBody::BlockProposal{ block_hash, block_file, signature, public_key } => block_hash.clone(),
            _ => panic!("Expected block proposal message")
        };
        let approve_body = MessageBody::BlockApproved {block_hash};
        let approve_from_2 = Message {
            from: peer_id_2,
            to: peer.id.clone(),
            body: approve_body.clone(),
        };
        peer.handle_message(approve_from_2);
        broadcasted_messages = network.get_broadcasted_messages();
        assert_eq!(broadcasted_messages.len(), 2);

        let approve_from_3 = Message {
            from: peer_id_3,
            to: peer.id.clone(),
            body: approve_body.clone(),
        };
        peer.handle_message(approve_from_3);
        broadcasted_messages = network.get_broadcasted_messages();
        assert_eq!(broadcasted_messages.len(), 3);
        assert!(broadcasted_messages.iter().any(|msg_body| {
            matches!(msg_body, MessageBody::BlockApproved { .. })
        }));

    }

    fn create_peer(peer_id: PeerId, network: Arc<LocalNetwork>, size: usize) -> Peer<LocalNetwork> {
        let (_, receiver) = mpsc::channel(1000);
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join("peer_1");
        recreate_dir(&peer_1_dir);
        Peer::<LocalNetwork>::create_with_storage(
            peer_id,
            receiver,
            peer_1_dir.clone(),
            BlockKeeper::new(peer_1_dir.clone(), size),
            network.clone(),
        )
    }


    fn recreate_dir(path: &PathBuf) {
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("Failed to create directory");
    }

    fn create_test_transaction(signing_key: &SigningKey) -> SignedTransaction {
        let transaction = Transaction {
            operation: Operation::AddCoin {
                asset_type: AssetType::USDT,
                amount: 10,
            },
            metadata: Metadata {
                timestamp_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                sequence_number: 1,
            },
        };

        SignedTransaction::new(transaction, signing_key)
    }
}
