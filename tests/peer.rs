#[cfg(test)]
mod tests {
    use k256::ecdsa::SigningKey;
    use rustchain::consensus::raft::{DEFAULT_ELECTION_TIMEOUT, DEFAULT_ELECTION_TIMEOUT_JITTER};
    use rustchain::consensus::{ConsensusEngine, ConsensusInput, ConsensusState, RaftRoleState};
    use rustchain::crypto::KeyManager;
    use rustchain::network::NetworkInterface;
    use rustchain::network::local_network::LocalNetwork;
    use rustchain::peer::{Message, MessageBody, Peer, PeerId};
    use rustchain::storage::BlockKeeper;
    use rustchain::transactions::{
        AssetType, Metadata, Operation, SignedTransaction, Transaction, VerifiedTransaction,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use tokio::sync::mpsc;

    const TEST_DATA_PATH: &str = "target/test/data";
    static TEST_DIR_COUNTER: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn test_peer_state_view_tracks_raft_state() {
        let network = Arc::new(LocalNetwork::default());
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 5);
        let state_view = peer.create_state_view();

        let initial_state = state_view.get_state(vec![PeerId::from(3), PeerId::from(2)]);
        assert_eq!(initial_state.peer_id, PeerId::from(1));
        assert_eq!(
            initial_state.known_peers,
            vec![PeerId::from(3), PeerId::from(2)]
        );
        assert_eq!(initial_state.block.block_height, 0);
        assert_eq!(
            initial_state.consensus,
            ConsensusState::Raft {
                role: RaftRoleState::Follower,
                term: 0,
                leader_id: None,
                commit_index: 0,
                last_log_index: 0,
            }
        );

        tick_consensus(
            &mut peer,
            &network,
            Instant::now()
                + DEFAULT_ELECTION_TIMEOUT
                + DEFAULT_ELECTION_TIMEOUT_JITTER
                + Duration::from_millis(1),
        );

        let leader_state = state_view.get_state(Vec::new());
        assert_eq!(
            leader_state.consensus,
            ConsensusState::Raft {
                role: RaftRoleState::Leader,
                term: 1,
                leader_id: Some(PeerId::from(1)),
                commit_index: 0,
                last_log_index: 0,
            }
        );
        let serialized = serde_json::to_value(leader_state).unwrap();
        assert_eq!(serialized["consensus"]["mode"], "raft");
        assert_eq!(serialized["consensus"]["role"], "leader");
    }

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
        assert_eq!(broadcasted_messages.len(), 3);
        assert!(broadcasted_messages.iter().any(|msg_body| {
            matches!(msg_body, MessageBody::Synchronization(verified_transaction) if verified_transaction.client_tx == transaction)
        }));
        assert!(
            broadcasted_messages
                .iter()
                .any(|msg_body| { matches!(msg_body, MessageBody::BlockProposal { .. }) })
        );
        assert!(
            broadcasted_messages
                .iter()
                .any(|msg_body| { matches!(msg_body, MessageBody::BlockApproved { .. }) })
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
            MessageBody::Synchronization(verified_transaction) =>
                verified_transaction.client_tx == transaction,
            _ => false,
        });
        let block_hash = match &broadcasted_messages[1] {
            MessageBody::BlockProposal { block_hash, .. } => block_hash.clone(),
            _ => panic!("Expected block proposal message"),
        };
        let approve_body = MessageBody::BlockApproved { block_hash };
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
        assert!(
            broadcasted_messages
                .iter()
                .any(|msg_body| { matches!(msg_body, MessageBody::BlockApproved { .. }) })
        );

        let approve_from_4 = Message {
            from: peer_id_4,
            to: peer.id.clone(),
            body: approve_body.clone(),
        };
        peer.handle_message(approve_from_4);
        assert_eq!(network.get_broadcasted_messages().len(), 3);

        peer.make_vote(block_hash, peer_id_4, true).unwrap();
        assert_eq!(network.get_broadcasted_messages().len(), 3);
    }

    #[tokio::test]
    async fn test_raft_election_request_is_broadcast() {
        let mut network = LocalNetwork::default();
        network.add_known_peer(PeerId::from(2));
        network.add_known_peer(PeerId::from(3));
        let network = Arc::new(network);
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 1);

        tick_consensus(
            &mut peer,
            &network,
            Instant::now() + Duration::from_secs(60),
        );

        let broadcasted_messages = network.get_broadcasted_messages();
        assert_eq!(broadcasted_messages.len(), 1);
        assert!(matches!(
            broadcasted_messages[0],
            MessageBody::RaftRequestVote {
                term: 1,
                candidate_id,
            } if candidate_id == PeerId::from(1)
        ));
    }

    #[tokio::test]
    async fn test_raft_vote_response_is_sent_to_candidate() {
        let (candidate_sender, mut candidate_receiver) = mpsc::channel(1000);
        let mut network = LocalNetwork::default();
        network.add_peer(PeerId::from(2), candidate_sender);
        let network = Arc::new(network);
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 1);
        tick_consensus(&mut peer, &network, Instant::now());

        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::RaftRequestVote {
                term: 1,
                candidate_id: PeerId::from(2),
            },
        });

        let response = candidate_receiver.try_recv().unwrap();
        assert_eq!(response.from, PeerId::from(1));
        assert_eq!(response.to, PeerId::from(2));
        assert!(matches!(
            response.body,
            MessageBody::RaftRequestVoteResponse {
                term: 1,
                vote_granted: true,
            }
        ));
        assert!(network.get_broadcasted_messages().is_empty());
    }

    #[tokio::test]
    async fn test_raft_follower_forwards_valid_client_transaction_to_leader() {
        let (leader_sender, mut leader_receiver) = mpsc::channel(1000);
        let mut network = LocalNetwork::default();
        network.add_peer(PeerId::from(2), leader_sender);
        let network = Arc::new(network);
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 1);
        tick_consensus(&mut peer, &network, Instant::now());

        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::RaftAppendEntries {
                term: 1,
                leader_id: PeerId::from(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            },
        });

        let client_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        peer.handle_message(Message {
            from: PeerId::from(0),
            to: PeerId::from(1),
            body: MessageBody::ClientTransaction(transaction.clone()),
        });

        let received_messages = vec![
            leader_receiver.try_recv().unwrap(),
            leader_receiver.try_recv().unwrap(),
        ];
        assert!(received_messages.iter().any(|message| {
            message.from == PeerId::from(1)
                && message.to == PeerId::from(2)
                && matches!(
                    &message.body,
                    MessageBody::ClientTransaction(client_transaction)
                        if *client_transaction == transaction
                )
        }));
        assert!(network.get_broadcasted_messages().is_empty());
    }

    #[tokio::test]
    async fn test_raft_follower_does_not_propose_block_from_synchronized_transaction() {
        let mut network = LocalNetwork::default();
        network.add_known_peer(PeerId::from(2));
        let network = Arc::new(network);
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 1);
        tick_consensus(&mut peer, &network, Instant::now());

        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::RaftAppendEntries {
                term: 1,
                leader_id: PeerId::from(2),
                prev_log_index: 0,
                prev_log_term: 0,
                entries: Vec::new(),
                leader_commit: 0,
            },
        });

        let client_key = KeyManager::create_key();
        let leader_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        let verified_transaction = VerifiedTransaction::new(transaction, &leader_key);
        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::Synchronization(verified_transaction),
        });

        assert!(
            !network
                .get_broadcasted_messages()
                .iter()
                .any(|message| matches!(message, MessageBody::BlockProposal { .. }))
        );
    }

    #[tokio::test]
    async fn test_synchronized_transaction_is_validated_before_mempool() {
        let mut network = LocalNetwork::default();
        network.add_known_peer(PeerId::from(2));
        let network = Arc::new(network);
        let mut peer = create_peer(PeerId::from(1), network.clone(), 1);

        let client_key = KeyManager::create_key();
        let peer_key = KeyManager::create_key();
        let transaction = create_invalid_send_transaction(&client_key);
        let verified_transaction = VerifiedTransaction::new(transaction, &peer_key);

        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::Synchronization(verified_transaction),
        });

        assert!(
            !network
                .get_broadcasted_messages()
                .iter()
                .any(|message| matches!(message, MessageBody::BlockProposal { .. }))
        );
    }

    #[tokio::test]
    async fn test_raft_leader_processes_client_transaction_locally() {
        let mut network = LocalNetwork::default();
        network.add_known_peer(PeerId::from(2));
        network.add_known_peer(PeerId::from(3));
        let network = Arc::new(network);
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 2);

        tick_consensus(
            &mut peer,
            &network,
            Instant::now() + Duration::from_secs(60),
        );
        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::RaftRequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        });

        let client_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        peer.handle_message(Message {
            from: PeerId::from(0),
            to: PeerId::from(1),
            body: MessageBody::ClientTransaction(transaction.clone()),
        });

        let broadcasted_messages = network.get_broadcasted_messages();
        assert_eq!(broadcasted_messages.len(), 1);
        assert!(matches!(
            &broadcasted_messages[0],
            MessageBody::RaftRequestVote { .. }
        ));
        assert!(
            !broadcasted_messages
                .iter()
                .any(|msg_body| { matches!(msg_body, MessageBody::Synchronization(_)) })
        );
    }

    #[tokio::test]
    async fn test_raft_leader_replicates_created_block_with_append_entries() {
        let (peer_2_sender, mut peer_2_receiver) = mpsc::channel(1000);
        let (peer_3_sender, mut peer_3_receiver) = mpsc::channel(1000);
        let mut network = LocalNetwork::default();
        network.add_peer(PeerId::from(2), peer_2_sender);
        network.add_peer(PeerId::from(3), peer_3_sender);
        let network = Arc::new(network);
        let mut peer = create_raft_peer(PeerId::from(1), network.clone(), 1);

        tick_consensus(
            &mut peer,
            &network,
            Instant::now() + Duration::from_secs(60),
        );
        peer.handle_message(Message {
            from: PeerId::from(2),
            to: PeerId::from(1),
            body: MessageBody::RaftRequestVoteResponse {
                term: 1,
                vote_granted: true,
            },
        });

        let client_key = KeyManager::create_key();
        let transaction = create_test_transaction(&client_key);
        peer.handle_message(Message {
            from: PeerId::from(0),
            to: PeerId::from(1),
            body: MessageBody::ClientTransaction(transaction),
        });

        let peer_2_messages = vec![
            peer_2_receiver.try_recv().unwrap(),
            peer_2_receiver.try_recv().unwrap(),
        ];
        let peer_3_messages = vec![
            peer_3_receiver.try_recv().unwrap(),
            peer_3_receiver.try_recv().unwrap(),
        ];
        assert!(peer_2_messages.iter().any(|message| matches!(
            &message.body,
            MessageBody::RaftAppendEntries {
                term: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 0,
                ..
            } if entries.len() == 1
                && entries[0].entry.term == 1
                && entries[0].entry.index == 1
                && !entries[0].block_file.is_empty()
        )));
        assert!(peer_3_messages.iter().any(|message| matches!(
            &message.body,
            MessageBody::RaftAppendEntries {
                term: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries,
                leader_commit: 0,
                ..
            } if entries.len() == 1
                && entries[0].entry.term == 1
                && entries[0].entry.index == 1
                && !entries[0].block_file.is_empty()
        )));
        assert!(!network.get_broadcasted_messages().iter().any(|message| {
            matches!(
                message,
                MessageBody::RaftAppendEntries { .. } | MessageBody::BlockProposal { .. }
            )
        }));
    }

    fn create_peer(peer_id: PeerId, network: Arc<LocalNetwork>, size: usize) -> Peer<LocalNetwork> {
        let dir_id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join(format!("peer_test_{}", dir_id));
        recreate_dir(&peer_1_dir);
        Peer::<LocalNetwork>::new(
            peer_id,
            network.clone(),
            ConsensusEngine::new_voting(peer_id),
            BlockKeeper::new(peer_1_dir.clone(), size),
            KeyManager::get_or_create_key(&peer_1_dir),
        )
    }

    fn create_raft_peer(
        peer_id: PeerId,
        network: Arc<LocalNetwork>,
        size: usize,
    ) -> Peer<LocalNetwork> {
        let dir_id = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
        let peer_1_dir = PathBuf::from(TEST_DATA_PATH).join(format!("peer_test_{}", dir_id));
        recreate_dir(&peer_1_dir);
        Peer::<LocalNetwork>::new(
            peer_id,
            network.clone(),
            ConsensusEngine::new_raft(peer_id),
            BlockKeeper::new(peer_1_dir.clone(), size),
            KeyManager::get_or_create_key(&peer_1_dir),
        )
    }

    fn tick_consensus(peer: &mut Peer<LocalNetwork>, network: &Arc<LocalNetwork>, now: Instant) {
        peer.handle_consensus_input(ConsensusInput::Tick {
            now,
            known_peers: network.known_peers(),
        })
        .unwrap();
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

    fn create_invalid_send_transaction(signing_key: &SigningKey) -> SignedTransaction {
        let recipient_key = KeyManager::create_key();
        let transaction = Transaction {
            operation: Operation::Send {
                recipient: KeyManager::to_string_hex(&recipient_key.verifying_key()),
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
