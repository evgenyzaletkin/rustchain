use crate::storage::{BlockFile, BlockStatus, KeyManager};
use derive_more::with_trait::From;
use derive_more::{Constructor, Display};
use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use std::collections::HashMap;
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};
use storage::BlockKeeper;
use transactions::{Transaction, TransactionProcessor};

pub mod storage;
pub mod transactions;

#[derive(Clone, Eq, PartialEq, Hash, Copy, Debug, Display, From, Constructor)]
pub struct PeerId {
    id: u32,
}

#[derive(Display, Clone)]
pub enum MessageBody {
    Ping,
    Pong,
    #[display("Transaction")]
    ClientTransaction(Transaction),
    #[display("Transaction")]
    NotifyTransaction {
        // for broadcasting
        transaction: Arc<Transaction>,
    },
    #[display("BlockProposal")]
    BlockProposal {
        // for broadcasting
        block_file: Arc<BlockFile>,
        signature: String,
        public_key: String,
    },
    BlockVote,
}

#[derive(Display)]
#[display("{from} -> {to} ")]
pub struct Message {
    from: PeerId,
    to: PeerId,
    body: MessageBody,
}

#[derive(Default)]
pub struct Network {
    senders: HashMap<PeerId, Sender<Message>>,
}

impl Network {
    fn send(&self, message: Message) {
        if let Some(sender) = self.senders.get(&message.to) {
            sender.send(message).expect("Failed to send message");
        } else {
            println!(
                "Warning: Attempted to send message to unknown peer {:?}",
                message.to
            );
        }
    }

    pub fn add_peer(&mut self, peer_id: PeerId, sender: Sender<Message>) {
        self.senders.insert(peer_id, sender);
    }
}

struct Voting {
    participants: Vec<PeerId>,
    approvals: Vec<PeerId>,
    rejections: Vec<PeerId>,
}

impl Voting {
    fn new(participants: Vec<PeerId>) -> Voting {
        Voting {
            approvals: Vec::with_capacity(participants.len()),
            rejections: Vec::with_capacity(participants.len()),
            participants,
        }
    }
}

pub struct Peer {
    pub id: PeerId,
    known_peers: Vec<PeerId>,
    last_ping_times: HashMap<PeerId, Instant>,
    last_response_times: HashMap<PeerId, Instant>,
    receiver: Receiver<Message>,
    transaction_processor: TransactionProcessor,
    block_keeper: BlockKeeper,
    votings: HashMap<String, Voting>,
    signing_key: SigningKey,
    public_key: VerifyingKey,
}

impl Peer {
    const PING_INTERVAL: Duration = Duration::from_secs(10);
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);

    pub fn new(id: u32, receiver: Receiver<Message>) -> Peer {
        let peer_dir = PathBuf::from(storage::DEFAULT_PATH_TO_BLOCKS).join(format!("peer_{}", id));
        let signing_key = KeyManager::get_or_create_key(&peer_dir);
        let public_key = VerifyingKey::from(signing_key.clone());
        Peer {
            id: id.into(),
            known_peers: Vec::new(),
            last_response_times: HashMap::new(),
            last_ping_times: HashMap::new(),
            receiver,
            transaction_processor: TransactionProcessor::new(),
            signing_key,
            public_key,
            block_keeper: BlockKeeper::new(peer_dir, storage::DEFAULT_MEMPOOL_SIZE),
            votings: HashMap::new(),
        }
    }

    pub fn create_with_storage(
        id: u32,
        receiver: Receiver<Message>,
        peer_dir: PathBuf,
        block_keeper: BlockKeeper,
    ) -> Peer {
        let signing_key = KeyManager::get_or_create_key(&peer_dir);
        let public_key = VerifyingKey::from(signing_key.clone());
        Peer {
            id: id.into(),
            known_peers: Vec::new(),
            last_response_times: HashMap::new(),
            last_ping_times: HashMap::new(),
            receiver,
            transaction_processor: TransactionProcessor::new(),
            signing_key,
            public_key,
            block_keeper,
            votings: HashMap::new(),
        }
    }

    pub fn connect_with_peer(&mut self, peer: PeerId) {
        self.known_peers.push(peer);
    }

    pub fn send_ping(&self, to: PeerId, network: &Network) {
        network.send(Message {
            from: self.id.clone(),
            to,
            body: MessageBody::Ping,
        });
    }

    fn process_message(&mut self, network: &Network) -> bool {
        let result = self.receiver.recv_timeout(Self::RECV_TIMEOUT);
        match result {
            Ok(message) => Some(message),
            Err(mpsc::RecvTimeoutError::Timeout) => None,
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("Channel disconnected"),
        }
        .map_or(false, |message| {
            self.handle_message(message, network);
            true
        })
    }

    fn handle_message(&mut self, message: Message, network: &Network) {
        println!("Received message: {message}");
        self.last_response_times
            .insert(message.from, Instant::now());
        match message.body {
            MessageBody::ClientTransaction(transaction) => {
                self.process_client_transaction(transaction, network)
            }
            MessageBody::Ping => {
                network.send(Message {
                    from: self.id,
                    to: message.from,
                    body: MessageBody::Pong,
                });
            }
            MessageBody::Pong => {}
            MessageBody::NotifyTransaction { transaction } => {
                self.process_transaction_notification(transaction, network)
            }
            MessageBody::BlockProposal {
                block_file: _,
                signature: _,
                public_key: _,
            } => {}
            MessageBody::BlockVote => {}
        }
    }

    fn process_client_transaction(&mut self, transaction: Transaction, network: &Network) {
        // let's keep two methods for now, since the logic for client and notification transaction
        // might be different in the future
        // For now, only Arc
        self.process_transaction_general(Arc::new(transaction), network);
    }

    fn process_transaction_notification(
        &mut self,
        transaction: Arc<Transaction>,
        network: &Network,
    ) {
        self.process_transaction_general(transaction, network);
    }

    fn process_transaction_general(&mut self, transaction: Arc<Transaction>, network: &Network) {
        self.transaction_processor
            .process_transaction(transaction.deref().clone());
        let status = self
            .block_keeper
            .add_transaction(transaction.deref().clone());
        self.broadcast_message(
            network,
            MessageBody::NotifyTransaction {
                transaction: transaction.clone(),
            },
        );
        if let BlockStatus::NewBlockCreated { block_hash } = status {
            if (!self.known_peers.is_empty()) {
                if let Some(block_file) = self.block_keeper.get_uncommited_block(&block_hash) {
                    let block_string =
                        serde_json::to_string(&block_file).expect("Failed to serialize block file");
                    let signature: Signature = self.signing_key.sign(block_string.as_bytes());
                    self.broadcast_message(
                        network,
                        MessageBody::BlockProposal {
                            block_file: Arc::new(block_file.clone()),
                            signature: signature.to_string(),
                            public_key: KeyManager::key_to_hex_string(&self.public_key),
                        },
                    )
                }
                self.votings.insert(
                    block_hash.to_string(),
                    Voting::new(self.known_peers.clone()),
                );
            } else {
                self.block_keeper
                    .commit_block(&block_hash)
                    .expect("Failed to commit block");
            }
        }
    }

    fn process_block_proposal(
        block_file: Arc<BlockFile>,
        signature: String,
        public_key: String,
        network: &Network,
    ) {
    }

    fn broadcast_message(&mut self, network: &Network, message_body: MessageBody) {
        for to in &self.known_peers {
            network.send(Message {
                from: self.id.clone(),
                to: to.clone(),
                body: message_body.clone(),
            })
        }
    }

    fn disconnect_dead_peers(&mut self) {
        self.known_peers.retain(|peer| {
            let last_ping_opt = self.last_ping_times.get(peer);
            let last_response_opt = self.last_response_times.get(peer);
            let retain = match (last_ping_opt, last_response_opt) {
                (None, _) => true,
                (Some(last_ping), None) => last_ping.elapsed() < Self::PING_INTERVAL,
                (Some(last_ping), Some(last_response)) => {
                    last_ping.elapsed() - last_response.elapsed() < Self::PING_INTERVAL
                }
            };
            if !retain {
                println!("{:?} is dead", peer);
            }
            retain
        });
    }

    fn send_ping_to_peers(&mut self, network: &Network) {
        for peer in &self.known_peers {
            let should_send_ping = match self.last_ping_times.get(peer) {
                Some(last_sent_time) => last_sent_time.elapsed() > Self::PING_INTERVAL,
                None => true,
            };
            if should_send_ping {
                self.send_ping(*peer, network);
                self.last_ping_times.insert(*peer, Instant::now());
            }
        }
    }

    pub fn run(&mut self, network: &Network) {
        loop {
            // Process any available messages
            while self.process_message(network) {}

            // Check for and disconnect dead peers
            self.disconnect_dead_peers();
            // Send pings only when needed (the method already has the timing logic)
            self.send_ping_to_peers(network);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Network, Peer};
    use std::sync::mpsc;

    #[test]
    fn simple_test() {
        let (sender1, receiver1) = mpsc::channel();
        let (sender2, receiver2) = mpsc::channel();
        let mut peer1 = Peer::new(1, receiver1);
        let mut peer2 = Peer::new(2, receiver2);
        peer1.connect_with_peer(peer2.id);
        peer2.connect_with_peer(peer1.id);
        let mut network = Network::default();
        network.add_peer(peer1.id, sender1);
        network.add_peer(peer2.id, sender2);

        peer1.send_ping(peer2.id, &network);
        assert!(peer2.process_message(&network));
        assert!(peer1.process_message(&network));
    }
}
