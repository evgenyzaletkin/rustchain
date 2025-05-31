use crate::transactions::{Transaction, TransactionProcessor};
use derive_more::Display;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::{Duration, Instant};

pub mod transactions;

#[derive(Clone, Eq, PartialEq, Hash, Copy, Debug, Display)]
pub struct PeerId {
    id: u32,
}

impl PeerId {
    pub fn new(id: u32) -> PeerId {
        PeerId { id }
    }
}

pub struct Peer {
    pub id: PeerId,
    known_peers: Vec<PeerId>,
    last_ping_times: HashMap<PeerId, Instant>,
    last_response_times: HashMap<PeerId, Instant>,
    receiver: Receiver<Message>,
    transaction_processor: TransactionProcessor,
}

#[derive(Display)]
enum MessageBody {
    Ping,
    Pong,
    #[display("Transaction")]
    Transaction(Transaction),
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

impl Peer {
    const PING_INTERVAL: Duration = Duration::from_secs(10);
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);

    pub fn new(id: u32, receiver: Receiver<Message>) -> Peer {
        Peer {
            id: PeerId { id },
            known_peers: Vec::new(),
            last_response_times: HashMap::new(),
            last_ping_times: HashMap::new(),
            receiver,
            transaction_processor: TransactionProcessor::new(PeerId { id }),
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
            MessageBody::Transaction(transaction) => {
                self.transaction_processor.process_transaction(transaction)
            }
            MessageBody::Ping => {
                network.send(Message {
                    from: self.id,
                    to: message.from,
                    body: MessageBody::Pong,
                });
            }
            MessageBody::Pong => {}
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
