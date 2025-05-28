use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct PeerId {
    id: u32,
}

struct Peer {
    id: PeerId,
    known_peers: Vec<PeerId>,
}

enum MessageBody {
    PING,
    PONG,
}

struct Message {
    from: PeerId,
    to: PeerId,
    body: MessageBody,
}

struct Network {
    senders: HashMap<PeerId, Sender<Message>>,
    receivers: HashMap<PeerId, Receiver<Message>>,
}

impl Network {
    fn new() -> Network {
        Network {
            senders: HashMap::new(),
            receivers: HashMap::new(),
        }
    }
    fn send(&self, message: Message) {
        self.senders[&message.to]
            .send(message)
            .expect("Failed to send message");
    }

    fn receive(&self, peer: PeerId) -> Message {
        self.receivers[&peer]
            .recv()
            .expect("Failed to receive message")
    }

    fn add_peer(&mut self, peer: PeerId) {
        let (sender, receiver) = mpsc::channel();
        self.senders.insert(peer.clone(), sender);
        self.receivers.insert(peer, receiver);
    }
}

impl Peer {
    fn new(id: u32) -> Peer {
        Peer {
            id: PeerId { id },
            known_peers: Vec::new(),
        }
    }

    fn add_known_peer(&mut self, peer: PeerId) {
        self.known_peers.push(peer);
    }

    pub fn send_ping(&self, to: PeerId, network: &Network) {
        network.send(Message {
            from: self.id.clone(),
            to,
            body: MessageBody::PING,
        });
    }

    fn reply_to_message_if_exist(&self, network: &Network) {
        let message = network.receive(self.id.clone());
        match message.body {
            MessageBody::PING => {
                network.send(Message {
                    from: self.id.clone(),
                    to: message.from.clone(),
                    body: MessageBody::PONG,
                });
            }
            MessageBody::PONG => {
                // do nothing
            }
        }
    }

    fn run() {}
}

#[cfg(test)]
mod tests {
    use crate::{Network, Peer};

    #[test]
    fn simple_test() {
        let mut peer1 = Peer::new(1);
        let mut peer2 = Peer::new(2);
        peer1.add_known_peer(peer2.id.clone());
        peer2.add_known_peer(peer1.id.clone());
        let mut network = Network::new();
        network.add_peer(peer1.id.clone());
        network.add_peer(peer2.id.clone());
        
        peer1.send_ping(peer2.id.clone(), &network);
        peer2.reply_to_message_if_exist(&network);
        peer1.reply_to_message_if_exist(&network);
    } 
}
