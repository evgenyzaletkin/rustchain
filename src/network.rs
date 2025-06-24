use crate::{Message, MessageBody, PeerId};
use rand::prelude::IteratorRandom;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc::Sender;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct RegisterRequest {
    pub peer_id: PeerId,
}

#[derive(Serialize, Deserialize)]
pub struct PeersResponse {
    pub peers: Vec<PeerWithAddr>,
}

impl From<&HashMap<PeerId, SocketAddr>> for PeersResponse{
    fn from(peers: &HashMap<PeerId, SocketAddr>) -> Self {
        let peers: Vec<PeerWithAddr> = peers
            .into_iter()
            .map(|(peer_id, addr)| PeerWithAddr::new(*peer_id, addr.clone()))
            .collect();
        Self { peers }
    }
}

#[derive(Serialize, Deserialize)]
struct PeerWithAddr {
    peer_id: PeerId,
    addr: SocketAddr,
}

impl PeerWithAddr {
    pub fn new(peer_id: PeerId, addr: SocketAddr) -> Self {
        Self { peer_id, addr }
    }
}

#[derive(Default)]
pub struct Network {
    senders: HashMap<PeerId, Sender<Message>>,
}

impl Network {
    pub fn send_client_message(&self, body: MessageBody) -> Result<(), String> {
        let to = self
            .senders
            .keys()
            .choose(&mut rand::rng())
            .ok_or_else(|| "Warning: No peers to send message to".to_string())?;
        self.send(Message {
            from: 0.into(),
            to: *to,
            body,
        });
        Ok(())
    }

    pub fn send(&self, message: Message) {
        if let Some(sender) = self.senders.get(&message.to) {
            sender.send(message).expect("Failed to send message");
        } else {
            println!(
                "Warning: Attempted to send message to unknown peer {:?}",
                message.to
            );
        }
    }

    pub fn broadcast(&self, message_body: &MessageBody, from: PeerId, recipients: &Vec<PeerId>) {
        for to in recipients {
            self.send(Message {
                from,
                to: *to,
                body: message_body.clone(),
            })
        }
    }

    pub fn add_peer(&mut self, peer_id: PeerId, sender: Sender<Message>) {
        self.senders.insert(peer_id, sender);
    }
}
