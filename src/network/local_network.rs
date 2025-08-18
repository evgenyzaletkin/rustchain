use crate::network::{NetworkInterface, NetworkMessage};
use crate::peer::{Message, MessageBody, PeerId};
use rand::prelude::IteratorRandom;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::de::DeserializeOwned;
use tokio::sync::mpsc::Sender;

#[derive(Default)]
pub struct LocalNetwork {
    senders: HashMap<PeerId, Sender<Message>>,
    known_peers: Vec<PeerId>,
    broadcasted_messages: Arc<Mutex<Vec<MessageBody>>>,
}

impl LocalNetwork {
    pub fn add_peer(&mut self, peer_id: PeerId, sender: Sender<Message>) {
        self.known_peers.push(peer_id);
        self.senders.insert(peer_id, sender);
    }
    
    // Just for voting testing
    pub fn add_known_peer(&mut self, peer_id: PeerId) {
        self.known_peers.push(peer_id);
    }

    pub fn get_broadcasted_messages(&self) -> Vec<MessageBody> {
        self.broadcasted_messages.lock().unwrap().clone()
    }
}

impl NetworkInterface for LocalNetwork {
    fn send_peer_message(&self, message: Message) {
        if let Some(sender) = self.senders.get(&message.to) {
            sender.try_send(message).unwrap();
        } else {
            println!(
                "Warning: Attempted to send message to unknown peer {:?}",
                message.to
            );
        }
    }

    fn broadcast_peer_message(&self, message_body: &MessageBody, from: PeerId) {
        self.broadcasted_messages.lock().unwrap().push(message_body.clone());
        for to in self.senders.keys() {
            if !to.eq(&from) {
                self.send_peer_message(Message {
                    from,
                    to: *to,
                    body: message_body.clone(),
                })
            }
        }
    }

    fn receive_client_message(&self, body: MessageBody) -> Result<(), String> {
        let to = self
            .senders
            .keys()
            .choose(&mut rand::rng())
            .ok_or_else(|| "Warning: No peers to send message to".to_string())?;
        self.send_peer_message(Message {
            from: 0.into(),
            to: *to,
            body,
        });
        Ok(())
    }

    fn on_message_received(&self, message: Message) -> Result<(), String> {
        self.senders
            .get(&message.from)
            .unwrap()
            .try_send(message)
            .map_err(|e| e.to_string())
    }
    
    fn known_peers(&self) -> Vec<PeerId> {
        self.known_peers.clone()
    }

    async fn send_and_wait<T: DeserializeOwned>(&self, _peer_id: PeerId, _message_body: NetworkMessage) -> Result<T, String> {
        !unimplemented!()
    }

    async fn send_and_wait_for_all<T: DeserializeOwned>(&self, _message_body: NetworkMessage, _peers: &Vec<PeerId>) -> HashMap<PeerId, Result<T, String>> {
        !unimplemented!()
    }

    async fn wait_for_readiness(&self) {
    }
}
