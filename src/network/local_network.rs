use crate::network::{NetworkInterface, NetworkMessage};
use crate::peer::{Message, MessageBody, PeerId};
use crate::storage::{BlockStorageState, BlockStorageView};
use rand::prelude::IteratorRandom;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;

#[derive(Default)]
pub struct LocalNetwork {
    senders: HashMap<PeerId, Sender<Message>>,
    known_peers: Vec<PeerId>,
    block_storage_views: HashMap<PeerId, BlockStorageView>,
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

    pub fn add_block_storage_view(&mut self, peer_id: PeerId, view: BlockStorageView) {
        self.known_peers.push(peer_id);
        self.block_storage_views.insert(peer_id, view);
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
        self.broadcasted_messages
            .lock()
            .unwrap()
            .push(message_body.clone());
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

    async fn send_and_wait<T: DeserializeOwned>(
        &self,
        peer_id: PeerId,
        message_body: NetworkMessage,
    ) -> Result<T, String> {
        let view = self
            .block_storage_views
            .get(&peer_id)
            .ok_or_else(|| "Peer storage view not found".to_string())?;
        match message_body {
            NetworkMessage::GetLatestBlockState => deserialize_response(view.get_latest_state()),
            NetworkMessage::GetBlockState(idx) => deserialize_response(
                view.get_block(idx)
                    .map(|block| BlockStorageState::from(&block))?,
            ),
            NetworkMessage::GetBlock(idx) => deserialize_response(view.get_block(idx)?),
            _ => Err("Unsupported local network request".to_string()),
        }
    }

    async fn send_and_wait_for_all<T: DeserializeOwned>(
        &self,
        message_body: NetworkMessage,
        peers: &Vec<PeerId>,
    ) -> HashMap<PeerId, Result<T, String>> {
        let mut results = HashMap::new();
        for peer_id in peers {
            results.insert(
                *peer_id,
                self.send_and_wait(*peer_id, message_body.clone()).await,
            );
        }
        results
    }

    async fn wait_for_readiness(&self) {}
}

fn deserialize_response<T, S>(response: S) -> Result<T, String>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let value = serde_json::to_value(response).map_err(|e| e.to_string())?;
    serde_json::from_value(value).map_err(|e| e.to_string())
}
