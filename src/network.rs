use crate::peer::{Message, MessageBody, PeerId};
use log::debug;
use rand::prelude::IteratorRandom;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time;

#[derive(Serialize, Deserialize, Clone)]
pub struct RegisterRequest {
    pub peer_id: PeerId,
    pub addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct PeersResponse {
    pub peers: Vec<PeerWithAddr>,
}

impl From<&HashMap<PeerId, SocketAddr>> for PeersResponse {
    fn from(peers: &HashMap<PeerId, SocketAddr>) -> Self {
        let peers: Vec<PeerWithAddr> = peers
            .into_iter()
            .map(|(peer_id, addr)| PeerWithAddr::new(*peer_id, addr.clone()))
            .collect();
        Self { peers }
    }
}

#[derive(Serialize, Deserialize)]
pub struct PeerWithAddr {
    peer_id: PeerId,
    addr: SocketAddr,
}

impl PeerWithAddr {
    pub fn new(peer_id: PeerId, addr: SocketAddr) -> Self {
        Self { peer_id, addr }
    }
}

pub trait NetworkInterface {
    fn send(&self, message: Message);
    fn broadcast(&self, message_body: &MessageBody, from: PeerId, recipients: &Vec<PeerId>);
    fn receive_client_message(&self, body: MessageBody) -> Result<(), String>;
    fn on_message_received(&self, message: Message) -> Result<(), String>;
}

#[derive(Default)]
pub struct LocalNetwork {
    senders: HashMap<PeerId, Sender<Message>>,
}

impl LocalNetwork {
    pub fn add_peer(&mut self, peer_id: PeerId, sender: Sender<Message>) {
        self.senders.insert(peer_id, sender);
    }
}

impl NetworkInterface for LocalNetwork {
    fn receive_client_message(&self, body: MessageBody) -> Result<(), String> {
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

    fn send(&self, message: Message) {
        if let Some(sender) = self.senders.get(&message.to) {
            sender.try_send(message).unwrap();
        } else {
            println!(
                "Warning: Attempted to send message to unknown peer {:?}",
                message.to
            );
        }
    }

    fn broadcast(&self, message_body: &MessageBody, from: PeerId, recipients: &Vec<PeerId>) {
        for to in recipients {
            self.send(Message {
                from,
                to: *to,
                body: message_body.clone(),
            })
        }
    }

    fn on_message_received(&self, message: Message) -> Result<(), String> {
        self.senders
            .get(&message.from)
            .unwrap()
            .try_send(message)
            .map_err(|e| e.to_string())
    }
}

pub struct RestNetwork {
    peer_id: PeerId,
    discovery_addr: SocketAddr,
    known_peers: RwLock<HashMap<PeerId, SocketAddr>>,
    client: Client,
    peer_sender: Sender<Message>,
}

impl RestNetwork {
    const TICK_DURATION: Duration = Duration::from_secs(10);

    pub fn new(peer_id: PeerId, peer_sender: Sender<Message>) -> Self {
        let discovery_port = std::env::var("DISCOVERY_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3000);
        let discovery_addr = SocketAddr::from(([127, 0, 0, 1], discovery_port));
        Self {
            peer_id,
            discovery_addr,
            known_peers: RwLock::new(HashMap::new()),
            client: Client::new(),
            peer_sender,
        }
    }

    pub async fn run(&self, addr: SocketAddr) {
        let register_req = RegisterRequest {
            peer_id: self.peer_id,
            addr,
        };
        Self::send_message(
            self.client.clone(),
            &register_req,
            &self.discovery_addr,
            "register",
        )
        .await;
        let mut network_check_interval = time::interval(Self::TICK_DURATION);
        loop {
            network_check_interval.tick().await;
            self.update_peers().await;
        }
    }

    async fn update_peers(&self) {
        let result = self
            .client
            .get(format!("http://{}/peers", self.discovery_addr))
            .send()
            .await
            .unwrap();
        let peers_response: PeersResponse = result.json().await.unwrap();
        let mut known_peers = self.known_peers.write().unwrap();
        known_peers.clear();
        for peer in peers_response.peers {
            known_peers.insert(peer.peer_id, peer.addr);
        }
    }

    async fn send_message<T: Serialize + Clone + Send + 'static>(
        client: Client,
        message: &T,
        to: &SocketAddr,
        path: &str,
    ) {
        debug!("Sending message to {:?}", to);
        let client = client.clone();
        let to = to.clone();
        let path = path.to_string();
        let message = message.clone();
        // task::block_in_place(move || {
        //     Handle::current().block_on(async move {
        //         println!("Sending message to {:?}", to);
        //         client
        //             .post(format!("http://{}/{}", to, path))
        //             .json(&message)
        //             .send()
        //             .await;
        //     })
        // });
        client
            .post(format!("http://{}/{}", to, path))
            .json(&message)
            .send()
            .await
            .unwrap();
    }
}

impl NetworkInterface for RestNetwork {
    fn send(&self, message: Message) {
        let known_peers = self.known_peers.read().unwrap();
        if let Some(addr) = known_peers.get(&message.to).cloned() {
            let client = self.client.clone();
            let socket_addr = addr.clone();
            tokio::spawn(async move {
                Self::send_message(client, &socket_addr, &socket_addr, "handle").await;
            });
        }
    }

    fn broadcast(&self, message_body: &MessageBody, from: PeerId, recipients: &Vec<PeerId>) {
        debug!("Broadcasting message {message_body}");
        for (key, val) in self.known_peers.read().unwrap().iter() {
            if !key.eq(&from) {
                let m = Message {
                    from,
                    to: *key,
                    body: message_body.clone(),
                };
                let client = self.client.clone();
                let socket_addr = val.clone();
                tokio::spawn(async move {
                    Self::send_message(client, &m, &socket_addr, "handle").await;
                });
            }
        }
    }

    fn receive_client_message(&self, body: MessageBody) -> Result<(), String> {
        self.on_message_received(Message {
            from: 0.into(),
            to: self.peer_id,
            body,
        })
    }

    fn on_message_received(&self, message: Message) -> Result<(), String> {
        self.peer_sender
            .try_send(message)
            .map_err(|e| e.to_string())
    }
}
