use crate::network::{network_constants, NetworkInterface, NetworkMessage, PeersResponse, RegisterRequest};
use crate::peer::{Message, MessageBody, PeerId};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Method};
use log::{debug, error, trace, warn};
use reqwest::{Client, Error, Response};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::mpsc::Sender;
use tokio::time;
use crate::network;

pub const TICK_DURATION: Duration = Duration::from_secs(5);
// Time after which inactive peers are removed
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const NETWORK_CHECK_TRIES: usize = 5;

pub struct RestNetwork {
    peer_id: PeerId,
    discovery_addr: SocketAddr,
    known_peers: RwLock<HashMap<PeerId, SocketAddr>>,
    client: Client,
    peer_sender: Sender<Message>,
    peers_ready: Notify,
}

impl RestNetwork {
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
            peers_ready: Notify::new(),
        }
    }

    pub async fn run(&self, addr: SocketAddr) -> Result<(), String> {
        let mut network_check_interval = time::interval(TICK_DURATION);
        let mut failed_checks = 0;
        let mut notify_waiters = true;
        loop {
            network_check_interval.tick().await;
            if failed_checks >= NETWORK_CHECK_TRIES {
                return Err(
                    "Failed to register with discovery server too many times. Exiting.".to_string(),
                );
            }
            trace!("Registering with discovery server: try {failed_checks}");

            match async {
                self.register(addr).await?;
                self.update_peers().await
            }.await
            {
                Ok(_) => {
                    if notify_waiters {
                        self.peers_ready.notify_waiters();
                        notify_waiters = false;
                    }
                    failed_checks = 0;
                }
                Err(e) => {
                    warn!("Failed to register/update peers: {e}");
                    failed_checks += 1;
                }
            }
        }
    }

    async fn register(&self, addr: SocketAddr) -> Result<Response, Error> {
        Self::send_message(
            self.client.clone(),
            &NetworkMessage::Register(RegisterRequest {
                peer_id: self.peer_id,
                addr,
            }),
            &self.discovery_addr,
        )
        .await
    }

    async fn update_peers(&self) -> Result<(), Error> {
        let result = Self::send_message(
            self.client.clone(),
            &NetworkMessage::GetPeers,
            &self.discovery_addr,
        ).await?;
        trace!("Got peers response: {result:?}");
        let peers_response: PeersResponse = result.json().await?;
        let mut known_peers = self.known_peers.write().unwrap();
        known_peers.clear();
        for peer in peers_response.peers {
            if peer.peer_id != self.peer_id {
                known_peers.insert(peer.peer_id, peer.addr);
            }
        }
        Ok(())
    }

    async fn send_message(
        client: Client,
        message: &NetworkMessage,
        to: &SocketAddr,
    ) -> Result<Response, Error> {
        let to = to.clone();
        let path = message.path();
        let method = message.method();
        trace!("Sending message to {to}{path}");

        // let result = client
        //     .request(method, format!("http://{}/{}", to, path))
        //     .timeout(REQUEST_TIMEOUT)
        //     .json(&message)
        //     .send()
        //     .await;

        let mut builder = client
            .request(method, format!("http://{}{}", to, path))
            .timeout(REQUEST_TIMEOUT);
        if message.method() != Method::GET {
            builder = builder
                .body(message.body())
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        let result = builder.send().await;
        trace!("Got a result: {}", result.is_ok());
        result
    }

    async fn send_to_all<T: DeserializeOwned>(
        &self,
        client: Client,
        message: &NetworkMessage,
        peer_ids: &Vec<PeerId>,
    ) -> HashMap<PeerId, Result<T, String>> {
        let mut results = HashMap::new();
        let mut handles = HashMap::new();
        let known_peers = self.known_peers.read().unwrap();
        for peer_id in peer_ids.iter() {
            let Some(addr) = known_peers.get(peer_id) else {
                results.insert(*peer_id, Err("Peer not found".to_string()));
                break;
            };
            let client = client.clone();
            let addr = addr.clone();
            let message = message.clone();
            let handle =
                tokio::spawn(async move { Self::send_message(client, &message, &addr).await });
            handles.insert(*peer_id, handle);
        }

        for (peer_id, handle) in handles {
            let handle_result = handle.await;

            match handle_result {
                Ok(Ok(resp)) => {
                    let result = Self::deserialize_resp(resp).await;
                    results.insert(peer_id, result);
                }
                Ok(Err(e)) => {
                    results.insert(peer_id, Err(e.to_string()));
                }
                Err(e) => {
                    results.insert(peer_id, Err(e.to_string()));
                }
            }
        }
        results
    }

    async fn deserialize_resp<T: DeserializeOwned>(resp: Response) -> Result<T, String> {
        let response_body = resp.text().await.unwrap();
        match serde_json::from_str(&response_body) {
            Ok(parsed) => Ok(parsed),
            Err(e) => Err(e.to_string()),
        }
    }
}

impl NetworkInterface for RestNetwork {
    fn send_peer_message(&self, message: Message) {
        let known_peers = self.known_peers.read().unwrap();
        if let Some(addr) = known_peers.get(&message.to).cloned() {
            let client = self.client.clone();
            let socket_addr = addr.clone();
            tokio::spawn(async move {
                Self::send_message(client, &NetworkMessage::PeerMessage(message), &socket_addr)
                    .await
            });
        }
    }

    fn broadcast_peer_message(&self, message_body: &MessageBody, from: PeerId) {
        debug!("Broadcasting message {message_body}");
        for (peer_id, socket_addr) in self.known_peers.read().unwrap().clone() {
            if !peer_id.eq(&from) {
                let m = Message {
                    from,
                    to: peer_id,
                    body: message_body.clone(),
                };
                let client = self.client.clone();
                tokio::spawn(async move {
                    let _ =
                        Self::send_message(client, &NetworkMessage::PeerMessage(m), &socket_addr)
                            .await;
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

    fn known_peers(&self) -> Vec<PeerId> {
        self.known_peers.read().unwrap().keys().cloned().collect()
    }

    async fn send_and_wait<T: DeserializeOwned>(
        &self,
        peer_id: PeerId,
        message_body: NetworkMessage,
    ) -> Result<T, String> {
        let peer_addr = self.known_peers.read().unwrap().get(&peer_id).cloned();
        if let Some(peer_addr) = peer_addr {
            match Self::send_message(self.client.clone(), &message_body, &peer_addr).await {
                Ok(resp) => Self::deserialize_resp(resp).await,
                Err(e) => Err(e.to_string()),
            }
        } else {
            Err("Peer not found".to_string())
        }
    }

    async fn send_and_wait_for_all<T: DeserializeOwned>(
        &self,
        sync_message: NetworkMessage,
        peer_ids: &Vec<PeerId>,
    ) -> HashMap<PeerId, Result<T, String>> {
        debug!("Sending and waiting {sync_message} to {peer_ids:?}");
        self.send_to_all(self.client.clone(), &sync_message, peer_ids)
            .await
    }

    async fn wait_for_readiness(&self) {
        self.peers_ready.notified().await;
    }
}