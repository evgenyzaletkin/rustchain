use crate::network::discovery_client::{DiscoveryClient, HttpDiscoveryClient};
use crate::network::{NetworkInterface, NetworkMessage};
use crate::peer::{Message, MessageBody, PeerId};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Method};
use log::{debug, trace, warn};
use reqwest::{Client, Error, Response};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::mpsc::Sender;
use tokio::time;

pub const TICK_DURATION: Duration = Duration::from_secs(5);
// Time after which inactive peers are removed
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const NETWORK_CHECK_TRIES: usize = 5;

pub struct RestNetwork<D = HttpDiscoveryClient> {
    peer_id: PeerId,
    discovery_client: D,
    known_peers: RwLock<HashMap<PeerId, SocketAddr>>,
    client: Client,
    peer_sender: Sender<Message>,
    peers_ready: Notify,
}

impl RestNetwork<HttpDiscoveryClient> {
    pub fn new(peer_id: PeerId, peer_sender: Sender<Message>) -> Self {
        Self::with_discovery_client(peer_id, peer_sender, HttpDiscoveryClient::from_env())
    }
}

impl<D: DiscoveryClient> RestNetwork<D> {
    pub fn with_discovery_client(
        peer_id: PeerId,
        peer_sender: Sender<Message>,
        discovery_client: D,
    ) -> Self {
        Self::with_discovery_client_and_http_client(
            peer_id,
            peer_sender,
            discovery_client,
            Client::new(),
        )
    }

    fn with_discovery_client_and_http_client(
        peer_id: PeerId,
        peer_sender: Sender<Message>,
        discovery_client: D,
        client: Client,
    ) -> Self {
        Self {
            peer_id,
            discovery_client,
            known_peers: RwLock::new(HashMap::new()),
            client,
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
                self.discovery_client.register(self.peer_id, addr).await?;
                self.update_peers().await
            }
            .await
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

    async fn update_peers(&self) -> Result<(), String> {
        let peers = self.discovery_client.peers().await?;
        let mut known_peers = self.known_peers.write().unwrap();
        known_peers.clear();
        for peer in peers {
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

impl<D: DiscoveryClient> NetworkInterface for RestNetwork<D> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::PeerWithAddr;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    struct FakeDiscoveryClient {
        peers: Mutex<Vec<PeerWithAddr>>,
    }

    impl FakeDiscoveryClient {
        fn new(peers: Vec<PeerWithAddr>) -> Self {
            Self {
                peers: Mutex::new(peers),
            }
        }
    }

    impl DiscoveryClient for FakeDiscoveryClient {
        async fn register(&self, _peer_id: PeerId, _addr: SocketAddr) -> Result<(), String> {
            Ok(())
        }

        async fn peers(&self) -> Result<Vec<PeerWithAddr>, String> {
            Ok(self.peers.lock().unwrap().clone())
        }
    }

    #[tokio::test]
    async fn update_peers_uses_discovery_client_and_filters_self() {
        let (sender, _receiver) = mpsc::channel(1);
        let client = Client::builder().no_proxy().build().unwrap();
        let network = RestNetwork::with_discovery_client_and_http_client(
            PeerId::from(1),
            sender,
            FakeDiscoveryClient::new(vec![
                PeerWithAddr::new(PeerId::from(1), "127.0.0.1:3001".parse().unwrap()),
                PeerWithAddr::new(PeerId::from(2), "127.0.0.1:3002".parse().unwrap()),
            ]),
            client,
        );

        network.update_peers().await.unwrap();

        assert_eq!(network.known_peers(), vec![PeerId::from(2)]);
    }
}
