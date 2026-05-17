use crate::network::{NetworkMessage, PeerWithAddr, PeersResponse, RegisterRequest};
use crate::peer::PeerId;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Method};
use log::trace;
use reqwest::Client;
use std::net::SocketAddr;
use std::time::Duration;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const DISCOVERY_ENV: &str = "DISCOVERY_PORT";

#[allow(async_fn_in_trait)]
pub trait DiscoveryClient: Send + Sync + 'static {
    async fn register(&self, peer_id: PeerId, addr: SocketAddr) -> Result<(), String>;
    async fn peers(&self) -> Result<Vec<PeerWithAddr>, String>;
}

pub struct HttpDiscoveryClient {
    discovery_addr: SocketAddr,
    client: Client,
}

impl HttpDiscoveryClient {
    pub fn from_env() -> Self {
        let discovery_port = std::env::var(DISCOVERY_ENV)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(super::network_constants::BASE_PORT);
        let discovery_addr =
            SocketAddr::from((super::network_constants::LOCAL_HOST, discovery_port));

        Self::new(discovery_addr)
    }

    pub fn new(discovery_addr: SocketAddr) -> Self {
        Self {
            discovery_addr,
            client: Client::new(),
        }
    }

    async fn send_message(&self, message: &NetworkMessage) -> Result<reqwest::Response, String> {
        let path = message.path();
        let method = message.method();
        trace!(
            "Sending discovery message to {}{}",
            self.discovery_addr, path
        );

        let mut builder = self
            .client
            .request(method, format!("http://{}{}", self.discovery_addr, path))
            .timeout(REQUEST_TIMEOUT);
        if message.method() != Method::GET {
            builder = builder
                .body(message.body())
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }

        builder.send().await.map_err(|e| e.to_string())
    }
}

impl DiscoveryClient for HttpDiscoveryClient {
    async fn register(&self, peer_id: PeerId, addr: SocketAddr) -> Result<(), String> {
        self.send_message(&NetworkMessage::Register(RegisterRequest { peer_id, addr }))
            .await
            .map(|_| ())
    }

    async fn peers(&self) -> Result<Vec<PeerWithAddr>, String> {
        let result = self.send_message(&NetworkMessage::GetPeers).await?;
        trace!("Got peers response: {result:?}");
        let peers_response: PeersResponse = result.json().await.map_err(|e| e.to_string())?;
        Ok(peers_response.peers)
    }
}
