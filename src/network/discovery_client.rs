use crate::config::{DISCOVERY_HOST_ENV_VAR, DISCOVERY_PORT_ENV_VAR};
use crate::network::{NetworkMessage, PeerWithAddr, PeersResponse, RegisterRequest};
use crate::peer::PeerId;
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderValue, Method};
use log::trace;
use reqwest::Client;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
#[allow(async_fn_in_trait)]
pub trait DiscoveryClient: Send + Sync + 'static {
    async fn register(&self, peer_id: PeerId, addr: SocketAddr) -> Result<(), String>;
    async fn peers(&self) -> Result<Vec<PeerWithAddr>, String>;
}

pub struct HttpDiscoveryClient {
    discovery_base_url: String,
    client: Client,
}

impl HttpDiscoveryClient {
    pub fn from_env() -> Self {
        let discovery_host = std::env::var(DISCOVERY_HOST_ENV_VAR)
            .unwrap_or_else(|_| IpAddr::from(super::network_constants::LOCAL_HOST).to_string());
        let discovery_port = std::env::var(DISCOVERY_PORT_ENV_VAR)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(super::network_constants::BASE_PORT);
        Self {
            discovery_base_url: format!("http://{discovery_host}:{discovery_port}"),
            client: Client::new(),
        }
    }

    pub fn new(discovery_addr: SocketAddr) -> Self {
        Self {
            discovery_base_url: format!("http://{discovery_addr}"),
            client: Client::new(),
        }
    }

    async fn send_message(&self, message: &NetworkMessage) -> Result<reqwest::Response, String> {
        let path = message.path();
        let method = message.method();
        trace!(
            "Sending discovery message to {}{}",
            self.discovery_base_url, path
        );

        let mut builder = self
            .client
            .request(method, format!("{}{path}", self.discovery_base_url))
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
