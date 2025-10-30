use crate::peer::{Message, MessageBody, PeerId};
use derive_more::Display;
use reqwest::{Body, Method};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;

pub mod local_network;
pub mod rest_network;
pub mod network_constants;

#[derive(Serialize, Deserialize, Clone)]
pub struct RegisterRequest {
    pub peer_id: PeerId,
    pub addr: SocketAddr,
}

#[derive(Serialize, Deserialize)]
pub struct PeersResponse {
    pub peers: Vec<PeerWithAddr>,
}

#[derive(Display, Clone, Serialize, Deserialize)]
pub enum NetworkMessage {
    GetLatestBlockState,
    #[display("GetBlockState")]
    GetBlockState(u32),
    #[display("GetBlocks")]
    GetBlock(u32),
    PeerMessage(Message),
    #[display("Register")]
    Register(RegisterRequest),
    GetPeers,
}

impl NetworkMessage {
    fn path(&self) -> String {
        match self {
            NetworkMessage::GetLatestBlockState => network_constants::LATEST_BLOCK_STATE_PATH.to_string(),
            NetworkMessage::GetBlockState(idx) => format!("/block/state/{}", idx),
            NetworkMessage::GetBlock(idx) => format!("/block/{}", idx),
            NetworkMessage::PeerMessage(_) => network_constants::HANDLE_PEER_MESSAGE_PATH.to_string(),
            NetworkMessage::Register(_) => network_constants::REGISTER_PATH.to_string(),
            NetworkMessage::GetPeers => network_constants::GET_PEERS_PATH.to_string(),
        }
    }

    fn method(&self) -> Method {
        match self {
            NetworkMessage::GetLatestBlockState => Method::GET,
            NetworkMessage::GetBlockState(_) => Method::GET,
            NetworkMessage::GetBlock(_) => Method::GET,
            NetworkMessage::PeerMessage(_) => Method::POST,
            NetworkMessage::Register(_) => Method::POST,
            NetworkMessage::GetPeers => Method::GET,
        }
    }

    fn body(&self) -> Body {
        match self {
            NetworkMessage::PeerMessage(message) => {
                Body::from(serde_json::to_string(&message).unwrap())
            }
            NetworkMessage::Register(req) => Body::from(serde_json::to_string(req).unwrap()),
            NetworkMessage::GetPeers => Body::default(),
            _ => match self.method() {
                Method::GET => Body::default(),
                _ => Body::from(serde_json::to_string(&self).unwrap()),
            },
        }
    }
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

pub trait NetworkInterface: Send + Sync + 'static {
    fn send_peer_message(&self, message: Message);
    fn broadcast_peer_message(&self, message_body: &MessageBody, from: PeerId);
    fn receive_client_message(&self, body: MessageBody) -> Result<(), String>;
    fn on_message_received(&self, message: Message) -> Result<(), String>;
    fn known_peers(&self) -> Vec<PeerId>;
    async fn send_and_wait<T: DeserializeOwned>(
        &self,
        _peer_id: PeerId,
        _message_body: NetworkMessage,
    ) -> Result<T, String>;
    async fn send_and_wait_for_all<T: DeserializeOwned>(
        &self,
        _message_body: NetworkMessage,
        _peers: &Vec<PeerId>,   
    ) -> HashMap<PeerId, Result<T, String>>;

    async fn wait_for_readiness(&self);
}
