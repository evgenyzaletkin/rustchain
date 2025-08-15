use log::info;
use rustchain::logging::init_logging;
use rustchain::network::network_constants;
use rustchain::network::rest_network::RestNetwork;
use rustchain::peer::{Peer, PeerId};
use rustchain::{peer, server};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;

const PEER_ID_ENV_VAR: &str = "PEER_ID";

struct PeerConfig {
    peer_id: PeerId,
    listening_addr: SocketAddr,
    channel_size: usize,
}

impl PeerConfig {
    fn from_env() -> Result<PeerConfig, String> {
        let peer_id: PeerId = std::env::var(PEER_ID_ENV_VAR)
            .map_err(|e| e.to_string())?
            .parse()
            .map_err(|_| "Peer Id must be a number")?;

        let port_offset: u16 = peer_id.try_into()?;
        let listening_addr = SocketAddr::from((
            network_constants::LOCAL_HOST,
            network_constants::BASE_PORT + port_offset,
        ));

        Ok(Self {
            peer_id,
            listening_addr,
            channel_size: peer::DEFAULT_CHANNEL_SIZE,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    init_logging();
    let peer_config = PeerConfig::from_env()?;
    let (sender, receiver) = mpsc::channel(peer_config.channel_size);
    let network = Arc::new(RestNetwork::new(peer_config.peer_id, sender));
    let mut peer = Peer::new(peer_config.peer_id, receiver, network.clone());

    let network_for_server = network.clone();
    info!(
        "Starting peer {} on {}",
        peer_config.peer_id, peer_config.listening_addr
    );
    let view = peer.create_block_storage_view();
    tokio::spawn(async move {
        server::run_server::<RestNetwork>(
            network_for_server,
            view,
            peer_config.listening_addr.clone(),
        )
        .await;
    });
    tokio::spawn(async move {
        let res = network.run(peer_config.listening_addr).await;
        info!("Network stopped with result {:?}", res);
    });
    peer.run().await
}
