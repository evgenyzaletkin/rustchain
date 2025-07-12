use log::info;
use rustchain::logging::init_logging;
use rustchain::peer::{Peer, PeerId};
use rustchain::server;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use rustchain::network::rest_network::RestNetwork;

#[tokio::main]
async fn main() {
    init_logging();
    let (sender1, receiver1) = mpsc::channel(1000);
    let peer_id_u16: u16 = std::env::var("PEER_ID")
        .ok()
        .and_then(|p| p.parse().ok())
        .expect("Peer Id must be a number");
    let peer_id = PeerId::from(peer_id_u16 as u32);
    let network = Arc::new(RestNetwork::new(peer_id, sender1));
    let mut peer = Peer::new(peer_id_u16 as u32, receiver1, network.clone());
    let listening_adder = SocketAddr::from(([127, 0, 0, 1], 3000 + peer_id_u16));
    let network_for_server = network.clone();
    info!("Starting peer {} on {}", peer_id_u16, listening_adder);
    let view = peer.create_block_storage_view();
    tokio::spawn(async move {
        server::run_server::<RestNetwork>(network_for_server, view, listening_adder.clone()).await;
    });
    tokio::spawn(async move {
        network.run(listening_adder).await;
    });
    peer.run().await;
}
