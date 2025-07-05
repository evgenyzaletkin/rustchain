use rustchain::network::RestNetwork;
use rustchain::peer::{Peer, PeerId};
use rustchain::server;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use tokio::sync::mpsc;
use env_logger::Env;
use log::info;
use rustchain::logging::init_logging;

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
    let mut peer1 = Peer::new(peer_id_u16 as u32, receiver1, network.clone());
    let listening_adder = SocketAddr::from(([127, 0, 0, 1], 3000 + peer_id_u16));
    let clone = network.clone();
    info!("Starting peer {} on {}", peer_id_u16, listening_adder);
    tokio::spawn(async move {
        server::run_server::<RestNetwork>(clone, listening_adder.clone()).await;
    });
    tokio::spawn(async move {
        network.run(listening_adder).await;
    });
    peer1.run().await;
}
