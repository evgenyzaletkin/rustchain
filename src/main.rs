mod server;

use rustchain::network::Network;
use rustchain::Peer;
use std::sync::{mpsc, Arc};
use std::thread;

#[tokio::main]
async fn main() {
    let (sender1, receiver1) = mpsc::channel();
    let (sender2, receiver2) = mpsc::channel();
    let mut peer1 = Peer::new(1, receiver1);
    let mut peer2 = Peer::new(2, receiver2);
    peer1.connect_with_peer(peer2.id);
    peer2.connect_with_peer(peer1.id);
    let mut network = Network::default();
    network.add_peer(peer1.id, sender1);
    network.add_peer(peer2.id, sender2);

    let network = Arc::new(network);

    let network_clone1 = Arc::clone(&network);
    let network_clone2 = Arc::clone(&network);
    
    tokio::spawn(async move {
       server::run_server(network).await; 
    });

    let f1 = thread::spawn(move || {
        peer1.run(&network_clone1);
    });

    let f2 = thread::spawn(move || {
        peer2.run(&network_clone2);
    });

    f1.join().unwrap();
    f2.join().unwrap();
}
