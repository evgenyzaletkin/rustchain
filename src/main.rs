#[tokio::main]
async fn main() {
    // let (sender1, receiver1) = mpsc::channel();
    // let (sender2, receiver2) = mpsc::channel();
    // let mut network = LocalNetwork::default();
    // let peer_id_1 = PeerId::from(1);
    // let peer_id_2= PeerId::from(2);
    // network.add_peer(peer_id_1, sender1);
    // network.add_peer(peer_id_2, sender2);
    //
    // let network = Arc::new(network);
    //
    // let mut peer1 = Peer::new(1, receiver1, network.clone());
    // let mut peer2 = Peer::new(2, receiver2, network.clone());
    // peer1.connect_with_peer(peer2.id);
    // peer2.connect_with_peer(peer1.id);
    // let clone = network.clone();
    // tokio::spawn(async move {
    //    server::run_server::<LocalNetwork>(clone, 3000).await;
    // });
    //
    // let f1 = thread::spawn(move || {
    //     peer1.run();
    // });
    //
    // let f2 = thread::spawn(move || {
    //     peer2.run();
    // });
    //
    // f1.join().unwrap();
    // f2.join().unwrap();
}
