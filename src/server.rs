use crate::crypto::KeyManager;
use crate::network::NetworkInterface;
use crate::peer::{Message, MessageBody};
use crate::transactions::{SignedTransaction, Transaction};
use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

type Network = dyn NetworkInterface + Send + Sync + 'static;


pub async fn run_server<N: NetworkInterface>(network: Arc<Network>, addr: SocketAddr) {
    let app: Router<()> = Router::new()
        .route("/transactions", post(handle_client_transaction))
        .route("/test/transactions", post(handle_test_transaction))
        .route("/handle", post(hande_peer_message))
        .route("/", get(handle_get))
        .with_state(network);

    let socket_listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(socket_listener, app).await.unwrap();
}
async fn handle_client_transaction (
    State(network): State<Arc<Network>>,
    Json(transaction): Json<SignedTransaction>,
) -> String {
    match network.receive_client_message(MessageBody::ClientTransaction(transaction)) {
        Ok(_) => String::from("Transaction processed"),
        Err(e) => e.to_string(),
    }
}

async fn handle_get(State(network): State<Arc<Network>>) -> String {
    String::from("Hello, World!")
}

const TEST_CLIENT_DIR: &str = "data/test_clients/";

async fn handle_test_transaction (
    State(network): State<Arc<Network>>,
    header_map: HeaderMap,
    Json(transaction): Json<Transaction>,
) -> String {
    let client_id = match header_map.get("client_id") {
        Some(header_value) => match header_value.to_str() {
            Ok(id) => id,
            Err(_) => return String::from("Invalid client_id header value"),
        },
        None => return String::from("client_id is not specified but required"),
    };

    let key = KeyManager::get_or_create_key(&PathBuf::from(TEST_CLIENT_DIR).join(client_id));
    let client_transaction = SignedTransaction::new(transaction, &key);
    match network.receive_client_message(MessageBody::ClientTransaction(client_transaction)) {
        Ok(_) => String::from("Transaction processed"),
        Err(e) => e.to_string(),
    }
}

async fn hande_peer_message(
    State(network): State<Arc<Network>>,
    Json(message): Json<Message>) {
    network.on_message_received(message).expect("Failed to process message");
}