use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use rustchain::MessageBody;
use rustchain::crypto::KeyManager;
use rustchain::network::Network;
use rustchain::transactions::{SignedTransaction, Transaction};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn run_server(network: Arc<Network>) {
    let app: Router<()> = Router::new()
        .route("/transactions", post(handle_client_transaction))
        .route("/test/transactions", post(handle_test_transaction))
        .route("/", get(handle_get))
        .with_state(network);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let socket_listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(socket_listener, app).await.unwrap();
}
async fn handle_client_transaction(
    State(network): State<Arc<Network>>,
    Json(transaction): Json<SignedTransaction>,
) -> String {
    match network.send_client_message(MessageBody::ClientTransaction(transaction)) {
        Ok(_) => String::from("Transaction processed"),
        Err(e) => e.to_string(),
    }
}

async fn handle_get(State(network): State<Arc<Network>>) -> String {
    String::from("Hello, World!")
}

const TEST_CLIENT_DIR: &str = "data/test_clients/";

async fn handle_test_transaction(
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
    match network.send_client_message(MessageBody::ClientTransaction(client_transaction)) {
        Ok(_) => String::from("Transaction processed"),
        Err(e) => e.to_string(),
    }
}
