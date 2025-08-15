use crate::crypto::KeyManager;
use crate::network::{network_constants, NetworkInterface};
use crate::peer::{Message, MessageBody};
use crate::storage::{BlockFile, BlockStorageState, BlockStorageView};
use crate::transactions::{SignedTransaction, Transaction};
use axum::extract::{Path, State};
use axum::http::{HeaderMap};
use axum::routing::{get, post};
use axum::{Json, Router};
use log::info;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

struct ServerState<N: NetworkInterface> {
    network: Arc<N>,
    latest_storage_view: BlockStorageView,
}

type SharedState<N> = Arc<RwLock<ServerState<N>>>;

pub async fn run_server<N: NetworkInterface>(
    network: Arc<N>,
    block_storage_view: BlockStorageView,
    addr: SocketAddr,
) {
    let server_state = Arc::new(RwLock::new(ServerState {
        network,
        latest_storage_view: block_storage_view,
    }));
    let app: Router<()> = Router::new()
        .route(network_constants::TRANSACTIONS_PATH, post(handle_client_transaction))
        .route(network_constants::TEST_TRANSACTIONS_PATH, post(handle_test_transaction))
        .route(network_constants::HANDLE_PEER_MESSAGE_PATH, post(hande_peer_message))
        .route(network_constants::LATEST_BLOCK_STATE_PATH, get(get_latest_storage_state))
        .route("/block/state/{block_index}", get(get_latest_storage_state))
        .route("/block/{block_index}", get(get_block))
        .with_state(server_state);

    let socket_listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(socket_listener, app).await.unwrap();
}
async fn handle_client_transaction<N: NetworkInterface>(
    State(state): State<SharedState<N>>,
    Json(transaction): Json<SignedTransaction>,
) -> String {
    match state
        .read()
        .await
        .network
        .receive_client_message(MessageBody::ClientTransaction(transaction))
    {
        Ok(_) => String::from("Transaction processed"),
        Err(e) => e.to_string(),
    }
}

const TEST_CLIENT_DIR: &str = "data/test_clients/";

async fn handle_test_transaction<N: NetworkInterface>(
    State(shared_state): State<SharedState<N>>,
    header_map: HeaderMap,
    Json(transaction): Json<Transaction>,
) -> String {
    let state = shared_state.read().await;
    let client_id = match header_map.get("client_id") {
        Some(header_value) => match header_value.to_str() {
            Ok(id) => id,
            Err(_) => return String::from("Invalid client_id header value"),
        },
        None => return String::from("client_id is not specified but required"),
    };

    let key = KeyManager::get_or_create_key(&PathBuf::from(TEST_CLIENT_DIR).join(client_id));
    let client_transaction = SignedTransaction::new(transaction, &key);
    match state
        .network
        .receive_client_message(MessageBody::ClientTransaction(client_transaction))
    {
        Ok(_) => String::from("Transaction processed"),
        Err(e) => e.to_string(),
    }
}

async fn hande_peer_message<N: NetworkInterface>(
    State(state): State<SharedState<N>>,
    Json(message): Json<Message>,
) {
    state
        .read()
        .await
        .network
        .on_message_received(message)
        .expect("Failed to process message");
}

async fn get_latest_storage_state<N: NetworkInterface>(
    State(state): State<SharedState<N>>,
) -> Json<BlockStorageState> {
    let latest_state = state.read().await.latest_storage_view.get_latest_state();
    info!("received storage state request, responding with: {latest_state:?}");
    Json(latest_state)
}

async fn get_block<N: NetworkInterface>(
    State(state): State<SharedState<N>>,
    Path(block_index): Path<u32>,
) -> Json<BlockFile> {
    Json(
        state
            .read()
            .await
            .latest_storage_view
            .get_block(block_index),
    )
}
