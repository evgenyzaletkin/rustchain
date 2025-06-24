use axum::extract::{ConnectInfo, State};
use axum::routing::{get, post};
use axum::{Json, Router, ServiceExt};
use rustchain::PeerId;
use rustchain::network::{PeersResponse, RegisterRequest};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Default)]
struct AppState {
    pub known_peers: HashMap<PeerId, SocketAddr>,
}

type SharedState = Arc<RwLock<AppState>>;

#[tokio::main]
async fn main() {
    let port = std::env::var("DISCOVERY_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    let shared_state = SharedState::default();
    let app = Router::new()
        .route("/register", post(register_peer))
        .route("/peers", get(get_peers))
        .with_state(shared_state);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let socket_listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(
        socket_listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn register_peer(
    State(shared_state): State<SharedState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    Json(peer_request): Json<RegisterRequest>,
) -> String {
    let mut state = shared_state.write().await;
    state.known_peers.insert(peer_request.peer_id, peer_addr);
    "Ok".to_string()
}

async fn get_peers(State(shared_state): State<SharedState>) -> String {
    let state = shared_state.read().await;
    serde_json::to_string(&PeersResponse::from(&state.known_peers)).unwrap()
}
