use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use log::info;
use rustchain::logging::init_logging;
use rustchain::network;
use rustchain::network::{PeersResponse, RegisterRequest};
use rustchain::peer::PeerId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpListener;
use tokio::sync::RwLock;

#[derive(Default)]
struct AppState {
    pub known_peers: HashMap<PeerId, (SocketAddr, Instant)>, // Added timestamp
}

type SharedState = Arc<RwLock<AppState>>;

#[tokio::main]
async fn main() {
    init_logging();

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
    info!("Starting discovery server on {}", addr);
    axum::serve(
        socket_listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}

async fn register_peer(
    State(shared_state): State<SharedState>,
    Json(peer_request): Json<RegisterRequest>,
) -> String {
    info!("Receiving probe message from peer: {}", peer_request.peer_id);
    let mut state = shared_state.write().await;
    state
        .known_peers
        .insert(peer_request.peer_id, (peer_request.addr, Instant::now()));
    "Ok".to_string()
}

async fn get_peers(State(shared_state): State<SharedState>) -> String {
    let mut state = shared_state.write().await;
    
    state
        .known_peers
        .retain(|_, (_, last_seen)| last_seen.elapsed() < network::KEEP_ALIVE);

    let peers: HashMap<PeerId, SocketAddr> = state
        .known_peers
        .iter()
        .map(|(id, (addr, _))| (*id, *addr))
        .collect();

    serde_json::to_string(&PeersResponse::from(&peers)).unwrap()
}
