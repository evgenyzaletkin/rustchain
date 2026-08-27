use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use log::info;
use rustchain::config::{DISCOVERY_BIND_HOST_ENV_VAR, DISCOVERY_PORT_ENV_VAR};
use rustchain::logging::init_logging;
use rustchain::network::{PeersResponse, RegisterRequest, network_constants};
use rustchain::peer::PeerId;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

const KEEP_ALIVE: Duration = Duration::from_secs(20);
#[derive(Default)]
struct AppState {
    known_peers: HashMap<PeerId, (SocketAddr, Instant)>,
}

type SharedState = Arc<RwLock<AppState>>;

struct DiscoveryConfig {
    listening_addr: SocketAddr,
}
impl DiscoveryConfig {
    fn from_env() -> Result<DiscoveryConfig, String> {
        let port = std::env::var(DISCOVERY_PORT_ENV_VAR)
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(network_constants::BASE_PORT);
        let bind_host = std::env::var(DISCOVERY_BIND_HOST_ENV_VAR)
            .unwrap_or_else(|_| IpAddr::from(network_constants::LOCAL_HOST).to_string());
        let bind_ip: IpAddr = bind_host
            .parse()
            .map_err(|_| format!("{DISCOVERY_BIND_HOST_ENV_VAR} must be an IP address"))?;
        let listening_addr = SocketAddr::from((bind_ip, port));

        Ok(Self { listening_addr })
    }
}

#[tokio::main]
async fn main() -> Result<(), String> {
    init_logging();
    let config = DiscoveryConfig::from_env()?;
    let shared_state = SharedState::default();
    let app = Router::new()
        .route(network_constants::REGISTER_PATH, post(register_peer))
        .route(network_constants::GET_PEERS_PATH, get(get_peers))
        .with_state(shared_state);
    let socket_listener = TcpListener::bind(&config.listening_addr).await.unwrap();
    info!("Starting discovery server on {}", config.listening_addr);
    axum::serve(
        socket_listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .map_err(|e| e.to_string())
}

async fn register_peer(
    State(shared_state): State<SharedState>,
    Json(peer_request): Json<RegisterRequest>,
) -> String {
    info!(
        "Receiving probe message from peer: {}",
        peer_request.peer_id
    );
    let mut state = shared_state.write().await;
    state
        .known_peers
        .insert(peer_request.peer_id, (peer_request.addr, Instant::now()));
    "Ok".to_string()
}

async fn get_peers(State(shared_state): State<SharedState>) -> String {
    let read_state = shared_state.read().await;
    let filtered: HashMap<_, _> = read_state
        .known_peers
        .iter()
        .filter(|(_, addr_and_time)| addr_and_time.1.elapsed() < KEEP_ALIVE) // your filter condition here
        .map(|(k, v)| (k.clone(), *v))
        .collect();

    if filtered.len() != read_state.known_peers.len() {
        drop(read_state);
        shared_state.write().await.known_peers = filtered.clone();
    }

    let peers: HashMap<PeerId, SocketAddr> = filtered
        .iter()
        .map(|(id, (addr, _))| (*id, *addr))
        .collect();

    serde_json::to_string(&PeersResponse::from(&peers)).unwrap()
}
