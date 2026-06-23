use rustchain::logging::init_logging;
use rustchain::peer_runtime::{PeerConfig, run_peer};

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), String> {
    init_logging();
    run_peer(PeerConfig::from_env()?).await
}
