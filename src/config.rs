use std::time::Duration;

pub const PEER_ID_ENV_VAR: &str = "PEER_ID";
pub const CONSENSUS_MODE_ENV_VAR: &str = "CONSENSUS_MODE";
pub const PEER_BIND_HOST_ENV_VAR: &str = "PEER_BIND_HOST";
pub const PEER_ADVERTISE_HOST_ENV_VAR: &str = "PEER_ADVERTISE_HOST";
pub const DISCOVERY_HOST_ENV_VAR: &str = "DISCOVERY_HOST";
pub const DISCOVERY_BIND_HOST_ENV_VAR: &str = "DISCOVERY_BIND_HOST";
pub const DISCOVERY_PORT_ENV_VAR: &str = "DISCOVERY_PORT";

pub const DEFAULT_CHANNEL_SIZE: usize = 1000;
pub const DEFAULT_PATH_TO_BLOCKS: &str = "data";
pub const DEFAULT_MEMPOOL_SIZE: usize = 5;

pub const DEFAULT_BASE_PORT: u16 = 3000;
pub const DEFAULT_LOCAL_HOST: [u8; 4] = [127, 0, 0, 1];

pub const DEFAULT_SYNC_INTERVAL: Duration = Duration::from_secs(20);
pub const DEFAULT_CONSENSUS_TICK_INTERVAL: Duration = Duration::from_millis(500);

pub const DEFAULT_RAFT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_RAFT_ELECTION_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_RAFT_ELECTION_TIMEOUT_JITTER: Duration = Duration::from_secs(5);
