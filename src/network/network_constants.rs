pub use crate::config::{
    DEFAULT_BASE_PORT as BASE_PORT, DEFAULT_CHANNEL_SIZE, DEFAULT_LOCAL_HOST as LOCAL_HOST,
};
pub const REGISTER_PATH: &str = "/register";
pub const GET_PEERS_PATH: &str = "/peers";
pub const HANDLE_PEER_MESSAGE_PATH: &str = "/handle";
pub const LATEST_BLOCK_STATE_PATH: &str = "/block/state/latest";
pub const TRANSACTIONS_PATH: &str = "/transactions";
pub const TEST_TRANSACTIONS_PATH: &str = "/test/transactions";
