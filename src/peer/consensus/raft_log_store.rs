use crate::peer::consensus::RaftLogEntry;
use std::fs;
use std::path::{Path, PathBuf};

const RAFT_LOG_FILENAME: &str = "raft_log.json";

pub(crate) trait RaftLogStorage {
    fn load(&self) -> Result<Vec<RaftLogEntry>, String>;
    fn save(&mut self, log: &[RaftLogEntry]) -> Result<(), String>;
}

pub(crate) struct FileRaftLogStore {
    path: PathBuf,
}

impl FileRaftLogStore {
    pub(crate) fn new(peer_dir: &Path) -> Self {
        Self {
            path: peer_dir.join(RAFT_LOG_FILENAME),
        }
    }
}

impl RaftLogStorage for FileRaftLogStore {
    fn load(&self) -> Result<Vec<RaftLogEntry>, String> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let contents = fs::read_to_string(&self.path)
            .map_err(|e| format!("Failed to read Raft log {}: {}", self.path.display(), e))?;
        serde_json::from_str(&contents).map_err(|e| {
            format!(
                "Failed to deserialize Raft log {}: {}",
                self.path.display(),
                e
            )
        })
    }

    fn save(&mut self, log: &[RaftLogEntry]) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create Raft log directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
        }
        let contents = serde_json::to_string_pretty(log).expect("Failed to serialize Raft log");
        fs::write(&self.path, contents)
            .map_err(|e| format!("Failed to write Raft log {}: {}", self.path.display(), e))
    }
}

pub(crate) struct InMemoryRaftLogStore {
    log: Vec<RaftLogEntry>,
}

impl InMemoryRaftLogStore {
    pub(crate) fn new() -> Self {
        Self { log: Vec::new() }
    }
}

impl RaftLogStorage for InMemoryRaftLogStore {
    fn load(&self) -> Result<Vec<RaftLogEntry>, String> {
        Ok(self.log.clone())
    }

    fn save(&mut self, log: &[RaftLogEntry]) -> Result<(), String> {
        self.log = log.to_vec();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{FileRaftLogStore, InMemoryRaftLogStore, RaftLogStorage};
    use crate::peer::consensus::RaftLogEntry;
    use crate::storage::BlockHash;
    use std::fs;

    #[test]
    fn saves_and_loads_raft_log_entries() {
        let dir =
            std::env::temp_dir().join(format!("rustchain_raft_log_store_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let mut store = FileRaftLogStore::new(&dir);
        let log = vec![RaftLogEntry {
            term: 2,
            index: 1,
            block_hash: BlockHash::new([7; 32]),
        }];

        store.save(&log).unwrap();

        assert_eq!(store.load().unwrap(), log);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn in_memory_store_saves_and_loads_raft_log_entries() {
        let mut store = InMemoryRaftLogStore::new();
        let log = vec![RaftLogEntry {
            term: 3,
            index: 2,
            block_hash: BlockHash::new([8; 32]),
        }];

        store.save(&log).unwrap();

        assert_eq!(store.load().unwrap(), log);
    }
}
