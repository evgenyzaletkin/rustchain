use crate::peer::consensus::RaftLogEntry;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

const RAFT_LOG_FILENAME: &str = "raft_log.json";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RaftLogState {
    pub(crate) log: Vec<RaftLogEntry>,
    pub(crate) commit_index: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct PersistedRaftLogState {
    log: Vec<RaftLogEntry>,
    commit_index: u64,
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

impl FileRaftLogStore {
    pub(crate) fn load(&self) -> Result<RaftLogState, String> {
        if !self.path.exists() {
            return Ok(RaftLogState {
                log: Vec::new(),
                commit_index: None,
            });
        }

        let contents = fs::read(&self.path)
            .map_err(|e| format!("Failed to read Raft log {}: {}", self.path.display(), e))?;
        let stored: PersistedRaftLogState = serde_json::from_slice(&contents).map_err(|e| {
            format!(
                "Failed to deserialize Raft log {}: {}",
                self.path.display(),
                e
            )
        })?;
        Ok(RaftLogState {
            log: stored.log,
            commit_index: Some(stored.commit_index),
        })
    }

    pub(crate) fn save(&mut self, log: &[RaftLogEntry], commit_index: u64) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                format!(
                    "Failed to create Raft log directory {}: {}",
                    parent.display(),
                    e
                )
            })?;
        }
        let contents = serde_json::to_string_pretty(&PersistedRaftLogState {
            log: log.to_vec(),
            commit_index,
        })
        .expect("Failed to serialize Raft log");
        fs::write(&self.path, contents)
            .map_err(|e| format!("Failed to write Raft log {}: {}", self.path.display(), e))
    }
}

pub(crate) struct InMemoryRaftLogStore {
    log: Vec<RaftLogEntry>,
    commit_index: Option<u64>,
}

impl InMemoryRaftLogStore {
    pub(crate) fn new() -> Self {
        Self {
            log: Vec::new(),
            commit_index: None,
        }
    }
}

impl InMemoryRaftLogStore {
    pub(crate) fn load(&self) -> Result<RaftLogState, String> {
        Ok(RaftLogState {
            log: self.log.clone(),
            commit_index: self.commit_index,
        })
    }

    pub(crate) fn save(&mut self, log: &[RaftLogEntry], commit_index: u64) -> Result<(), String> {
        self.log = log.to_vec();
        self.commit_index = Some(commit_index);
        Ok(())
    }
}

pub(crate) enum AnyRaftLogStore {
    File(FileRaftLogStore),
    InMemory(InMemoryRaftLogStore),
}

impl AnyRaftLogStore {
    pub(crate) fn load(&self) -> Result<RaftLogState, String> {
        match self {
            AnyRaftLogStore::File(store) => store.load(),
            AnyRaftLogStore::InMemory(store) => store.load(),
        }
    }

    pub(crate) fn save(&mut self, log: &[RaftLogEntry], commit_index: u64) -> Result<(), String> {
        match self {
            AnyRaftLogStore::File(store) => store.save(log, commit_index),
            AnyRaftLogStore::InMemory(store) => store.save(log, commit_index),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FileRaftLogStore, InMemoryRaftLogStore};
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

        store.save(&log, 1).unwrap();

        assert_eq!(store.load().unwrap().log, log);
        assert_eq!(store.load().unwrap().commit_index, Some(1));
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

        store.save(&log, 1).unwrap();

        assert_eq!(store.load().unwrap().log, log);
        assert_eq!(store.load().unwrap().commit_index, Some(1));
    }
}
