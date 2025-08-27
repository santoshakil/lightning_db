use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use super::core::{NodeId, Term};
use super::snapshot::Snapshot;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
}

#[async_trait]
pub trait RaftStorage: Send + Sync {
    async fn save_state(&self, state: PersistentState) -> Result<(), Error>;
    async fn load_state(&self) -> Result<PersistentState, Error>;
    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), Error>;
    async fn load_snapshot(&self) -> Result<Option<Snapshot>, Error>;
    async fn delete_snapshot(&self, index: u64) -> Result<(), Error>;
    async fn list_snapshots(&self) -> Result<Vec<u64>, Error>;
}

pub struct FileStorage {
    base_path: PathBuf,
}

impl FileStorage {
    pub async fn new(base_path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = base_path.as_ref().to_path_buf();
        
        tokio::fs::create_dir_all(&path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        Ok(Self {
            base_path: path,
        })
    }

    fn state_path(&self) -> PathBuf {
        self.base_path.join("raft_state.bin")
    }

    fn snapshot_path(&self, index: u64) -> PathBuf {
        self.base_path.join(format!("snapshot_{:020}.bin", index))
    }

    fn latest_snapshot_path(&self) -> PathBuf {
        self.base_path.join("latest_snapshot.bin")
    }
}

#[async_trait]
impl RaftStorage for FileStorage {
    async fn save_state(&self, state: PersistentState) -> Result<(), Error> {
        let data = bincode::serialize(&state)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        let temp_path = self.state_path().with_extension("tmp");
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        file.write_all(&data).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        file.sync_all().await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        tokio::fs::rename(temp_path, self.state_path()).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        Ok(())
    }

    async fn load_state(&self) -> Result<PersistentState, Error> {
        let path = self.state_path();
        
        if !path.exists() {
            return Ok(PersistentState {
                current_term: 0,
                voted_for: None,
            });
        }
        
        let data = tokio::fs::read(&path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        bincode::deserialize(&data)
            .map_err(|e| Error::Serialization(e.to_string()))
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), Error> {
        let index = snapshot.metadata.last_included_index;
        let data = bincode::serialize(&snapshot)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        let snapshot_path = self.snapshot_path(index);
        let temp_path = snapshot_path.with_extension("tmp");
        
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        file.write_all(&data).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        file.sync_all().await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        tokio::fs::rename(&temp_path, &snapshot_path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        // Update latest snapshot link
        let latest_path = self.latest_snapshot_path();
        let _ = tokio::fs::remove_file(&latest_path).await;
        
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            symlink(&snapshot_path, &latest_path)
                .map_err(|e| Error::IoError(e.to_string()))?;
        }
        
        #[cfg(not(unix))]
        {
            tokio::fs::copy(&snapshot_path, &latest_path).await
                .map_err(|e| Error::IoError(e.to_string()))?;
        }
        
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<Option<Snapshot>, Error> {
        let latest_path = self.latest_snapshot_path();
        
        if !latest_path.exists() {
            return Ok(None);
        }
        
        let data = tokio::fs::read(&latest_path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let snapshot = bincode::deserialize(&data)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        Ok(Some(snapshot))
    }

    async fn delete_snapshot(&self, index: u64) -> Result<(), Error> {
        let path = self.snapshot_path(index);
        
        if path.exists() {
            tokio::fs::remove_file(&path).await
                .map_err(|e| Error::IoError(e.to_string()))?;
        }
        
        Ok(())
    }

    async fn list_snapshots(&self) -> Result<Vec<u64>, Error> {
        let mut snapshots = Vec::new();
        
        let mut dir = tokio::fs::read_dir(&self.base_path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        while let Some(entry) = dir.next_entry().await
            .map_err(|e| Error::IoError(e.to_string()))? {
            
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            if name_str.starts_with("snapshot_") && name_str.ends_with(".bin") {
                if let Some(index_str) = name_str
                    .strip_prefix("snapshot_")
                    .and_then(|s| s.strip_suffix(".bin")) {
                    
                    if let Ok(index) = index_str.parse::<u64>() {
                        snapshots.push(index);
                    }
                }
            }
        }
        
        snapshots.sort();
        Ok(snapshots)
    }
}

pub struct MemoryStorage {
    state: parking_lot::RwLock<PersistentState>,
    snapshots: parking_lot::RwLock<std::collections::BTreeMap<u64, Snapshot>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            state: parking_lot::RwLock::new(PersistentState {
                current_term: 0,
                voted_for: None,
            }),
            snapshots: parking_lot::RwLock::new(std::collections::BTreeMap::new()),
        }
    }
}

#[async_trait]
impl RaftStorage for MemoryStorage {
    async fn save_state(&self, state: PersistentState) -> Result<(), Error> {
        *self.state.write() = state;
        Ok(())
    }

    async fn load_state(&self) -> Result<PersistentState, Error> {
        Ok(self.state.read().clone())
    }

    async fn save_snapshot(&self, snapshot: Snapshot) -> Result<(), Error> {
        let index = snapshot.metadata.last_included_index;
        self.snapshots.write().insert(index, snapshot);
        Ok(())
    }

    async fn load_snapshot(&self) -> Result<Option<Snapshot>, Error> {
        Ok(self.snapshots.read().values().last().cloned())
    }

    async fn delete_snapshot(&self, index: u64) -> Result<(), Error> {
        self.snapshots.write().remove(&index);
        Ok(())
    }

    async fn list_snapshots(&self) -> Result<Vec<u64>, Error> {
        Ok(self.snapshots.read().keys().cloned().collect())
    }
}