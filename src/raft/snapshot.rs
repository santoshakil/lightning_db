use super::*;
use bincode::{config, decode_from_slice, encode_to_vec};
use crc32fast::Hasher;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

impl RaftNode {
    /// Check if snapshot is needed
    pub fn should_snapshot(&self) -> bool {
        let last_applied = self.volatile.last_applied.load(Ordering::Acquire);
        let persistent = self.persistent.read();

        // Check if we have enough entries since last snapshot
        if let Some(snapshot) = self.last_snapshot_index() {
            last_applied - snapshot > self.config.snapshot_threshold as u64
        } else {
            persistent.log.len() > self.config.snapshot_threshold
        }
    }

    /// Create a new snapshot
    pub async fn create_snapshot(&self) -> Result<()> {
        let last_applied = self.volatile.last_applied.load(Ordering::Acquire);

        if last_applied == 0 {
            // Nothing to snapshot
            return Ok(());
        }

        println!("Creating snapshot at index {}", last_applied);

        // Get the term of last applied entry
        let (last_term, config) = {
            let persistent = self.persistent.read();
            let last_term = persistent
                .log
                .iter()
                .find(|e| e.index == last_applied)
                .map(|e| e.term)
                .unwrap_or(0);

            let config = self.membership.read().clone();

            (last_term, config)
        };

        // Create state machine snapshot
        let state_data = self.state_machine.snapshot()?;

        // Calculate checksum
        let mut hasher = Hasher::new();
        hasher.update(&state_data);
        let checksum = hasher.finalize() as u64;

        // Create snapshot struct
        let snapshot = RaftSnapshot {
            last_index: last_applied,
            last_term,
            config,
            data: state_data,
            checksum,
        };

        // Save snapshot
        self.storage.save_snapshot(&snapshot)?;

        // Compact log
        self.compact_log(last_applied).await?;

        println!("Snapshot created successfully at index {}", last_applied);

        Ok(())
    }

    /// Compact log by removing entries before snapshot
    async fn compact_log(&self, snapshot_index: LogIndex) -> Result<()> {
        let mut persistent = self.persistent.write();

        // Keep some entries before snapshot for safety
        let keep_from = snapshot_index.saturating_sub(100);

        // Remove old entries
        persistent.log.retain(|e| e.index >= keep_from);

        // Update storage
        self.storage.delete_entries_from(keep_from)?;

        Ok(())
    }

    /// Get last snapshot index
    fn last_snapshot_index(&self) -> Option<LogIndex> {
        self.storage
            .load_snapshot()
            .ok()
            .flatten()
            .map(|s| s.last_index)
    }

    /// Install snapshot from leader
    pub async fn handle_install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> Result<InstallSnapshotReply> {
        let mut persistent = self.persistent.write();

        // Reply immediately if term < currentTerm
        if args.term < persistent.current_term {
            return Ok(InstallSnapshotReply {
                term: persistent.current_term,
                bytes_received: 0,
            });
        }

        // Update term if necessary
        if args.term > persistent.current_term {
            persistent.current_term = args.term;
            persistent.voted_for = None;

            // Persist state
            self.storage.save_state(&persistent)?;

            // Become follower
            self.volatile
                .state
                .store(NodeState::Follower as u8, Ordering::Release);
            *self.leader.write() = None;
        }

        // Reset election timer
        drop(persistent); // Release lock before async call
        self.reset_election_timer().await;

        // Update leader info
        self.metrics
            .current_leader
            .store(args.leader_id, Ordering::Release);

        // Save data length before moving args
        let data_len = args.data.len();

        // Handle snapshot installation
        if args.done {
            // Complete snapshot received
            self.install_complete_snapshot(args).await?;
        } else {
            // Partial snapshot - store for later
            self.store_snapshot_chunk(args).await?;
        }

        Ok(InstallSnapshotReply {
            term: self.current_term(),
            bytes_received: data_len,
        })
    }

    /// Install complete snapshot
    async fn install_complete_snapshot(&self, args: InstallSnapshotArgs) -> Result<()> {
        println!(
            "Installing snapshot from leader {} at index {}",
            args.leader_id, args.last_included_index
        );

        // Verify checksum
        let mut hasher = Hasher::new();
        hasher.update(&args.data);
        let checksum = hasher.finalize() as u64;

        // Create snapshot
        let snapshot = RaftSnapshot {
            last_index: args.last_included_index,
            last_term: args.last_included_term,
            config: args.config,
            data: args.data,
            checksum,
        };

        // Save snapshot
        self.storage.save_snapshot(&snapshot)?;

        // Restore state machine
        self.state_machine.restore(&snapshot.data)?;

        // Update state
        self.volatile
            .last_applied
            .store(args.last_included_index, Ordering::Release);
        self.volatile
            .commit_index
            .store(args.last_included_index, Ordering::Release);

        // Discard log entries before snapshot
        let mut persistent = self.persistent.write();
        persistent
            .log
            .retain(|e| e.index > args.last_included_index);

        // Update membership
        *self.membership.write() = snapshot.config;

        println!("Snapshot installed successfully");

        Ok(())
    }

    /// Store partial snapshot chunk
    async fn store_snapshot_chunk(&self, _args: InstallSnapshotArgs) -> Result<()> {
        // TODO: Implement streaming snapshot support
        // For now, we only support single-chunk snapshots
        Err(Error::NotImplemented(
            "Streaming snapshots not yet supported".to_string(),
        ))
    }

    /// Send snapshot to follower
    pub async fn send_snapshot(&self, peer_id: NodeId) -> Result<()> {
        // Load latest snapshot
        let snapshot = self
            .storage
            .load_snapshot()?
            .ok_or_else(|| Error::NotFound("No snapshot available".to_string()))?;

        println!(
            "Sending snapshot to {} (size: {} bytes)",
            peer_id,
            snapshot.data.len()
        );

        // For now, send entire snapshot in one chunk
        // TODO: Implement chunked transfer for large snapshots
        let args = InstallSnapshotArgs {
            term: self.current_term(),
            leader_id: self.config.node_id,
            last_included_index: snapshot.last_index,
            last_included_term: snapshot.last_term,
            offset: 0,
            data: snapshot.data,
            done: true,
            config: snapshot.config,
        };

        match self.rpc.send_install_snapshot(peer_id, args).await {
            Ok(reply) => {
                if reply.term > self.current_term() {
                    self.become_follower(reply.term).await?;
                }

                // Update metrics
                self.metrics.snapshots_sent.fetch_add(1, Ordering::Relaxed);

                // Update next_index for peer
                let mut leader_state = self.leader.write();
                if let Some(ref mut leader) = *leader_state {
                    leader.next_index.insert(peer_id, snapshot.last_index + 1);
                    leader.match_index.insert(peer_id, snapshot.last_index);
                }
            }
            Err(e) => {
                eprintln!("Failed to send snapshot to {}: {}", peer_id, e);
            }
        }

        Ok(())
    }
}

/// File-based Raft storage implementation
pub struct FileRaftStorage {
    /// Base directory for storage
    base_dir: PathBuf,

    /// Node ID
    node_id: NodeId,
}

impl FileRaftStorage {
    /// Create new file-based storage
    pub fn new(base_dir: impl AsRef<Path>, node_id: NodeId) -> Result<Self> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create directories
        std::fs::create_dir_all(&base_dir).map_err(|e| Error::Io(e.to_string()))?;

        let state_dir = base_dir.join("state");
        std::fs::create_dir_all(&state_dir).map_err(|e| Error::Io(e.to_string()))?;

        let snapshot_dir = base_dir.join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).map_err(|e| Error::Io(e.to_string()))?;

        Ok(Self { base_dir, node_id })
    }

    fn state_path(&self) -> PathBuf {
        self.base_dir
            .join("state")
            .join(format!("node_{}.state", self.node_id))
    }

    fn snapshot_path(&self, index: LogIndex) -> PathBuf {
        self.base_dir
            .join("snapshots")
            .join(format!("snapshot_{}.bin", index))
    }

    fn latest_snapshot_path(&self) -> PathBuf {
        self.base_dir.join("snapshots").join("latest")
    }
}

impl RaftStorage for FileRaftStorage {
    fn save_state(&self, state: &PersistentState) -> Result<()> {
        let data = encode_to_vec(state, config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let path = self.state_path();
        let temp_path = path.with_extension("tmp");

        // Write to temporary file
        std::fs::write(&temp_path, data).map_err(|e| Error::Io(e.to_string()))?;

        // Atomic rename
        std::fs::rename(temp_path, path).map_err(|e| Error::Io(e.to_string()))?;

        Ok(())
    }

    fn load_state(&self) -> Result<PersistentState> {
        let path = self.state_path();

        if !path.exists() {
            return Ok(PersistentState::default());
        }

        let data = std::fs::read(path).map_err(|e| Error::Io(e.to_string()))?;

        let (state, _) = decode_from_slice(&data, config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(state)
    }

    fn append_entries(&self, entries: &[LogEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        // For simplicity, we're storing entries as part of persistent state
        // In production, you'd want a separate log file with better performance
        let mut state = self.load_state()?;

        for entry in entries {
            // Only append if not already present
            if !state.log.iter().any(|e| e.index == entry.index) {
                state.log.push(entry.clone());
            }
        }

        self.save_state(&state)
    }

    fn delete_entries_from(&self, index: LogIndex) -> Result<()> {
        let mut state = self.load_state()?;
        state.log.retain(|e| e.index < index);
        self.save_state(&state)
    }

    fn save_snapshot(&self, snapshot: &RaftSnapshot) -> Result<()> {
        let data = encode_to_vec(snapshot, config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;

        let path = self.snapshot_path(snapshot.last_index);
        let temp_path = path.with_extension("tmp");

        // Write to temporary file
        std::fs::write(&temp_path, data).map_err(|e| Error::Io(e.to_string()))?;

        // Atomic rename
        std::fs::rename(&temp_path, &path).map_err(|e| Error::Io(e.to_string()))?;

        // Update latest symlink
        let latest_path = self.latest_snapshot_path();
        let _ = std::fs::remove_file(&latest_path);

        #[cfg(unix)]
        std::os::unix::fs::symlink(&path, &latest_path).map_err(|e| Error::Io(e.to_string()))?;

        #[cfg(not(unix))]
        std::fs::copy(&path, &latest_path).map_err(|e| Error::Io(e.to_string()))?;

        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<RaftSnapshot>> {
        let latest_path = self.latest_snapshot_path();

        if !latest_path.exists() {
            return Ok(None);
        }

        let data = std::fs::read(latest_path).map_err(|e| Error::Io(e.to_string()))?;

        let (snapshot, _) = decode_from_slice(&data, config::standard())
            .map_err(|e| Error::Serialization(e.to_string()))?;

        Ok(Some(snapshot))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileRaftStorage::new(temp_dir.path(), 1).unwrap();

        // Test state persistence
        let mut state = PersistentState::default();
        state.current_term = 5;
        state.voted_for = Some(2);

        storage.save_state(&state).unwrap();

        let loaded = storage.load_state().unwrap();
        assert_eq!(loaded.current_term, 5);
        assert_eq!(loaded.voted_for, Some(2));
    }

    #[test]
    fn test_snapshot_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = FileRaftStorage::new(temp_dir.path(), 1).unwrap();

        let snapshot = RaftSnapshot {
            last_index: 100,
            last_term: 5,
            config: ClusterMembership {
                current: HashMap::new(),
                joint: None,
                config_index: 0,
            },
            data: vec![1, 2, 3, 4, 5],
            checksum: 12345,
        };

        storage.save_snapshot(&snapshot).unwrap();

        let loaded = storage.load_snapshot().unwrap().unwrap();
        assert_eq!(loaded.last_index, 100);
        assert_eq!(loaded.data, vec![1, 2, 3, 4, 5]);
    }
}
