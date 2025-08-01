use super::*;
use super::snapshot::FileRaftStorage;
use std::path::{Path, PathBuf};
use parking_lot::RwLock;
use std::sync::Arc;

/// In-memory Raft storage for testing
pub struct MemoryRaftStorage {
    state: Arc<RwLock<PersistentState>>,
    snapshot: Arc<RwLock<Option<RaftSnapshot>>>,
}

impl MemoryRaftStorage {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(PersistentState::default())),
            snapshot: Arc::new(RwLock::new(None)),
        }
    }
}

impl RaftStorage for MemoryRaftStorage {
    fn save_state(&self, state: &PersistentState) -> Result<()> {
        *self.state.write() = state.clone();
        Ok(())
    }
    
    fn load_state(&self) -> Result<PersistentState> {
        Ok(self.state.read().clone())
    }
    
    fn append_entries(&self, entries: &[LogEntry]) -> Result<()> {
        let mut state = self.state.write();
        for entry in entries {
            if !state.log.iter().any(|e| e.index == entry.index) {
                state.log.push(entry.clone());
            }
        }
        Ok(())
    }
    
    fn delete_entries_from(&self, index: LogIndex) -> Result<()> {
        let mut state = self.state.write();
        state.log.retain(|e| e.index < index);
        Ok(())
    }
    
    fn save_snapshot(&self, snapshot: &RaftSnapshot) -> Result<()> {
        *self.snapshot.write() = Some(snapshot.clone());
        Ok(())
    }
    
    fn load_snapshot(&self) -> Result<Option<RaftSnapshot>> {
        Ok(self.snapshot.read().clone())
    }
}

/// Hybrid storage that uses memory for speed and disk for persistence
pub struct HybridRaftStorage {
    memory: MemoryRaftStorage,
    disk: FileRaftStorage,
}

impl HybridRaftStorage {
    pub fn new(base_dir: impl AsRef<Path>, node_id: NodeId) -> Result<Self> {
        let disk = FileRaftStorage::new(base_dir, node_id)?;
        let memory = MemoryRaftStorage::new();
        
        // Load initial state from disk
        if let Ok(state) = disk.load_state() {
            memory.save_state(&state)?;
        }
        
        if let Ok(Some(snapshot)) = disk.load_snapshot() {
            memory.save_snapshot(&snapshot)?;
        }
        
        Ok(Self { memory, disk })
    }
}

impl RaftStorage for HybridRaftStorage {
    fn save_state(&self, state: &PersistentState) -> Result<()> {
        // Save to memory first
        self.memory.save_state(state)?;
        
        // Then persist to disk
        self.disk.save_state(state)
    }
    
    fn load_state(&self) -> Result<PersistentState> {
        // Always read from memory (which was loaded from disk on startup)
        self.memory.load_state()
    }
    
    fn append_entries(&self, entries: &[LogEntry]) -> Result<()> {
        self.memory.append_entries(entries)?;
        self.disk.append_entries(entries)
    }
    
    fn delete_entries_from(&self, index: LogIndex) -> Result<()> {
        self.memory.delete_entries_from(index)?;
        self.disk.delete_entries_from(index)
    }
    
    fn save_snapshot(&self, snapshot: &RaftSnapshot) -> Result<()> {
        self.memory.save_snapshot(snapshot)?;
        self.disk.save_snapshot(snapshot)
    }
    
    fn load_snapshot(&self) -> Result<Option<RaftSnapshot>> {
        self.memory.load_snapshot()
    }
}

/// Storage metrics for monitoring
#[derive(Debug, Default)]
pub struct StorageMetrics {
    pub state_writes: AtomicU64,
    pub state_reads: AtomicU64,
    pub log_appends: AtomicU64,
    pub log_truncates: AtomicU64,
    pub snapshot_saves: AtomicU64,
    pub snapshot_loads: AtomicU64,
}

/// Instrumented storage wrapper for metrics
pub struct InstrumentedStorage<S: RaftStorage> {
    inner: S,
    metrics: Arc<StorageMetrics>,
}

impl<S: RaftStorage> InstrumentedStorage<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            metrics: Arc::new(StorageMetrics::default()),
        }
    }
    
    pub fn metrics(&self) -> Arc<StorageMetrics> {
        self.metrics.clone()
    }
}

impl<S: RaftStorage> RaftStorage for InstrumentedStorage<S> {
    fn save_state(&self, state: &PersistentState) -> Result<()> {
        self.metrics.state_writes.fetch_add(1, Ordering::Relaxed);
        self.inner.save_state(state)
    }
    
    fn load_state(&self) -> Result<PersistentState> {
        self.metrics.state_reads.fetch_add(1, Ordering::Relaxed);
        self.inner.load_state()
    }
    
    fn append_entries(&self, entries: &[LogEntry]) -> Result<()> {
        self.metrics.log_appends.fetch_add(entries.len() as u64, Ordering::Relaxed);
        self.inner.append_entries(entries)
    }
    
    fn delete_entries_from(&self, index: LogIndex) -> Result<()> {
        self.metrics.log_truncates.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_entries_from(index)
    }
    
    fn save_snapshot(&self, snapshot: &RaftSnapshot) -> Result<()> {
        self.metrics.snapshot_saves.fetch_add(1, Ordering::Relaxed);
        self.inner.save_snapshot(snapshot)
    }
    
    fn load_snapshot(&self) -> Result<Option<RaftSnapshot>> {
        self.metrics.snapshot_loads.fetch_add(1, Ordering::Relaxed);
        self.inner.load_snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_memory_storage() {
        let storage = MemoryRaftStorage::new();
        
        let mut state = PersistentState::default();
        state.current_term = 10;
        
        storage.save_state(&state).unwrap();
        
        let loaded = storage.load_state().unwrap();
        assert_eq!(loaded.current_term, 10);
    }
    
    #[test]
    fn test_hybrid_storage() {
        let temp_dir = TempDir::new().unwrap();
        let storage = HybridRaftStorage::new(temp_dir.path(), 1).unwrap();
        
        let entry = LogEntry {
            term: 1,
            index: 1,
            command: Command::NoOp,
            client_id: None,
            request_id: None,
        };
        
        storage.append_entries(&[entry.clone()]).unwrap();
        
        let state = storage.load_state().unwrap();
        assert_eq!(state.log.len(), 1);
        assert_eq!(state.log[0].index, 1);
    }
    
    #[test]
    fn test_instrumented_storage() {
        let inner = MemoryRaftStorage::new();
        let storage = InstrumentedStorage::new(inner);
        
        let state = PersistentState::default();
        storage.save_state(&state).unwrap();
        storage.load_state().unwrap();
        
        let metrics = storage.metrics();
        assert_eq!(metrics.state_writes.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.state_reads.load(Ordering::Relaxed), 1);
    }
}