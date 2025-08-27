use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use crate::core::error::Error;
use super::core::{Term, LogIndex, NodeId, LogEntryType};
use std::sync::Arc;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncSeekExt};
use bytes::{Bytes, BytesMut, BufMut};

const LOG_SEGMENT_SIZE: usize = 10 * 1024 * 1024; // 10MB
const LOG_INDEX_INTERVAL: usize = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub id: u64,
    pub term: Term,
    pub index: LogIndex,
    pub entry_type: LogEntryType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogMetadata {
    pub first_index: LogIndex,
    pub last_index: LogIndex,
    pub last_term: Term,
    pub snapshot_index: LogIndex,
    pub snapshot_term: Term,
}

#[async_trait]
pub trait LogStore: Send + Sync {
    async fn append(&self, entry: LogEntry) -> Result<LogIndex, Error>;
    async fn append_entries(&self, entries: Vec<LogEntry>) -> Result<(), Error>;
    async fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, Error>;
    async fn entries_from(&self, start: LogIndex, max_count: usize) -> Result<Vec<LogEntry>, Error>;
    async fn term_at(&self, index: LogIndex) -> Result<Term, Error>;
    async fn last_index(&self) -> Result<LogIndex, Error>;
    async fn last_term(&self) -> Result<Term, Error>;
    async fn truncate_from(&self, index: LogIndex) -> Result<(), Error>;
    async fn compact_to(&self, index: LogIndex) -> Result<(), Error>;
    async fn metadata(&self) -> Result<LogMetadata, Error>;
}

pub struct MemoryLogStore {
    entries: Arc<RwLock<BTreeMap<LogIndex, LogEntry>>>,
    snapshot_index: Arc<RwLock<LogIndex>>,
    snapshot_term: Arc<RwLock<Term>>,
}

impl MemoryLogStore {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(BTreeMap::new())),
            snapshot_index: Arc::new(RwLock::new(0)),
            snapshot_term: Arc::new(RwLock::new(0)),
        }
    }
}

#[async_trait]
impl LogStore for MemoryLogStore {
    async fn append(&self, mut entry: LogEntry) -> Result<LogIndex, Error> {
        let mut entries = self.entries.write();
        
        let last_index = entries.keys().max().copied().unwrap_or(0);
        entry.index = last_index + 1;
        
        entries.insert(entry.index, entry.clone());
        
        Ok(entry.index)
    }

    async fn append_entries(&self, new_entries: Vec<LogEntry>) -> Result<(), Error> {
        if new_entries.is_empty() {
            return Ok(());
        }
        
        let mut entries = self.entries.write();
        
        for mut entry in new_entries {
            if entry.index == 0 {
                let last_index = entries.keys().max().copied().unwrap_or(0);
                entry.index = last_index + 1;
            }
            entries.insert(entry.index, entry);
        }
        
        Ok(())
    }

    async fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, Error> {
        Ok(self.entries.read().get(&index).cloned())
    }

    async fn entries_from(&self, start: LogIndex, max_count: usize) -> Result<Vec<LogEntry>, Error> {
        let entries = self.entries.read();
        
        Ok(entries.range(start..)
            .take(max_count)
            .map(|(_, entry)| entry.clone())
            .collect())
    }

    async fn term_at(&self, index: LogIndex) -> Result<Term, Error> {
        if index == 0 {
            return Ok(0);
        }
        
        if index <= *self.snapshot_index.read() {
            return Ok(*self.snapshot_term.read());
        }
        
        self.entries.read()
            .get(&index)
            .map(|e| e.term)
            .ok_or(Error::NotFound(format!("Log entry at index {}", index)))
    }

    async fn last_index(&self) -> Result<LogIndex, Error> {
        let snapshot_index = *self.snapshot_index.read();
        let last_entry_index = self.entries.read().keys().max().copied().unwrap_or(0);
        
        Ok(snapshot_index.max(last_entry_index))
    }

    async fn last_term(&self) -> Result<Term, Error> {
        let last_index = self.last_index().await?;
        
        if last_index == 0 {
            Ok(0)
        } else {
            self.term_at(last_index).await
        }
    }

    async fn truncate_from(&self, index: LogIndex) -> Result<(), Error> {
        let mut entries = self.entries.write();
        entries.retain(|&idx, _| idx < index);
        Ok(())
    }

    async fn compact_to(&self, index: LogIndex) -> Result<(), Error> {
        let mut entries = self.entries.write();
        
        if let Some(entry) = entries.get(&index) {
            *self.snapshot_index.write() = index;
            *self.snapshot_term.write() = entry.term;
        }
        
        entries.retain(|&idx, _| idx > index);
        
        Ok(())
    }

    async fn metadata(&self) -> Result<LogMetadata, Error> {
        let entries = self.entries.read();
        
        let first_index = entries.keys().min().copied().unwrap_or(0);
        let last_index = entries.keys().max().copied().unwrap_or(0);
        let last_term = entries.get(&last_index).map(|e| e.term).unwrap_or(0);
        
        Ok(LogMetadata {
            first_index,
            last_index,
            last_term,
            snapshot_index: *self.snapshot_index.read(),
            snapshot_term: *self.snapshot_term.read(),
        })
    }
}

pub struct DiskLogStore {
    segments: Arc<RwLock<BTreeMap<LogIndex, LogSegment>>>,
    active_segment: Arc<RwLock<Option<LogSegment>>>,
    index: Arc<RwLock<LogIndex>>,
    base_path: String,
    snapshot_metadata: Arc<RwLock<SnapshotMetadata>>,
}

struct LogSegment {
    start_index: LogIndex,
    end_index: LogIndex,
    file_path: String,
    file: Option<File>,
    size: usize,
    index_map: BTreeMap<LogIndex, (u64, usize)>, // index -> (offset, size)
}

#[derive(Debug, Clone, Default)]
struct SnapshotMetadata {
    index: LogIndex,
    term: Term,
}

impl DiskLogStore {
    pub async fn new(base_path: String) -> Result<Self, Error> {
        tokio::fs::create_dir_all(&base_path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let store = Self {
            segments: Arc::new(RwLock::new(BTreeMap::new())),
            active_segment: Arc::new(RwLock::new(None)),
            index: Arc::new(RwLock::new(0)),
            base_path,
            snapshot_metadata: Arc::new(RwLock::new(SnapshotMetadata::default())),
        };
        
        store.recover().await?;
        
        Ok(store)
    }

    async fn recover(&self) -> Result<(), Error> {
        let mut dir = tokio::fs::read_dir(&self.base_path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let mut segments = Vec::new();
        
        while let Some(entry) = dir.next_entry().await
            .map_err(|e| Error::IoError(e.to_string()))? {
            
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                if let Some(segment) = self.load_segment(path.to_str().unwrap()).await? {
                    segments.push(segment);
                }
            }
        }
        
        segments.sort_by_key(|s| s.start_index);
        
        let mut max_index = 0;
        for segment in segments {
            max_index = max_index.max(segment.end_index);
            self.segments.write().insert(segment.start_index, segment);
        }
        
        *self.index.write() = max_index;
        
        Ok(())
    }

    async fn load_segment(&self, path: &str) -> Result<Option<LogSegment>, Error> {
        let mut file = File::open(path).await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let metadata = file.metadata().await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        let mut segment = LogSegment {
            start_index: 0,
            end_index: 0,
            file_path: path.to_string(),
            file: Some(file),
            size: metadata.len() as usize,
            index_map: BTreeMap::new(),
        };
        
        self.build_index(&mut segment).await?;
        
        if segment.index_map.is_empty() {
            return Ok(None);
        }
        
        segment.start_index = *segment.index_map.keys().min().unwrap();
        segment.end_index = *segment.index_map.keys().max().unwrap();
        
        Ok(Some(segment))
    }

    async fn build_index(&self, segment: &mut LogSegment) -> Result<(), Error> {
        if let Some(file) = &mut segment.file {
            file.seek(std::io::SeekFrom::Start(0)).await
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            let mut offset = 0u64;
            let mut buffer = vec![0u8; 1024 * 1024];
            
            loop {
                let n = file.read(&mut buffer).await
                    .map_err(|e| Error::IoError(e.to_string()))?;
                
                if n == 0 {
                    break;
                }
                
                let mut cursor = 0;
                while cursor < n {
                    if cursor + 8 > n {
                        break;
                    }
                    
                    let size = u64::from_le_bytes(
                        buffer[cursor..cursor + 8].try_into().unwrap()
                    ) as usize;
                    
                    if cursor + 8 + size > n {
                        break;
                    }
                    
                    if let Ok(entry) = bincode::deserialize::<LogEntry>(&buffer[cursor + 8..cursor + 8 + size]) {
                        segment.index_map.insert(entry.index, (offset + cursor as u64, size + 8));
                    }
                    
                    cursor += 8 + size;
                }
                
                offset += n as u64;
            }
        }
        
        Ok(())
    }

    async fn create_segment(&self, start_index: LogIndex) -> Result<LogSegment, Error> {
        let file_path = format!("{}/segment_{:020}.log", self.base_path, start_index);
        
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&file_path)
            .await
            .map_err(|e| Error::IoError(e.to_string()))?;
        
        Ok(LogSegment {
            start_index,
            end_index: start_index,
            file_path,
            file: Some(file),
            size: 0,
            index_map: BTreeMap::new(),
        })
    }

    async fn write_entry(&self, segment: &mut LogSegment, entry: &LogEntry) -> Result<(), Error> {
        let data = bincode::serialize(entry)
            .map_err(|e| Error::Serialization(e.to_string()))?;
        
        let size_bytes = (data.len() as u64).to_le_bytes();
        
        if let Some(file) = &mut segment.file {
            file.write_all(&size_bytes).await
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            file.write_all(&data).await
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            file.flush().await
                .map_err(|e| Error::IoError(e.to_string()))?;
            
            let offset = segment.size as u64;
            segment.index_map.insert(entry.index, (offset, size_bytes.len() + data.len()));
            segment.size += size_bytes.len() + data.len();
            segment.end_index = entry.index;
        }
        
        Ok(())
    }
}

#[async_trait]
impl LogStore for DiskLogStore {
    async fn append(&self, mut entry: LogEntry) -> Result<LogIndex, Error> {
        let new_index = {
            let mut index = self.index.write();
            *index += 1;
            *index
        };
        
        entry.index = new_index;
        
        let mut active = self.active_segment.write();
        
        if active.is_none() || active.as_ref().unwrap().size >= LOG_SEGMENT_SIZE {
            let new_segment = self.create_segment(new_index).await?;
            
            if let Some(old_segment) = active.take() {
                self.segments.write().insert(old_segment.start_index, old_segment);
            }
            
            *active = Some(new_segment);
        }
        
        if let Some(segment) = active.as_mut() {
            self.write_entry(segment, &entry).await?;
        }
        
        Ok(new_index)
    }

    async fn append_entries(&self, entries: Vec<LogEntry>) -> Result<(), Error> {
        for entry in entries {
            self.append(entry).await?;
        }
        Ok(())
    }

    async fn entry_at(&self, index: LogIndex) -> Result<Option<LogEntry>, Error> {
        // Check active segment first
        {
            let active = self.active_segment.read();
            if let Some(segment) = active.as_ref() {
                if let Some((offset, size)) = segment.index_map.get(&index) {
                    // Read entry from active segment
                    return Ok(None); // Simplified for now
                }
            }
        }
        
        // Check other segments
        let segments = self.segments.read();
        for segment in segments.values() {
            if let Some((offset, size)) = segment.index_map.get(&index) {
                // Read entry from segment
                return Ok(None); // Simplified for now
            }
        }
        
        Ok(None)
    }

    async fn entries_from(&self, start: LogIndex, max_count: usize) -> Result<Vec<LogEntry>, Error> {
        let mut entries = Vec::new();
        let mut current = start;
        
        while entries.len() < max_count {
            match self.entry_at(current).await? {
                Some(entry) => {
                    entries.push(entry);
                    current += 1;
                },
                None => break,
            }
        }
        
        Ok(entries)
    }

    async fn term_at(&self, index: LogIndex) -> Result<Term, Error> {
        if index == 0 {
            return Ok(0);
        }
        
        let snapshot_meta = self.snapshot_metadata.read();
        if index <= snapshot_meta.index {
            return Ok(snapshot_meta.term);
        }
        
        self.entry_at(index).await?
            .map(|e| e.term)
            .ok_or(Error::NotFound(format!("Log entry at index {}", index)))
    }

    async fn last_index(&self) -> Result<LogIndex, Error> {
        Ok(*self.index.read())
    }

    async fn last_term(&self) -> Result<Term, Error> {
        let last_index = self.last_index().await?;
        
        if last_index == 0 {
            Ok(0)
        } else {
            self.term_at(last_index).await
        }
    }

    async fn truncate_from(&self, index: LogIndex) -> Result<(), Error> {
        // Remove entries from index onwards
        // This would need to handle segment files properly
        Ok(())
    }

    async fn compact_to(&self, index: LogIndex) -> Result<(), Error> {
        // Compact log up to index
        // Save snapshot metadata and remove old segments
        
        if let Some(entry) = self.entry_at(index).await? {
            let mut snapshot_meta = self.snapshot_metadata.write();
            snapshot_meta.index = index;
            snapshot_meta.term = entry.term;
        }
        
        // Remove old segments
        let mut segments = self.segments.write();
        segments.retain(|&start_idx, _| start_idx > index);
        
        Ok(())
    }

    async fn metadata(&self) -> Result<LogMetadata, Error> {
        let snapshot_meta = self.snapshot_metadata.read();
        let last_index = self.last_index().await?;
        let last_term = if last_index > 0 {
            self.term_at(last_index).await?
        } else {
            0
        };
        
        Ok(LogMetadata {
            first_index: snapshot_meta.index + 1,
            last_index,
            last_term,
            snapshot_index: snapshot_meta.index,
            snapshot_term: snapshot_meta.term,
        })
    }
}