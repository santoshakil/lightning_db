use crate::core::error::Error;
use crate::core::async_page_manager::PageManagerAsync;
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use bytes::{Bytes, BytesMut, BufMut};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorMetadata {
    pub id: u64,
    pub dimensions: usize,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorStorage {
    pub vector_id: u64,
    pub vector_data: Vec<f32>,
    pub metadata: VectorMetadata,
    pub index_entries: Vec<IndexEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    pub index_name: String,
    pub index_type: String,
    pub position: u64,
    pub auxiliary_data: Vec<u8>,
}

const VECTOR_METADATA_PREFIX: &[u8] = b"VM:";
const VECTOR_DATA_PREFIX: &[u8] = b"VD:";
const INDEX_ENTRY_PREFIX: &[u8] = b"IE:";

pub async fn store_metadata(
    storage: &dyn PageManagerAsync,
    metadata: &VectorMetadata,
) -> Result<(), Error> {
    let key = build_metadata_key(metadata.id);
    let value = serialize_metadata(metadata)?;
    
    storage.write_page_async(
        key_to_page_id(&key),
        &value,
    ).await?;
    
    Ok(())
}

pub async fn load_metadata(
    storage: &dyn PageManagerAsync,
    id: u64,
) -> Result<VectorMetadata, Error> {
    let key = build_metadata_key(id);
    let page_id = key_to_page_id(&key);
    
    let data = storage.read_page_async(page_id).await?;
    deserialize_metadata(&data)
}

pub async fn delete_metadata(
    storage: &dyn PageManagerAsync,
    id: u64,
) -> Result<(), Error> {
    let key = build_metadata_key(id);
    let page_id = key_to_page_id(&key);
    
    storage.delete_page_async(page_id).await?;
    
    Ok(())
}

pub async fn store_vector_data(
    storage: &dyn PageManagerAsync,
    id: u64,
    vector: &[f32],
) -> Result<(), Error> {
    let key = build_vector_key(id);
    let value = serialize_vector(vector)?;
    
    storage.write_page_async(
        key_to_page_id(&key),
        &value,
    ).await?;
    
    Ok(())
}

pub async fn load_vector_data(
    storage: &dyn PageManagerAsync,
    id: u64,
) -> Result<Vec<f32>, Error> {
    let key = build_vector_key(id);
    let page_id = key_to_page_id(&key);
    
    let data = storage.read_page_async(page_id).await?;
    deserialize_vector(&data)
}

pub async fn batch_store_vectors(
    storage: &dyn PageManagerAsync,
    vectors: Vec<(u64, Vec<f32>, VectorMetadata)>,
) -> Result<(), Error> {
    for (id, vector, metadata) in vectors {
        store_vector_data(storage, id, &vector).await?;
        store_metadata(storage, &metadata).await?;
    }
    
    Ok(())
}

pub async fn batch_load_vectors(
    storage: &dyn PageManagerAsync,
    ids: &[u64],
) -> Result<Vec<(Vec<f32>, VectorMetadata)>, Error> {
    let mut results = Vec::new();
    
    for &id in ids {
        let vector = load_vector_data(storage, id).await?;
        let metadata = load_metadata(storage, id).await?;
        results.push((vector, metadata));
    }
    
    Ok(results)
}

pub async fn store_index_entry(
    storage: &dyn PageManagerAsync,
    vector_id: u64,
    entry: &IndexEntry,
) -> Result<(), Error> {
    let key = build_index_entry_key(vector_id, &entry.index_name);
    let value = serialize_index_entry(entry)?;
    
    storage.write_page_async(
        key_to_page_id(&key),
        &value,
    ).await?;
    
    Ok(())
}

pub async fn load_index_entries(
    storage: &dyn PageManagerAsync,
    vector_id: u64,
) -> Result<Vec<IndexEntry>, Error> {
    let prefix = build_index_entry_prefix(vector_id);
    let start_page = key_to_page_id(&prefix);
    
    let mut entries = Vec::new();
    let mut current_page = start_page;
    
    for _ in 0..100 {
        match storage.read_page_async(current_page).await {
            Ok(data) => {
                if let Ok(entry) = deserialize_index_entry(&data) {
                    entries.push(entry);
                }
                current_page += 1;
            },
            Err(_) => break,
        }
    }
    
    Ok(entries)
}

pub struct VectorStorageManager {
    storage: std::sync::Arc<dyn PageManagerAsync>,
    cache: dashmap::DashMap<u64, CachedVector>,
}

struct CachedVector {
    vector: Vec<f32>,
    metadata: VectorMetadata,
    last_accessed: std::time::Instant,
}

impl VectorStorageManager {
    pub fn new(storage: std::sync::Arc<dyn PageManagerAsync>) -> Self {
        Self {
            storage,
            cache: dashmap::DashMap::new(),
        }
    }

    pub async fn store(&self, id: u64, vector: Vec<f32>, metadata: HashMap<String, Vec<u8>>) -> Result<(), Error> {
        let meta = VectorMetadata {
            id,
            dimensions: vector.len(),
            created_at: Utc::now(),
            metadata,
        };
        
        store_vector_data(&*self.storage, id, &vector).await?;
        store_metadata(&*self.storage, &meta).await?;
        
        self.cache.insert(id, CachedVector {
            vector,
            metadata: meta,
            last_accessed: std::time::Instant::now(),
        });
        
        Ok(())
    }

    pub async fn load(&self, id: u64) -> Result<(Vec<f32>, VectorMetadata), Error> {
        if let Some(cached) = self.cache.get(&id) {
            return Ok((cached.vector.clone(), cached.metadata.clone()));
        }
        
        let vector = load_vector_data(&*self.storage, id).await?;
        let metadata = load_metadata(&*self.storage, id).await?;
        
        self.cache.insert(id, CachedVector {
            vector: vector.clone(),
            metadata: metadata.clone(),
            last_accessed: std::time::Instant::now(),
        });
        
        Ok((vector, metadata))
    }

    pub async fn delete(&self, id: u64) -> Result<(), Error> {
        self.cache.remove(&id);
        
        delete_metadata(&*self.storage, id).await?;
        
        let vector_key = build_vector_key(id);
        self.storage.delete_page_async(key_to_page_id(&vector_key)).await?;
        
        Ok(())
    }

    pub fn evict_cache(&self, max_size: usize) {
        if self.cache.len() <= max_size {
            return;
        }
        
        let mut entries: Vec<(u64, std::time::Instant)> = self.cache
            .iter()
            .map(|entry| (*entry.key(), entry.last_accessed))
            .collect();
        
        entries.sort_by_key(|e| e.1);
        
        let to_evict = self.cache.len() - max_size;
        for (id, _) in entries.into_iter().take(to_evict) {
            self.cache.remove(&id);
        }
    }
}

fn build_metadata_key(id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(VECTOR_METADATA_PREFIX.len() + 8);
    key.extend_from_slice(VECTOR_METADATA_PREFIX);
    key.extend_from_slice(&id.to_le_bytes());
    key
}

fn build_vector_key(id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(VECTOR_DATA_PREFIX.len() + 8);
    key.extend_from_slice(VECTOR_DATA_PREFIX);
    key.extend_from_slice(&id.to_le_bytes());
    key
}

fn build_index_entry_key(vector_id: u64, index_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(INDEX_ENTRY_PREFIX.len() + 8 + index_name.len());
    key.extend_from_slice(INDEX_ENTRY_PREFIX);
    key.extend_from_slice(&vector_id.to_le_bytes());
    key.extend_from_slice(index_name.as_bytes());
    key
}

fn build_index_entry_prefix(vector_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(INDEX_ENTRY_PREFIX.len() + 8);
    key.extend_from_slice(INDEX_ENTRY_PREFIX);
    key.extend_from_slice(&vector_id.to_le_bytes());
    key
}

fn key_to_page_id(key: &[u8]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

fn serialize_metadata(metadata: &VectorMetadata) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(metadata)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn deserialize_metadata(data: &[u8]) -> Result<VectorMetadata, Error> {
    bincode::deserialize(data)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn serialize_vector(vector: &[f32]) -> Result<Vec<u8>, Error> {
    let mut bytes = BytesMut::with_capacity(vector.len() * 4);
    
    for &value in vector {
        bytes.put_f32_le(value);
    }
    
    Ok(bytes.to_vec())
}

fn deserialize_vector(data: &[u8]) -> Result<Vec<f32>, Error> {
    if data.len() % 4 != 0 {
        return Err(Error::InvalidInput("Invalid vector data".to_string()));
    }
    
    let mut vector = Vec::with_capacity(data.len() / 4);
    let mut cursor = 0;
    
    while cursor < data.len() {
        let value = f32::from_le_bytes([
            data[cursor],
            data[cursor + 1],
            data[cursor + 2],
            data[cursor + 3],
        ]);
        vector.push(value);
        cursor += 4;
    }
    
    Ok(vector)
}

fn serialize_index_entry(entry: &IndexEntry) -> Result<Vec<u8>, Error> {
    bincode::encode_to_vec(entry)
        .map_err(|e| Error::Serialization(e.to_string()))
}

fn deserialize_index_entry(data: &[u8]) -> Result<IndexEntry, Error> {
    bincode::deserialize(data)
        .map_err(|e| Error::Serialization(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_serialization() {
        let vector = vec![1.0, 2.0, 3.0, 4.0];
        let serialized = serialize_vector(&vector).unwrap();
        let deserialized = deserialize_vector(&serialized).unwrap();
        assert_eq!(vector, deserialized);
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = VectorMetadata {
            id: 123,
            dimensions: 128,
            created_at: Utc::now(),
            metadata: HashMap::new(),
        };
        
        let serialized = serialize_metadata(&metadata).unwrap();
        let deserialized = deserialize_metadata(&serialized).unwrap();
        
        assert_eq!(metadata.id, deserialized.id);
        assert_eq!(metadata.dimensions, deserialized.dimensions);
    }
}