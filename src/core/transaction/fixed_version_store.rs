use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct VersionedValue {
    pub value: Option<Vec<u8>>,
    pub timestamp: u64,
    pub tx_id: u64,
}

impl VersionedValue {
    pub fn new(value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) -> Self {
        Self {
            value,
            timestamp,
            tx_id,
        }
    }
}

/// Fixed version store that properly handles cleanup
#[derive(Debug)]
pub struct FixedVersionStore {
    // key -> (timestamp -> value)
    versions: SkipMap<Vec<u8>, Arc<SkipMap<u64, VersionedValue>>>,
}

impl FixedVersionStore {
    pub fn new() -> Self {
        Self {
            versions: SkipMap::new(),
        }
    }

    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u64, tx_id: u64) {
        let versioned_value = VersionedValue::new(value, timestamp, tx_id);

        if let Some(key_versions) = self.versions.get(&key) {
            key_versions.value().insert(timestamp, versioned_value);
        } else {
            let key_versions = Arc::new(SkipMap::new());
            key_versions.insert(timestamp, versioned_value);
            self.versions.insert(key, key_versions);
        }
    }

    pub fn get(&self, key: &[u8], read_timestamp: u64) -> Option<Vec<u8>> {
        self.get_versioned(key, read_timestamp)
            .and_then(|versioned| versioned.value)
    }

    pub fn get_versioned(&self, key: &[u8], read_timestamp: u64) -> Option<VersionedValue> {
        if let Some(key_versions) = self.versions.get(key) {
            // Find the latest version that is <= read_timestamp
            let mut latest_version = None;

            for entry in key_versions.value().iter() {
                if *entry.key() <= read_timestamp {
                    latest_version = Some(entry.value().clone());
                } else {
                    break;
                }
            }

            latest_version
        } else {
            None
        }
    }

    /// Fixed cleanup that actually removes old versions and empty keys
    pub fn cleanup_old_versions(&self, before_timestamp: u64, keep_min_versions: usize) -> usize {
        let mut removed_count = 0;
        let mut empty_keys = Vec::new();

        for entry in self.versions.iter() {
            let key = entry.key().clone();
            let key_versions = entry.value();

            // Count total versions
            let total_versions = key_versions.len();
            if total_versions <= keep_min_versions {
                continue; // Keep minimum number of versions
            }

            // Find versions to remove
            let mut to_remove = Vec::new();
            let mut kept_count = 0;

            // Iterate from newest to oldest
            for version_entry in key_versions.iter().rev() {
                if kept_count < keep_min_versions {
                    kept_count += 1;
                    continue;
                }

                if *version_entry.key() < before_timestamp {
                    to_remove.push(*version_entry.key());
                }
            }

            // Remove old versions
            for version in to_remove {
                key_versions.remove(&version);
                removed_count += 1;
            }

            // Check if key has become empty
            if key_versions.is_empty() {
                empty_keys.push(key);
            }
        }

        // Remove empty keys from the main map
        for key in empty_keys {
            self.versions.remove(&key);
        }

        removed_count
    }

    pub fn get_all_versions(&self, key: &[u8]) -> Vec<(u64, Option<Vec<u8>>)> {
        if let Some(key_versions) = self.versions.get(key) {
            key_versions
                .value()
                .iter()
                .map(|entry| (*entry.key(), entry.value().value.clone()))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get total number of versions across all keys
    pub fn total_version_count(&self) -> usize {
        self.versions.iter().map(|entry| entry.value().len()).sum()
    }

    /// Get number of unique keys
    pub fn key_count(&self) -> usize {
        self.versions.len()
    }
}

impl Default for FixedVersionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cleanup_removes_versions() {
        let store = FixedVersionStore::new();

        // Add multiple versions
        for i in 0..100 {
            store.put(
                b"key1".to_vec(),
                Some(format!("value{}", i).into_bytes()),
                i as u64,
                i as u64,
            );
        }

        assert_eq!(store.total_version_count(), 100);

        // Cleanup versions older than 50
        let removed = store.cleanup_old_versions(50, 2);

        // Should keep only versions >= 50 or the minimum 2 versions
        assert!(removed > 0);
        assert!(store.total_version_count() < 100);
    }

    #[test]
    fn test_cleanup_removes_empty_keys() {
        let store = FixedVersionStore::new();

        // Add a key with one old version
        store.put(b"old_key".to_vec(), Some(b"value".to_vec()), 1, 1);

        assert_eq!(store.key_count(), 1);

        // Cleanup with no minimum versions
        store.cleanup_old_versions(1000, 0);

        // Key should be removed entirely
        assert_eq!(store.key_count(), 0);
    }
}
