use crate::core::error::Result;
use crate::{Database, DatabaseStats, LightningDbConfig};
use std::path::Path;
use std::sync::Arc;
use tokio::task;

/// Async wrapper around the Lightning DB database
pub struct AsyncDatabase {
    inner: Arc<Database>,
}

impl AsyncDatabase {
    pub async fn create<P: AsRef<Path> + Send + 'static>(
        path: P,
        config: LightningDbConfig,
    ) -> Result<Self> {
        let db = task::spawn_blocking(move || Database::create(path, config))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))??;

        Ok(Self {
            inner: Arc::new(db),
        })
    }

    pub async fn open<P: AsRef<Path> + Send + 'static>(
        path: P,
        config: LightningDbConfig,
    ) -> Result<Self> {
        let db = task::spawn_blocking(move || Database::open(path, config))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))??;

        Ok(Self {
            inner: Arc::new(db),
        })
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.put(&key, &value))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.get(&key))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn delete(&self, key: Vec<u8>) -> Result<bool> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.delete(&key))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn begin_transaction(&self) -> Result<u64> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.begin_transaction())
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn commit_transaction(&self, tx_id: u64) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.commit_transaction(tx_id))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn abort_transaction(&self, tx_id: u64) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.abort_transaction(tx_id))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn put_tx(&self, tx_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.put_tx(tx_id, &key, &value))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn get_tx(&self, tx_id: u64, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.get_tx(tx_id, &key))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn delete_tx(&self, tx_id: u64, key: Vec<u8>) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.delete_tx(tx_id, &key))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn put_batch(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.put_batch(&pairs))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn get_batch(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.get_batch(&keys))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn delete_batch(&self, keys: Vec<Vec<u8>>) -> Result<Vec<bool>> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.delete_batch(&keys))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn sync(&self) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.sync())
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn stats(&self) -> Result<DatabaseStats> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || Ok(db.stats()))
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn flush_lsm(&self) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.flush_lsm())
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub async fn compact_lsm(&self) -> Result<()> {
        let db = Arc::clone(&self.inner);
        task::spawn_blocking(move || db.compact_lsm())
            .await
            .map_err(|e| crate::core::error::Error::Io(e.to_string()))?
    }

    pub fn cache_stats(&self) -> Option<String> {
        let stats = self.inner.cache_stats();
        let hits = stats.hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = stats.misses.load(std::sync::atomic::Ordering::Relaxed);
        let total = hits + misses;
        let hit_ratio = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
        Some(format!("hits: {}, misses: {}, hit_ratio: {:.2}%",
                     hits,
                     misses,
                     hit_ratio * 100.0))
    }

    pub fn lsm_stats(&self) -> Option<crate::core::lsm::LSMStats> {
        self.inner.lsm_stats()
    }

    /// Pipeline multiple operations for better throughput
    pub async fn pipeline<F, Fut>(&self, operations: Vec<F>) -> Vec<Result<()>>
    where
        F: FnOnce(Arc<Database>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        let futures: Vec<_> = operations
            .into_iter()
            .map(|op| {
                let db = Arc::clone(&self.inner);
                tokio::spawn(async move { op(db).await })
            })
            .collect();

        let mut results = Vec::new();
        for future in futures {
            match future.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(crate::core::error::Error::Io(e.to_string()))),
            }
        }
        results
    }

    /// Concurrent read operations
    pub async fn concurrent_gets(&self, keys: Vec<Vec<u8>>) -> Vec<Result<Option<Vec<u8>>>> {
        let futures: Vec<_> = keys
            .into_iter()
            .map(|key| {
                let db = Arc::clone(&self.inner);
                tokio::spawn(async move { db.get(&key) })
            })
            .collect();

        let mut results = Vec::new();
        for future in futures {
            match future.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(crate::core::error::Error::Io(e.to_string()))),
            }
        }
        results
    }

    /// Concurrent write operations
    pub async fn concurrent_puts(&self, pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<Result<()>> {
        let futures: Vec<_> = pairs
            .into_iter()
            .map(|(key, value)| {
                let db = Arc::clone(&self.inner);
                tokio::spawn(async move { db.put(&key, &value) })
            })
            .collect();

        let mut results = Vec::new();
        for future in futures {
            match future.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(crate::core::error::Error::Io(e.to_string()))),
            }
        }
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio;

    #[tokio::test]
    async fn test_async_basic_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("async_test.db");

        let config = LightningDbConfig {
            use_optimized_transactions: false, // Disable for tests
            ..Default::default()
        };
        let db = AsyncDatabase::create(db_path, config).await.unwrap();

        // Test put and get
        db.put(b"test_key".to_vec(), b"test_value".to_vec())
            .await
            .unwrap();

        let result = db.get(b"test_key".to_vec()).await.unwrap();
        assert_eq!(result.unwrap(), b"test_value");

        // Test delete
        db.delete(b"test_key".to_vec()).await.unwrap();
        let result = db.get(b"test_key".to_vec()).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_async_transactions() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("async_tx_test.db");

        let config = LightningDbConfig {
            use_optimized_transactions: false, // Disable for tests
            ..Default::default()
        };
        let db = AsyncDatabase::create(db_path, config).await.unwrap();

        let tx_id = db.begin_transaction().await.unwrap();
        db.put_tx(tx_id, b"tx_key".to_vec(), b"tx_value".to_vec())
            .await
            .unwrap();

        let result = db.get_tx(tx_id, b"tx_key".to_vec()).await.unwrap();
        assert_eq!(result.unwrap(), b"tx_value");

        db.commit_transaction(tx_id).await.unwrap();

        let result = db.get(b"tx_key".to_vec()).await.unwrap();
        assert_eq!(result.unwrap(), b"tx_value");
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("concurrent_test.db");

        let config = LightningDbConfig {
            use_optimized_transactions: false, // Disable for tests
            ..Default::default()
        };
        let db = AsyncDatabase::create(db_path, config).await.unwrap();

        // Test concurrent puts
        let pairs = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        let put_results = db.concurrent_puts(pairs).await;
        assert!(put_results.iter().all(|r| r.is_ok()));

        // Test concurrent gets
        let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
        let get_results = db.concurrent_gets(keys).await;
        assert!(get_results
            .iter()
            .all(|r| r.is_ok() && r.as_ref().unwrap().is_some()));
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("batch_async_test.db");

        let config = LightningDbConfig {
            use_optimized_transactions: false, // Disable for tests
            ..Default::default()
        };
        let db = AsyncDatabase::create(db_path, config).await.unwrap();

        // Test batch put
        let pairs = vec![
            (b"batch_key_1".to_vec(), b"batch_value_1".to_vec()),
            (b"batch_key_2".to_vec(), b"batch_value_2".to_vec()),
            (b"batch_key_3".to_vec(), b"batch_value_3".to_vec()),
        ];
        db.put_batch(pairs).await.unwrap();

        // Test batch get
        let keys = vec![
            b"batch_key_1".to_vec(),
            b"batch_key_2".to_vec(),
            b"batch_key_3".to_vec(),
        ];
        let results = db.get_batch(keys.clone()).await.unwrap();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_some()));

        // Test batch delete
        let delete_results = db.delete_batch(keys.clone()).await.unwrap();
        assert_eq!(delete_results.len(), 3);
        assert!(delete_results.iter().all(|&r| r));

        // Verify deletions
        let final_results = db.get_batch(keys).await.unwrap();
        assert!(final_results.iter().all(|r| r.is_none()));
    }
}
