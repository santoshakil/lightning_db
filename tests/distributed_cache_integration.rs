use lightning_db::features::distributed_cache::{
    DistributedCache, DistributedCacheConfig, ReplicationStrategy,
    NodeInfo, NodeStatus
};
use std::sync::Arc;
use std::time::Duration;
use std::net::SocketAddr;
use tokio;

#[tokio::test]
async fn test_distributed_cache_basic_operations() {
    let config = DistributedCacheConfig {
        capacity: 1000,
        replication: ReplicationStrategy::None,
        replication_factor: 0,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    cache.put(b"key1", b"value1", None).await.unwrap();
    
    let value = cache.get(b"key1").await.unwrap();
    assert_eq!(value, Some(b"value1".to_vec()));
    
    cache.delete(b"key1").await.unwrap();
    
    let value = cache.get(b"key1").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_distributed_cache_ttl() {
    let config = DistributedCacheConfig {
        capacity: 100,
        replication: ReplicationStrategy::None,
        replication_factor: 0,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    cache.put(b"ttl_key", b"ttl_value", Some(Duration::from_millis(100))).await.unwrap();
    
    let value = cache.get(b"ttl_key").await.unwrap();
    assert_eq!(value, Some(b"ttl_value".to_vec()));
    
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    let value = cache.get(b"ttl_key").await.unwrap();
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_distributed_cache_replication() {
    let config = DistributedCacheConfig {
        capacity: 1000,
        replication: ReplicationStrategy::PrimaryBackup,
        replication_factor: 2,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(1),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config.clone()).await.unwrap();
    
    let node1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let node2: SocketAddr = "127.0.0.1:9002".parse().unwrap();
    
    cache.add_node(NodeInfo {
        id: "node1".to_string(),
        address: node1,
        status: NodeStatus::Active,
        capacity: 500,
        load: 0.0,
    }).await.unwrap();
    
    cache.add_node(NodeInfo {
        id: "node2".to_string(),
        address: node2,
        status: NodeStatus::Active,
        capacity: 500,
        load: 0.0,
    }).await.unwrap();
    
    cache.put(b"replicated_key", b"replicated_value", None).await.unwrap();
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    let stats = cache.get_stats();
    assert!(stats.replicated_entries > 0);
}

#[tokio::test]
async fn test_distributed_cache_consistent_hashing() {
    let config = DistributedCacheConfig {
        capacity: 10000,
        replication: ReplicationStrategy::Chain,
        replication_factor: 3,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    for i in 1..=5 {
        let addr: SocketAddr = format!("127.0.0.1:900{}", i).parse().unwrap();
        cache.add_node(NodeInfo {
            id: format!("node{}", i),
            address: addr,
            status: NodeStatus::Active,
            capacity: 2000,
            load: 0.0,
        }).await.unwrap();
    }
    
    let mut key_distributions = vec![0; 5];
    
    for i in 0..1000 {
        let key = format!("key_{}", i);
        cache.put(key.as_bytes(), b"value", None).await.unwrap();
    }
    
    let stats = cache.get_distribution_stats();
    for (node_id, count) in stats.iter() {
        assert!(*count > 100 && *count < 300);
    }
}

#[tokio::test]
async fn test_distributed_cache_quorum_consistency() {
    let config = DistributedCacheConfig {
        capacity: 1000,
        replication: ReplicationStrategy::Quorum,
        replication_factor: 3,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(1),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    for i in 1..=3 {
        let addr: SocketAddr = format!("127.0.0.1:901{}", i).parse().unwrap();
        cache.add_node(NodeInfo {
            id: format!("node{}", i),
            address: addr,
            status: NodeStatus::Active,
            capacity: 500,
            load: 0.0,
        }).await.unwrap();
    }
    
    cache.put(b"quorum_key", b"quorum_value", None).await.unwrap();
    
    let value = cache.get(b"quorum_key").await.unwrap();
    assert_eq!(value, Some(b"quorum_value".to_vec()));
    
    cache.remove_node("node3").await.unwrap();
    
    let value = cache.get(b"quorum_key").await.unwrap();
    assert_eq!(value, Some(b"quorum_value".to_vec()));
}

#[tokio::test]
async fn test_distributed_cache_concurrent_operations() {
    let config = DistributedCacheConfig {
        capacity: 10000,
        replication: ReplicationStrategy::Primary,
        replication_factor: 1,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = Arc::new(DistributedCache::new(config).await.unwrap());
    
    let mut handles = vec![];
    
    for thread_id in 0..10 {
        let cache_clone = Arc::clone(&cache);
        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key = format!("thread_{}_key_{}", thread_id, i);
                let value = format!("thread_{}_value_{}", thread_id, i);
                cache_clone.put(key.as_bytes(), value.as_bytes(), None).await.unwrap();
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    for thread_id in 0..10 {
        for i in 0..100 {
            let key = format!("thread_{}_key_{}", thread_id, i);
            let expected_value = format!("thread_{}_value_{}", thread_id, i);
            let value = cache.get(key.as_bytes()).await.unwrap();
            assert_eq!(value, Some(expected_value.into_bytes()));
        }
    }
}

#[tokio::test]
async fn test_distributed_cache_compression() {
    let config = DistributedCacheConfig {
        capacity: 1000,
        replication: ReplicationStrategy::None,
        replication_factor: 0,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: true,
        compression_threshold: 100,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    let large_value = vec![b'A'; 1000];
    cache.put(b"compressed_key", &large_value, None).await.unwrap();
    
    let value = cache.get(b"compressed_key").await.unwrap();
    assert_eq!(value, Some(large_value));
    
    let stats = cache.get_stats();
    assert!(stats.compressed_entries > 0);
    assert!(stats.compression_ratio > 1.0);
}

#[tokio::test]
async fn test_distributed_cache_eviction() {
    let config = DistributedCacheConfig {
        capacity: 10,
        replication: ReplicationStrategy::None,
        replication_factor: 0,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(10),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    for i in 0..20 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        cache.put(key.as_bytes(), value.as_bytes(), None).await.unwrap();
    }
    
    let stats = cache.get_stats();
    assert_eq!(stats.total_entries, 10);
    assert!(stats.evictions > 0);
    
    let first_key_value = cache.get(b"key_0").await.unwrap();
    assert_eq!(first_key_value, None);
}

#[tokio::test]
async fn test_distributed_cache_invalidation() {
    let config = DistributedCacheConfig {
        capacity: 1000,
        replication: ReplicationStrategy::PrimaryBackup,
        replication_factor: 2,
        virtual_nodes: 150,
        sync_interval: Duration::from_secs(1),
        node_timeout: Duration::from_secs(30),
        compression_enabled: false,
        compression_threshold: 1024,
    };
    
    let cache = DistributedCache::new(config).await.unwrap();
    
    cache.put(b"invalidate_key", b"value", None).await.unwrap();
    
    cache.invalidate(b"invalidate_key").await.unwrap();
    
    let value = cache.get(b"invalidate_key").await.unwrap();
    assert_eq!(value, None);
    
    cache.invalidate_pattern(b"invalidate_*").await.unwrap();
    
    let stats = cache.get_stats();
    assert!(stats.invalidations > 0);
}