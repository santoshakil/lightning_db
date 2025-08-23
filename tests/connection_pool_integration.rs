use lightning_db::features::connection_pool::{
    ConnectionPool, ConnectionPoolConfig, LoadBalancingStrategy
};
use std::sync::Arc;
use std::time::Duration;
use tokio;

#[tokio::test]
async fn test_connection_pool_basic_operations() {
    let config = ConnectionPoolConfig {
        min_connections: 2,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(3600),
        health_check_interval: Duration::from_secs(10),
        load_balancing: LoadBalancingStrategy::RoundRobin,
    };
    
    let pool = ConnectionPool::new(config).await.unwrap();
    
    let conn1 = pool.acquire().await.unwrap();
    assert!(conn1.is_healthy().await);
    
    let conn2 = pool.acquire().await.unwrap();
    assert!(conn2.is_healthy().await);
    
    let stats = pool.get_stats();
    assert_eq!(stats.active_connections, 2);
    assert_eq!(stats.idle_connections, 0);
}

#[tokio::test]
async fn test_connection_pool_max_connections() {
    let config = ConnectionPoolConfig {
        min_connections: 1,
        max_connections: 3,
        connection_timeout: Duration::from_millis(100),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(3600),
        health_check_interval: Duration::from_secs(10),
        load_balancing: LoadBalancingStrategy::LeastRecentlyUsed,
    };
    
    let pool = ConnectionPool::new(config).await.unwrap();
    
    let mut connections = Vec::new();
    for _ in 0..3 {
        connections.push(pool.acquire().await.unwrap());
    }
    
    let timeout_result = tokio::time::timeout(
        Duration::from_millis(200),
        pool.acquire()
    ).await;
    
    assert!(timeout_result.is_err());
    
    drop(connections[0].clone());
    
    let conn = tokio::time::timeout(
        Duration::from_millis(200),
        pool.acquire()
    ).await.unwrap().unwrap();
    
    assert!(conn.is_healthy().await);
}

#[tokio::test]
async fn test_connection_pool_health_checks() {
    let config = ConnectionPoolConfig {
        min_connections: 2,
        max_connections: 5,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(3600),
        health_check_interval: Duration::from_millis(100),
        load_balancing: LoadBalancingStrategy::Random,
    };
    
    let pool = ConnectionPool::new(config).await.unwrap();
    
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    let stats = pool.get_stats();
    assert!(stats.health_checks_performed > 0);
}

#[tokio::test]
async fn test_connection_pool_weighted_load_balancing() {
    let config = ConnectionPoolConfig {
        min_connections: 4,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(3600),
        health_check_interval: Duration::from_secs(10),
        load_balancing: LoadBalancingStrategy::Weighted(vec![
            (0, 40),
            (1, 30),
            (2, 20),
            (3, 10),
        ]),
    };
    
    let pool = ConnectionPool::new(config).await.unwrap();
    
    for _ in 0..100 {
        let conn = pool.acquire().await.unwrap();
        assert!(conn.is_healthy().await);
    }
    
    let stats = pool.get_stats();
    assert!(stats.total_acquired > 100);
}

#[tokio::test]
async fn test_connection_pool_concurrent_access() {
    let config = ConnectionPoolConfig {
        min_connections: 5,
        max_connections: 20,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(3600),
        health_check_interval: Duration::from_secs(10),
        load_balancing: LoadBalancingStrategy::RoundRobin,
    };
    
    let pool = Arc::new(ConnectionPool::new(config).await.unwrap());
    
    let mut handles = vec![];
    for _ in 0..10 {
        let pool_clone = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                let conn = pool_clone.acquire().await.unwrap();
                assert!(conn.is_healthy().await);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await.unwrap();
    }
    
    let stats = pool.get_stats();
    assert!(stats.total_acquired >= 100);
}

#[tokio::test]
async fn test_connection_pool_graceful_shutdown() {
    let config = ConnectionPoolConfig {
        min_connections: 3,
        max_connections: 10,
        connection_timeout: Duration::from_secs(5),
        idle_timeout: Duration::from_secs(60),
        max_lifetime: Duration::from_secs(3600),
        health_check_interval: Duration::from_secs(10),
        load_balancing: LoadBalancingStrategy::RoundRobin,
    };
    
    let pool = ConnectionPool::new(config).await.unwrap();
    
    let conn1 = pool.acquire().await.unwrap();
    let conn2 = pool.acquire().await.unwrap();
    
    pool.shutdown().await;
    
    let stats = pool.get_stats();
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.idle_connections, 0);
    
    let result = pool.acquire().await;
    assert!(result.is_err());
}