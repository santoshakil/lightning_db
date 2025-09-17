use std::path::Path;
use std::sync::Arc;
use crate::performance::small_alloc::SmallAllocPool;
use std::time::Duration;
use parking_lot::RwLock;

use crate::{
    Database, Error, Result, LightningDbConfig, WalSyncMode, CompType,
    core::{
        btree::BPlusTree,
        lsm::{LSMConfig, LSMTree},
        storage::{
            page::{PageManager},
            page_wrappers::PageManagerWrapper,
        },
        transaction::{
            UnifiedTransactionManager,
            unified_manager::UnifiedVersionStore as VersionStore,
            version_cleanup::VersionCleanupThread,
        },
        wal::{UnifiedWalConfig, UnifiedWalSyncMode, UnifiedWriteAheadLog, UnifiedTransactionRecoveryState},
        index::IndexManager,
    },
    features::{
        statistics::MetricsCollector,
    },
    utils::{
        safety::consistency::ConsistencyManager,
        batching::auto_batcher::FastAutoBatcher,
    },
    performance::prefetch::{PrefetchConfig, PrefetchManager},
    encryption,
    monitoring,
    query_planner,
};

impl Database {
    pub fn create<P: AsRef<Path>>(path: P, config: LightningDbConfig) -> Result<Self> {
        let db_path = path.as_ref();
        std::fs::create_dir_all(db_path)?;

        // Use separate files/directories for different components
        let btree_path = db_path.join("btree.db");
        let lsm_path = db_path.join("lsm");
        let _wal_path = db_path.join("wal.log");

        let initial_size = config.page_size * 16; // Start with 16 pages

        // Initialize encryption manager if enabled (before creating page managers)
        let encryption_manager = if config.encryption_config.enabled {
            let manager = Arc::new(encryption::EncryptionManager::new(
                config.encryption_config.clone(),
            )?);
            Some(manager)
        } else {
            None
        };

        // Initialize quota manager if enabled
        let quota_manager = if config.quota_config.enabled {
            Some(Arc::new(
                crate::utils::quotas::QuotaManager::new(config.quota_config.clone())?,
            ))
        } else {
            None
        };

        // Create page manager
        let page_manager = PageManager::create(&btree_path, initial_size)?;
        let page_manager_arc = Arc::new(RwLock::new(page_manager));
        let page_manager_wrapper = PageManagerWrapper::from_arc(page_manager_arc.clone());

        // Wrap with encryption if enabled
        if let Some(ref _enc_manager) = encryption_manager {}

        // Create BPlusTree with the final wrapper
        let btree = BPlusTree::new_with_wrapper(page_manager_wrapper.clone())?;

        // Initialize UnifiedCache replacing legacy MemoryPool
        let unified_cache = if config.cache_size > 0 {
            use crate::performance::cache::unified_cache::UnifiedCacheConfig;
            let cache_config = UnifiedCacheConfig {
                capacity: config.cache_size as usize,
                num_segments: 16, // Optimize for concurrency
                enable_adaptive_sizing: true,
                enable_prefetching: true,
                eviction_batch_size: 32,
                enable_thread_local: true,
                adaptive_config: Default::default(),
            };
            Some(Arc::new(
                crate::performance::cache::UnifiedCache::with_config(cache_config),
            ))
        } else {
            None
        };

        // Optional LSM tree for write optimization
        let lsm_tree = if config.compression_enabled {
            let lsm_config = LSMConfig {
                compression_type: match config.compression_type {
                    0 => CompType::None,
                    #[cfg(feature = "zstd-compression")]
                    1 => CompType::Zstd,
                    #[cfg(not(feature = "zstd-compression"))]
                    1 => CompType::Snappy,
                    2 => CompType::LZ4,
                    3 => CompType::Snappy,
                    _ => CompType::LZ4,
                },
                ..Default::default()
            };
            Some(Arc::new(LSMTree::new(&lsm_path, lsm_config)?))
        } else {
            None
        };

        let version_store = Arc::new(VersionStore::new());

        // Use the unified transaction manager combining MVCC, optimizations, and safety
        let transaction_manager = UnifiedTransactionManager::new(config.max_active_transactions);

        let _optimized_transaction_manager: Option<Arc<UnifiedTransactionManager>> = None;

        // WAL for durability
        let unified_wal = if config.use_unified_wal {
            let wal_config = UnifiedWalConfig {
                sync_mode: match config.wal_sync_mode {
                    WalSyncMode::Sync => UnifiedWalSyncMode::Sync,
                    WalSyncMode::Async => UnifiedWalSyncMode::Async,
                    WalSyncMode::Periodic { interval_ms } => {
                        UnifiedWalSyncMode::Periodic { interval_ms }
                    }
                },
                group_commit_enabled: matches!(config.wal_sync_mode, WalSyncMode::Sync),
                max_segment_size: 64 * 1024 * 1024, // 64MB segments
                enable_compression: config.compression_enabled,
                strict_ordering: false,
            };
            Some(UnifiedWriteAheadLog::create_with_config(
                db_path.join("wal"),
                wal_config,
            )?)
        } else {
            Some(UnifiedWriteAheadLog::create_with_config(
                db_path.join("wal"),
                UnifiedWalConfig::default(),
            )?)
        };

        // Optional prefetch manager for performance
        let prefetch_manager = if config.prefetch_enabled {
            let prefetch_config = PrefetchConfig {
                enabled: true,
                max_prefetch_distance: config.prefetch_distance,
                worker_threads: config.prefetch_workers,
                ..Default::default()
            };
            let mut manager = PrefetchManager::new(prefetch_config);

            // Set up page cache adapter
            let page_cache_adapter = Arc::new(crate::core::storage::PageCacheAdapter::new(
                page_manager_arc.clone(),
            ));
            manager.set_page_cache(page_cache_adapter);

            let manager = Arc::new(manager);
            manager.start()?;
            Some(manager)
        } else {
            None
        };

        // Initialize metrics collector
        let metrics_collector = Arc::new(MetricsCollector::new(
            Duration::from_secs(10), // Collect metrics every 10 seconds
            360,                     // Keep 1 hour of history (360 * 10s = 3600s)
        ));
        // Only start background collection if we have features enabled that need it
        if config.cache_size > 0 || config.compression_enabled || config.prefetch_enabled {
            metrics_collector.start();
        }

        // Register components
        if lsm_tree.is_some() {
            metrics_collector.register_component("lsm_tree");
        }
        if prefetch_manager.is_some() {
            metrics_collector.register_component("prefetch");
        }

        // Initialize consistency manager
        let consistency_manager =
            Arc::new(ConsistencyManager::new(config.consistency_config.clone()));

        // Initialize index manager
        let index_manager = Arc::new(IndexManager::new(page_manager_wrapper.clone()));

        // Initialize query planner
        let query_planner = Arc::new(RwLock::new(query_planner::QueryPlanner::new(
            query_planner::planner::PlannerConfig::default(),
        )));

        // Create write buffer for B+Tree if LSM is disabled
        let btree_arc = Arc::new(RwLock::new(btree));
        let btree_write_buffer = if lsm_tree.is_none() && !config.use_optimized_transactions {
            Some(Arc::new(crate::core::btree::write_buffer::BTreeWriteBuffer::new(
                btree_arc.clone(),
                1000, // Buffer up to 1000 writes
            )))
        } else {
            None
        };

        // Start version cleanup thread to prevent memory leaks
        let version_cleanup_thread = Arc::new(VersionCleanupThread::new(
            version_store.clone(),
            Duration::from_secs(30),  // cleanup every 30 seconds
            Duration::from_secs(300), // keep versions for 5 minutes
        ));
        let _cleanup_handle = version_cleanup_thread.clone().start();

        // Initialize isolation manager with default configuration
        let isolation_manager =
            Arc::new(crate::features::transactions::isolation::IsolationManager::new());

        Ok(Self {
            path: db_path.to_path_buf(),
            page_manager: page_manager_wrapper,
            btree: btree_arc,
            transaction_manager,
            version_store,
            unified_cache,
            lsm_tree,
            unified_wal,
            prefetch_manager,
            metrics_collector,
            consistency_manager,
            write_batcher: None,
            btree_write_buffer,
            index_manager,
            query_planner,
            production_monitor: Arc::new(monitoring::production_hooks::ProductionMonitor::new()),
            _version_cleanup_thread: Some(version_cleanup_thread),
            encryption_manager,
            quota_manager,
            compaction_manager: None, // Will be initialized when first needed
            isolation_manager,
            small_alloc_pool: Arc::new(SmallAllocPool::new()),
            _config: config,
        })
    }

    /// Create a temporary database for testing
    pub fn create_temp() -> Result<Self> {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| Error::Io(format!("System time error: {}", e)))?
            .as_nanos();

        let temp_dir = std::env::temp_dir().join(format!("lightning_db_test_{}", nanos));
        Self::create(temp_dir, LightningDbConfig::default())
    }

    pub fn open<P: AsRef<Path>>(path: P, config: LightningDbConfig) -> Result<Self> {
        let db_path = path.as_ref();

        // Check if the directory exists AND the database files exist
        let btree_path = db_path.join("btree.db");

        if db_path.exists() && btree_path.exists() {
            let lsm_path = db_path.join("lsm");

            // Initialize encryption manager if enabled (before creating page managers)
            let encryption_manager = if config.encryption_config.enabled {
                let manager = Arc::new(encryption::EncryptionManager::new(
                    config.encryption_config.clone(),
                )?);
                // Implementation pending master key initialization from environment or key management system
                // For now, we'll leave the key uninitialized until explicitly set
                Some(manager)
            } else {
                None
            };

            // Initialize quota manager if enabled
            let quota_manager = if config.quota_config.enabled {
                Some(Arc::new(
                    crate::utils::quotas::QuotaManager::new(
                        config.quota_config.clone(),
                    )?,
                ))
            } else {
                None
            };

            // Open page manager
            let page_manager = PageManager::open(&btree_path)?;
            let page_manager_arc = Arc::new(RwLock::new(page_manager));
            let page_manager_wrapper = PageManagerWrapper::from_arc(page_manager_arc.clone());

            // Wrap with encryption if enabled
            if let Some(ref _enc_manager) = encryption_manager {
                // Encryption wrapping would happen here if needed
                // For now we keep the wrapper as-is
            }

            // Create BPlusTree with the final wrapper
            let mut btree =
                BPlusTree::from_existing_with_wrapper(page_manager_wrapper.clone(), 1, 1);

            let lsm_tree = if config.compression_enabled {
                let lsm_config = LSMConfig {
                    compression_type: match config.compression_type {
                        0 => CompType::None,
                        #[cfg(feature = "zstd-compression")]
                        1 => CompType::Zstd,
                        #[cfg(not(feature = "zstd-compression"))]
                        1 => CompType::Snappy,
                        2 => CompType::LZ4,
                        3 => CompType::Snappy,
                        _ => CompType::LZ4,
                    },
                    ..Default::default()
                };
                Some(Arc::new(LSMTree::new(&lsm_path, lsm_config)?))
            } else {
                None
            };

            let version_store = Arc::new(VersionStore::new());

            // Use the unified transaction manager combining MVCC, optimizations, and safety
            let transaction_manager =
                UnifiedTransactionManager::new(config.max_active_transactions);

            let _optimized_transaction_manager: Option<Arc<UnifiedTransactionManager>> = None;

            // WAL recovery for existing database
            let unified_wal = if config.use_unified_wal {
                // Use unified WAL with better recovery
                let wal_dir = db_path.join("wal");
                if wal_dir.exists() {
                    let wal_config = UnifiedWalConfig {
                        sync_mode: match config.wal_sync_mode {
                            WalSyncMode::Sync => UnifiedWalSyncMode::Sync,
                            WalSyncMode::Async => UnifiedWalSyncMode::Async,
                            WalSyncMode::Periodic { interval_ms } => {
                                UnifiedWalSyncMode::Periodic { interval_ms }
                            }
                        },
                        group_commit_enabled: matches!(config.wal_sync_mode, WalSyncMode::Sync),
                        max_segment_size: 64 * 1024 * 1024,
                        enable_compression: config.compression_enabled,
                        strict_ordering: false,
                    };
                    let unified_wal_instance =
                        UnifiedWriteAheadLog::open_with_config(&wal_dir, wal_config)?;

                    // Replay with transaction awareness
                    {
                        let lsm_ref = &lsm_tree;
                        let _version_store_ref = &version_store;
                        let mut operations_recovered = 0;

                        unified_wal_instance.recover_with_progress(
                            |operation, tx_state| {
                                match operation {
                                    crate::core::wal::WALOperation::Put { key, value } => {
                                        // Only apply if transaction is committed
                                        if matches!(
                                            tx_state,
                                            UnifiedTransactionRecoveryState::Committed
                                        ) {
                                            if let Some(ref lsm) = lsm_ref {
                                                lsm.insert(key.to_vec(), value.to_vec())?;
                                            } else {
                                                // Write directly to B+Tree when there's no LSM
                                                btree.insert(key, value)?;
                                            }
                                            operations_recovered += 1;
                                        }
                                    }
                                    crate::core::wal::WALOperation::Delete { key } => {
                                        // Only apply if transaction is committed
                                        if matches!(
                                            tx_state,
                                            UnifiedTransactionRecoveryState::Committed
                                        ) {
                                            if let Some(ref lsm) = lsm_ref {
                                                lsm.delete(key)?;
                                            } else {
                                                // Delete directly from B+Tree when there's no LSM
                                                btree.delete(key)?;
                                            }
                                            operations_recovered += 1;
                                        }
                                    }
                                    _ => {}
                                }
                                Ok(())
                            },
                            |current, _total| {
                                if current % 1000 == 0 {
                                    tracing::info!("Recovery progress: LSN {}", current);
                                }
                            },
                        )?;

                        tracing::info!(
                            "WAL recovery complete: {} operations recovered",
                            operations_recovered
                        );

                        // Sync page manager to ensure recovered data is persisted
                        if operations_recovered > 0 && lsm_ref.is_none() {
                            page_manager_wrapper.sync()?;
                        }
                    }

                    Some(unified_wal_instance)
                } else {
                    let wal_config = UnifiedWalConfig {
                        sync_mode: match config.wal_sync_mode {
                            WalSyncMode::Sync => UnifiedWalSyncMode::Sync,
                            WalSyncMode::Async => UnifiedWalSyncMode::Async,
                            WalSyncMode::Periodic { interval_ms } => {
                                UnifiedWalSyncMode::Periodic { interval_ms }
                            }
                        },
                        group_commit_enabled: matches!(config.wal_sync_mode, WalSyncMode::Sync),
                        max_segment_size: 64 * 1024 * 1024,
                        enable_compression: config.compression_enabled,
                        strict_ordering: false,
                    };
                    let wal = UnifiedWriteAheadLog::create_with_config(&wal_dir, wal_config)?;
                    Some(wal)
                }
            } else {
                // Always use UnifiedWriteAheadLog for consistency
                let wal_dir = db_path.join("wal");
                let wal_config = UnifiedWalConfig::default();
                if wal_dir.exists() {
                    Some(UnifiedWriteAheadLog::open_with_config(
                        &wal_dir, wal_config,
                    )?)
                } else {
                    Some(UnifiedWriteAheadLog::create_with_config(
                        &wal_dir, wal_config,
                    )?)
                }
            };

            // Optional prefetch manager for performance
            let prefetch_manager = if config.prefetch_enabled {
                let prefetch_config = PrefetchConfig {
                    enabled: true,
                    max_prefetch_distance: config.prefetch_distance,
                    worker_threads: config.prefetch_workers,
                    ..Default::default()
                };
                let mut manager = PrefetchManager::new(prefetch_config);

                // Set up page cache adapter
                let page_cache_adapter = Arc::new(crate::core::storage::PageCacheAdapter::new(
                    page_manager_arc.clone(),
                ));
                manager.set_page_cache(page_cache_adapter);

                let manager = Arc::new(manager);
                manager.start()?;
                Some(manager)
            } else {
                None
            };

            // Initialize metrics collector
            let metrics_collector = Arc::new(MetricsCollector::new(
                Duration::from_secs(10), // Collect metrics every 10 seconds
                360,                     // Keep 1 hour of history (360 * 10s = 3600s)
            ));

            // Only start background collection if we have features enabled that need it
            if config.cache_size > 0 || config.compression_enabled || config.prefetch_enabled {
                metrics_collector.start();
            }

            // Register components
            if lsm_tree.is_some() {
                metrics_collector.register_component("lsm_tree");
            }
            if prefetch_manager.is_some() {
                metrics_collector.register_component("prefetch");
            }

            // Initialize consistency manager
            let consistency_manager =
                Arc::new(ConsistencyManager::new(config.consistency_config.clone()));

            // Initialize index manager
            let index_manager = Arc::new(IndexManager::new(page_manager_wrapper.clone()));

            // Initialize query planner
            let query_planner = Arc::new(RwLock::new(query_planner::QueryPlanner::new(
                query_planner::planner::PlannerConfig::default(),
            )));

            // Create write buffer for B+Tree if LSM is disabled
            let btree_arc = Arc::new(RwLock::new(btree));
            let btree_write_buffer = if lsm_tree.is_none() && !config.use_optimized_transactions {
                Some(Arc::new(crate::core::btree::write_buffer::BTreeWriteBuffer::new(
                    btree_arc.clone(),
                    1000, // Buffer up to 1000 writes
                )))
            } else {
                None
            };

            // Start version cleanup thread to prevent memory leaks
            let version_cleanup_thread = Arc::new(VersionCleanupThread::new(
                version_store.clone(),
                Duration::from_secs(30),  // cleanup every 30 seconds
                Duration::from_secs(300), // keep versions for 5 minutes
            ));
            let _cleanup_handle = version_cleanup_thread.clone().start();

            // Initialize isolation manager with default configuration
            let isolation_manager =
                Arc::new(crate::features::transactions::isolation::IsolationManager::new());

            // Initialize UnifiedCache replacing legacy MemoryPool
            let unified_cache = if config.cache_size > 0 {
                use crate::performance::cache::unified_cache::UnifiedCacheConfig;
                let cache_config = UnifiedCacheConfig {
                    capacity: config.cache_size as usize,
                    num_segments: 16, // Optimize for concurrency
                    enable_adaptive_sizing: true,
                    enable_prefetching: true,
                    eviction_batch_size: 32,
                    enable_thread_local: true,
                    adaptive_config: Default::default(),
                };
                Some(Arc::new(
                    crate::performance::cache::UnifiedCache::with_config(cache_config),
                ))
            } else {
                None
            };

            Ok(Self {
                path: db_path.to_path_buf(),
                page_manager: page_manager_wrapper,
                btree: btree_arc,
                transaction_manager,
                version_store,
                unified_cache,
                lsm_tree,
                unified_wal,
                prefetch_manager,
                metrics_collector,
                consistency_manager,
                write_batcher: None,
                btree_write_buffer,
                index_manager,
                query_planner,
                production_monitor: Arc::new(monitoring::production_hooks::ProductionMonitor::new()),
                _version_cleanup_thread: Some(version_cleanup_thread),
                encryption_manager,
                quota_manager,
                compaction_manager: None, // Will be initialized when first needed
                isolation_manager,
                small_alloc_pool: Arc::new(SmallAllocPool::new()),
                _config: config,
            })
        } else {
            Self::create(path, config)
        }
    }

    /// Start the version cleanup thread for automatic garbage collection
    pub fn start_version_cleanup(db: Arc<Self>) -> Arc<Self> {
        // Create and start the version cleanup thread
        let cleanup_thread = Arc::new(VersionCleanupThread::new(
            db.version_store.clone(),
            Duration::from_secs(30),  // Cleanup every 30 seconds
            Duration::from_secs(300), // Keep versions for 5 minutes
        ));

        // Start the cleanup thread
        let _handle = cleanup_thread.clone().start();

        db
    }

    pub fn enable_write_batching(db: Arc<Self>) -> Arc<Self> {
        let _batcher = FastAutoBatcher::new(
            db.clone(),
            db._config.write_batch_size,
            10, // 10ms max delay
        );

        db
    }

    pub fn create_batcher(db: Arc<Self>, batch_size: Option<usize>, max_delay_ms: Option<u64>) -> Arc<FastAutoBatcher> {
        let batch_size = batch_size.unwrap_or(1000);
        let max_delay = max_delay_ms.unwrap_or_else(|| {
            match &db._config.wal_sync_mode {
                WalSyncMode::Sync => 50,
                _ => 10,
            }
        });
        FastAutoBatcher::new(db, batch_size, max_delay)
    }
}