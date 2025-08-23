use crate::{Database, Result};
use super::{
    detector::{CorruptionDetector, DetectionResult, CorruptionType},
    validator::DataValidator,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, Instant};
use tokio::sync::{RwLock, Mutex, mpsc};
use tokio::time;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanConfig {
    pub batch_size: usize,
    pub parallel: bool,
    pub deep_validation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    pub scan_id: u64,
    pub start_time: SystemTime,
    pub end_time: Option<SystemTime>,
    pub pages_scanned: u64,
    pub corruptions: Vec<DetectionResult>,
    pub scan_type: ScanType,
    pub duration: Option<Duration>,
    pub scan_progress: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ScanType {
    Quick,
    Full,
    Background,
    Targeted,
}

impl ScanResult {
    pub fn has_corruption(&self) -> bool {
        !self.corruptions.is_empty()
    }

    pub fn corruption_count(&self) -> usize {
        self.corruptions.len()
    }

    pub fn critical_corruptions(&self) -> Vec<&DetectionResult> {
        self.corruptions.iter()
            .filter(|c| matches!(c.severity, super::detector::CorruptionSeverity::Critical))
            .collect()
    }
}

pub struct IntegrityScanner {
    database: Arc<Database>,
    detector: Arc<CorruptionDetector>,
    validator: Arc<DataValidator>,
    config: ScanConfig,
    scan_counter: Arc<Mutex<u64>>,
    background_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    scan_control: Arc<RwLock<ScanControl>>,
    scan_progress: Arc<RwLock<ScanProgress>>,
}

#[derive(Debug, Default)]
struct ScanControl {
    should_stop: bool,
    current_scan_id: Option<u64>,
    scan_interval: Duration,
}

#[derive(Debug, Default)]
struct ScanProgress {
    current_page: u64,
    total_pages: u64,
    corruptions_found: usize,
    scan_start: Option<Instant>,
}

impl IntegrityScanner {
    pub fn new(
        database: Arc<Database>,
        detector: Arc<CorruptionDetector>,
        validator: Arc<DataValidator>,
        config: ScanConfig,
    ) -> Self {
        Self {
            database,
            detector,
            validator,
            config,
            scan_counter: Arc::new(Mutex::new(0)),
            background_handle: Arc::new(RwLock::new(None)),
            scan_control: Arc::new(RwLock::new(ScanControl {
                should_stop: false,
                current_scan_id: None,
                scan_interval: Duration::from_secs(3600), // Default 1 hour
            })),
            scan_progress: Arc::new(RwLock::new(ScanProgress::default())),
        }
    }

    pub async fn start_background_scan(&self, interval: Duration) -> Result<()> {
        // Stop existing background scan if running
        self.stop_background_scan().await?;

        // Update scan control
        {
            let mut control = self.scan_control.write().await;
            control.should_stop = false;
            control.scan_interval = interval;
        }

        // Start background task
        let scanner = self.clone_for_background();
        let handle = tokio::spawn(async move {
            scanner.background_scan_loop().await;
        });

        *self.background_handle.write().await = Some(handle);
        Ok(())
    }

    pub async fn stop_background_scan(&self) -> Result<()> {
        // Signal stop
        self.scan_control.write().await.should_stop = true;

        // Wait for background task to finish
        if let Some(handle) = self.background_handle.write().await.take() {
            handle.abort();
        }

        Ok(())
    }

    pub async fn full_scan(&self) -> Result<ScanResult> {
        let scan_id = self.get_next_scan_id().await;
        let start_time = SystemTime::now();
        let scan_start = Instant::now();

        // Initialize progress tracking
        {
            let mut progress = self.scan_progress.write().await;
            progress.current_page = 0;
            progress.total_pages = self.get_total_pages().await?;
            progress.corruptions_found = 0;
            progress.scan_start = Some(scan_start);
        }

        let total_pages = self.get_total_pages().await?;

        let corruptions = if self.config.parallel {
            self.parallel_scan(scan_id, total_pages).await?
        } else {
            self.sequential_scan(scan_id, total_pages).await?
        };

        let end_time = SystemTime::now();
        let duration = end_time.duration_since(start_time).ok();

        Ok(ScanResult {
            scan_id,
            start_time,
            end_time: Some(end_time),
            pages_scanned: total_pages,
            corruptions,
            scan_type: ScanType::Full,
            duration,
            scan_progress: 100.0,
        })
    }

    pub async fn quick_scan(&self) -> Result<ScanResult> {
        let scan_id = self.get_next_scan_id().await;
        let start_time = SystemTime::now();

        // Quick scan: sample 10% of pages
        let total_pages = self.get_total_pages().await?;
        let sample_size = (total_pages / 10).max(100); // At least 100 pages
        let mut corruptions = Vec::new();

        for i in 0..sample_size {
            let page_id = (i * 10) + 1; // Sample every 10th page
            
            if let Ok(page_data) = self.get_page_data(page_id).await {
                if let Ok(page_corruptions) = self.detector.detect_page_corruption(page_id, &page_data).await {
                    corruptions.extend(page_corruptions);
                }
            }

            // Update progress
            {
                let mut progress = self.scan_progress.write().await;
                progress.current_page = i;
                progress.total_pages = sample_size;
                progress.corruptions_found = corruptions.len();
            }
        }

        let end_time = SystemTime::now();
        let duration = end_time.duration_since(start_time).ok();

        Ok(ScanResult {
            scan_id,
            start_time,
            end_time: Some(end_time),
            pages_scanned: sample_size,
            corruptions,
            scan_type: ScanType::Quick,
            duration,
            scan_progress: 100.0,
        })
    }

    pub async fn targeted_scan(&self, page_ids: Vec<u64>) -> Result<ScanResult> {
        let scan_id = self.get_next_scan_id().await;
        let start_time = SystemTime::now();
        let mut corruptions = Vec::new();

        // Initialize progress
        {
            let mut progress = self.scan_progress.write().await;
            progress.current_page = 0;
            progress.total_pages = page_ids.len() as u64;
            progress.corruptions_found = 0;
        }

        for (idx, page_id) in page_ids.iter().enumerate() {
            if let Ok(page_data) = self.get_page_data(*page_id).await {
                if let Ok(page_corruptions) = self.detector.detect_page_corruption(*page_id, &page_data).await {
                    corruptions.extend(page_corruptions);
                }
            }

            // Update progress
            {
                let mut progress = self.scan_progress.write().await;
                progress.current_page = idx as u64 + 1;
                progress.corruptions_found = corruptions.len();
            }
        }

        let end_time = SystemTime::now();
        let duration = end_time.duration_since(start_time).ok();

        Ok(ScanResult {
            scan_id,
            start_time,
            end_time: Some(end_time),
            pages_scanned: page_ids.len() as u64,
            corruptions,
            scan_type: ScanType::Targeted,
            duration,
            scan_progress: 100.0,
        })
    }

    async fn parallel_scan(&self, _scan_id: u64, total_pages: u64) -> Result<Vec<DetectionResult>> {
        let (tx, mut rx) = mpsc::channel(1000);
        let mut corruptions = Vec::new();
        let num_workers = num_cpus::get().min(8);
        let pages_per_worker = total_pages / num_workers as u64;

        // Spawn worker tasks
        let mut handles = Vec::new();
        for worker_id in 0..num_workers {
            let start_page = worker_id as u64 * pages_per_worker;
            let end_page = if worker_id == num_workers - 1 {
                total_pages
            } else {
                (worker_id as u64 + 1) * pages_per_worker
            };

            let worker_tx = tx.clone();
            let detector = self.detector.clone();
            let validator = self.validator.clone();
            let database = self.database.clone();
            let deep_validation = self.config.deep_validation;

            let handle = tokio::spawn(async move {
                for page_id in start_page..end_page {
                    if let Ok(page_data) = Self::get_page_data_static(&database, page_id).await {
                        if let Ok(page_corruptions) = detector.detect_page_corruption(page_id, &page_data).await {
                            for corruption in page_corruptions {
                                let _ = worker_tx.send((worker_id, page_id, corruption)).await;
                            }
                        }

                        if deep_validation {
                            if let Ok(validation_result) = validator.validate_btree_node(&page_data).await {
                                if !validation_result.is_valid {
                                    for error in validation_result.errors {
                                        let corruption = DetectionResult {
                                            page_id,
                                            corruption_type: CorruptionType::BTreeStructureViolation,
                                            severity: super::detector::CorruptionSeverity::Medium,
                                            description: format!("Validation error: {:?}", error),
                                            affected_data: page_data.clone(),
                                            recovery_hint: Some("Rebuild B+Tree structure".to_string()),
                                            timestamp: SystemTime::now(),
                                        };
                                        let _ = worker_tx.send((worker_id, page_id, corruption)).await;
                                    }
                                }
                            }
                        }
                    }
                }
            });

            handles.push(handle);
        }

        // Close sender
        drop(tx);

        // Collect results
        let mut pages_processed = 0;
        while let Some((_worker_id, _page_id, corruption)) = rx.recv().await {
            corruptions.push(corruption);
            pages_processed += 1;

            // Update progress periodically
            if pages_processed % 100 == 0 {
                let mut progress = self.scan_progress.write().await;
                progress.current_page = pages_processed;
                progress.corruptions_found = corruptions.len();
            }
        }

        // Wait for all workers to complete
        for handle in handles {
            let _ = handle.await;
        }

        Ok(corruptions)
    }

    async fn sequential_scan(&self, _scan_id: u64, total_pages: u64) -> Result<Vec<DetectionResult>> {
        let mut corruptions = Vec::new();

        for page_id in 1..=total_pages {
            if let Ok(page_data) = self.get_page_data(page_id).await {
                if let Ok(page_corruptions) = self.detector.detect_page_corruption(page_id, &page_data).await {
                    corruptions.extend(page_corruptions);
                }

                if self.config.deep_validation {
                    if let Ok(validation_result) = self.validator.validate_btree_node(&page_data).await {
                        if !validation_result.is_valid {
                            for error in validation_result.errors {
                                corruptions.push(DetectionResult {
                                    page_id,
                                    corruption_type: CorruptionType::BTreeStructureViolation,
                                    severity: super::detector::CorruptionSeverity::Medium,
                                    description: format!("Validation error: {:?}", error),
                                    affected_data: page_data.clone(),
                                    recovery_hint: Some("Rebuild B+Tree structure".to_string()),
                                    timestamp: SystemTime::now(),
                                });
                            }
                        }
                    }
                }
            }

            // Update progress every 100 pages
            if page_id % 100 == 0 {
                let mut progress = self.scan_progress.write().await;
                progress.current_page = page_id;
                progress.corruptions_found = corruptions.len();
            }

            // Yield control periodically
            if page_id % self.config.batch_size as u64 == 0 {
                tokio::task::yield_now().await;
            }
        }

        Ok(corruptions)
    }

    async fn background_scan_loop(&self) {
        loop {
            // Check if we should stop
            {
                let control = self.scan_control.read().await;
                if control.should_stop {
                    break;
                }
            }

            // Perform background scan
            if let Ok(scan_result) = self.quick_scan().await {
                if scan_result.has_corruption() {
                    // Log corruption found during background scan
                    eprintln!("Background scan found {} corruptions", scan_result.corruption_count());
                }
            }

            // Wait for next scan interval
            let interval = self.scan_control.read().await.scan_interval;
            time::sleep(interval).await;
        }
    }

    pub async fn get_scan_progress(&self) -> (f64, u64, usize) {
        let progress = self.scan_progress.read().await;
        let percentage = if progress.total_pages > 0 {
            (progress.current_page as f64 / progress.total_pages as f64) * 100.0
        } else {
            0.0
        };
        (percentage, progress.current_page, progress.corruptions_found)
    }

    pub async fn update_scan_interval(&self, interval: Duration) -> Result<()> {
        self.scan_control.write().await.scan_interval = interval;
        Ok(())
    }

    pub fn get_pages_scanned(&self) -> u64 {
        // This would return the actual count from the last scan
        0 // Simplified
    }

    // Helper methods

    async fn get_next_scan_id(&self) -> u64 {
        let mut counter = self.scan_counter.lock().await;
        *counter += 1;
        *counter
    }

    async fn get_total_pages(&self) -> Result<u64> {
        // This would query the database for actual page count
        Ok(1000) // Simplified
    }

    async fn get_page_data(&self, _page_id: u64) -> Result<Vec<u8>> {
        // This would retrieve actual page data from the database
        Ok(vec![b'L', b'N', b'D', b'B', 1, 0, 0, 0, 0, 0, 0, 0]) // Simplified valid page header
    }

    async fn get_page_data_static(_database: &Database, _page_id: u64) -> Result<Vec<u8>> {
        // Static version for use in spawned tasks
        Ok(vec![b'L', b'N', b'D', b'B', 1, 0, 0, 0, 0, 0, 0, 0]) // Simplified
    }

    fn clone_for_background(&self) -> Self {
        Self {
            database: self.database.clone(),
            detector: self.detector.clone(),
            validator: self.validator.clone(),
            config: self.config.clone(),
            scan_counter: self.scan_counter.clone(),
            background_handle: Arc::new(RwLock::new(None)), // New handle for background task
            scan_control: self.scan_control.clone(),
            scan_progress: self.scan_progress.clone(),
        }
    }
}