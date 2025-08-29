use std::sync::Arc;
use std::time::Duration;
use crate::core::error::Result;

#[derive(Debug, Clone)]
pub struct WatermarkGenerator {
    strategy: WatermarkStrategy,
    max_out_of_orderness: Duration,
    current_watermark: Arc<std::sync::atomic::AtomicU64>,
}

#[derive(Debug, Clone)]
pub enum WatermarkStrategy {
    Periodic(Duration),
    Punctuated,
    BoundedOutOfOrderness(Duration),
    MonotonousTimestamps,
    Custom(Arc<dyn Fn(u64) -> u64 + Send + Sync>),
}

pub trait EventTimeExtractor: Send + Sync {
    fn extract_timestamp(&self, event: &[u8]) -> Result<u64>;
}

impl WatermarkGenerator {
    pub fn new(strategy: WatermarkStrategy) -> Self {
        Self {
            strategy,
            max_out_of_orderness: Duration::from_secs(10),
            current_watermark: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
    
    pub fn get_current_watermark(&self) -> u64 {
        self.current_watermark.load(std::sync::atomic::Ordering::Relaxed)
    }
    
    pub fn update_watermark(&self, timestamp: u64) -> u64 {
        let watermark = match &self.strategy {
            WatermarkStrategy::MonotonousTimestamps => timestamp,
            WatermarkStrategy::BoundedOutOfOrderness(delay) => {
                timestamp.saturating_sub(delay.as_millis() as u64)
            }
            _ => timestamp,
        };
        
        self.current_watermark.store(watermark, std::sync::atomic::Ordering::Relaxed);
        watermark
    }
}