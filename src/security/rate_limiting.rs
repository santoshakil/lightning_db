use crate::security::{SecurityError, SecurityResult};
use governor::{
    clock::{Clock, DefaultClock},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed, keyed::DashMapStateStore},
    Quota, RateLimiter,
};
use ipnet::IpNet;
use std::collections::HashMap;
use std::net::IpAddr;
use std::num::NonZeroU32;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock as TokioRwLock;

type GlobalRateLimiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>;
type IpRateLimiter = RateLimiter<IpAddr, DashMapStateStore<IpAddr>, DefaultClock, NoOpMiddleware>;

pub struct RateLimitingManager {
    global_limiter: GlobalRateLimiter,
    ip_limiters: Arc<TokioRwLock<HashMap<String, IpRateLimiter>>>,
    user_limiters: Arc<TokioRwLock<HashMap<String, GlobalRateLimiter>>>,
    connection_tracker: Arc<RwLock<ConnectionTracker>>,
    blocked_ips: Arc<RwLock<HashMap<IpAddr, BlockedIp>>>,
    blocked_networks: Arc<RwLock<Vec<IpNet>>>,
    ddos_detector: Arc<RwLock<DDoSDetector>>,
}

struct ConnectionTracker {
    connections_per_ip: HashMap<IpAddr, usize>,
    max_connections_per_ip: usize,
    total_connections: usize,
    max_total_connections: usize,
}

#[derive(Clone)]
struct BlockedIp {
    blocked_at: Instant,
    blocked_until: Instant,
    reason: String,
    block_count: u32,
}

struct DDoSDetector {
    request_history: HashMap<IpAddr, Vec<Instant>>,
    detection_window: Duration,
    threshold: usize,
    ban_duration: Duration,
}

impl RateLimitingManager {
    pub fn new(
        global_requests_per_minute: u32,
        ip_requests_per_minute: u32,
        _user_requests_per_minute: u32,
        max_connections_per_ip: usize,
        max_total_connections: usize,
    ) -> SecurityResult<Self> {
        let global_quota = Quota::per_minute(
            NonZeroU32::new(global_requests_per_minute)
                .ok_or_else(|| SecurityError::InputValidationFailed("Invalid global rate limit".to_string()))?
        );
        
        let _ip_quota = Quota::per_minute(
            NonZeroU32::new(ip_requests_per_minute)
                .ok_or_else(|| SecurityError::InputValidationFailed("Invalid IP rate limit".to_string()))?
        );

        let global_limiter = RateLimiter::direct(global_quota);

        let connection_tracker = ConnectionTracker {
            connections_per_ip: HashMap::new(),
            max_connections_per_ip,
            total_connections: 0,
            max_total_connections,
        };

        let ddos_detector = DDoSDetector {
            request_history: HashMap::new(),
            detection_window: Duration::from_secs(60),
            threshold: ip_requests_per_minute as usize * 10,
            ban_duration: Duration::from_secs(3600),
        };

        Ok(Self {
            global_limiter,
            ip_limiters: Arc::new(TokioRwLock::new(HashMap::new())),
            user_limiters: Arc::new(TokioRwLock::new(HashMap::new())),
            connection_tracker: Arc::new(RwLock::new(connection_tracker)),
            blocked_ips: Arc::new(RwLock::new(HashMap::new())),
            blocked_networks: Arc::new(RwLock::new(Vec::new())),
            ddos_detector: Arc::new(RwLock::new(ddos_detector)),
        })
    }

    pub async fn check_global_limit(&self) -> SecurityResult<()> {
        if self.global_limiter.check().is_ok() {
            Ok(())
        } else {
            Err(SecurityError::RateLimitExceeded("Global rate limit exceeded".to_string()))
        }
    }

    pub async fn check_ip_limit(&self, ip: IpAddr) -> SecurityResult<()> {
        if self.is_ip_blocked(ip)? {
            return Err(SecurityError::RateLimitExceeded("IP address is blocked".to_string()));
        }

        self.detect_ddos(ip)?;

        let mut limiters = self.ip_limiters.write().await;
        let key = ip.to_string();
        
        if !limiters.contains_key(&key) {
            let quota = Quota::per_minute(NonZeroU32::new(100).unwrap());
            let limiter = RateLimiter::keyed(quota);
            limiters.insert(key.clone(), limiter);
        }

        let limiter = limiters.get(&key).unwrap();
        if limiter.check_key(&ip).is_ok() {
            Ok(())
        } else {
            self.handle_rate_limit_violation(ip).await?;
            Err(SecurityError::RateLimitExceeded(format!("Rate limit exceeded for IP: {}", ip)))
        }
    }

    pub async fn check_user_limit(&self, user_id: &str) -> SecurityResult<()> {
        let mut limiters = self.user_limiters.write().await;
        
        if !limiters.contains_key(user_id) {
            let quota = Quota::per_minute(NonZeroU32::new(200).unwrap());
            let limiter = RateLimiter::direct(quota);
            limiters.insert(user_id.to_string(), limiter);
        }

        let limiter = limiters.get(user_id).unwrap();
        if limiter.check().is_ok() {
            Ok(())
        } else {
            Err(SecurityError::RateLimitExceeded(format!("Rate limit exceeded for user: {}", user_id)))
        }
    }

    pub fn add_connection(&self, ip: IpAddr) -> SecurityResult<()> {
        let mut tracker = self.connection_tracker.write().unwrap();
        
        if tracker.total_connections >= tracker.max_total_connections {
            return Err(SecurityError::ResourceQuotaExceeded("Maximum total connections reached".to_string()));
        }

        let max_connections_per_ip = tracker.max_connections_per_ip;
        let ip_connections = tracker.connections_per_ip.entry(ip).or_insert(0);
        if *ip_connections >= max_connections_per_ip {
            return Err(SecurityError::ResourceQuotaExceeded(
                format!("Maximum connections per IP ({}) reached for {}", max_connections_per_ip, ip)
            ));
        }

        *ip_connections += 1;
        tracker.total_connections += 1;
        Ok(())
    }

    pub fn remove_connection(&self, ip: IpAddr) {
        let mut tracker = self.connection_tracker.write().unwrap();
        
        let mut should_decrement_total = false;
        let should_remove = if let Some(count) = tracker.connections_per_ip.get_mut(&ip) {
            if *count > 0 {
                *count -= 1;
                should_decrement_total = true;
                *count == 0
            } else {
                false
            }
        } else {
            false
        };
        
        if should_decrement_total {
            tracker.total_connections = tracker.total_connections.saturating_sub(1);
        }
        
        if should_remove {
            tracker.connections_per_ip.remove(&ip);
        }
    }

    pub fn block_ip(&self, ip: IpAddr, duration: Duration, reason: String) {
        let mut blocked = self.blocked_ips.write().unwrap();
        let now = Instant::now();
        
        let block_count = blocked.get(&ip).map(|b| b.block_count + 1).unwrap_or(1);
        
        let blocked_ip = BlockedIp {
            blocked_at: now,
            blocked_until: now + duration,
            reason,
            block_count,
        };
        
        blocked.insert(ip, blocked_ip);
    }

    pub fn unblock_ip(&self, ip: IpAddr) {
        let mut blocked = self.blocked_ips.write().unwrap();
        blocked.remove(&ip);
    }

    pub fn block_network(&self, network: IpNet) {
        let mut blocked_networks = self.blocked_networks.write().unwrap();
        if !blocked_networks.contains(&network) {
            blocked_networks.push(network);
        }
    }

    pub fn is_ip_blocked(&self, ip: IpAddr) -> SecurityResult<bool> {
        let blocked_ips = self.blocked_ips.read().unwrap();
        if let Some(blocked_ip) = blocked_ips.get(&ip) {
            if Instant::now() < blocked_ip.blocked_until {
                return Ok(true);
            }
        }

        let blocked_networks = self.blocked_networks.read().unwrap();
        for network in blocked_networks.iter() {
            if network.contains(&ip) {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn get_connection_stats(&self) -> (usize, usize, HashMap<IpAddr, usize>) {
        let tracker = self.connection_tracker.read().unwrap();
        (
            tracker.total_connections,
            tracker.max_total_connections,
            tracker.connections_per_ip.clone(),
        )
    }

    pub fn cleanup_expired_blocks(&self) {
        let mut blocked = self.blocked_ips.write().unwrap();
        let now = Instant::now();
        blocked.retain(|_, block| now < block.blocked_until);
    }

    fn detect_ddos(&self, ip: IpAddr) -> SecurityResult<()> {
        let mut detector = self.ddos_detector.write().unwrap();
        let now = Instant::now();
        
        let detection_window = detector.detection_window;
        let threshold = detector.threshold;
        
        let history = detector.request_history.entry(ip).or_insert_with(Vec::new);
        history.push(now);
        history.retain(|&time| now.duration_since(time) <= detection_window);
        
        if history.len() > threshold {
            drop(detector);
            self.block_ip(ip, Duration::from_secs(3600), "DDoS attack detected".to_string());
            return Err(SecurityError::RateLimitExceeded(
                format!("DDoS attack detected from IP: {}", ip)
            ));
        }
        
        Ok(())
    }

    async fn handle_rate_limit_violation(&self, ip: IpAddr) -> SecurityResult<()> {
        let mut blocked_ips = self.blocked_ips.write().unwrap();
        
        if let Some(blocked_ip) = blocked_ips.get_mut(&ip) {
            if blocked_ip.block_count >= 3 {
                blocked_ip.blocked_until = Instant::now() + Duration::from_secs(3600);
                blocked_ip.block_count += 1;
                blocked_ip.reason = "Repeated rate limit violations".to_string();
            }
        } else {
            let blocked_ip = BlockedIp {
                blocked_at: Instant::now(),
                blocked_until: Instant::now() + Duration::from_secs(300),
                reason: "Rate limit violation".to_string(),
                block_count: 1,
            };
            blocked_ips.insert(ip, blocked_ip);
        }
        
        Ok(())
    }
}

pub struct RequestMetrics {
    pub total_requests: u64,
    pub blocked_requests: u64,
    pub rate_limited_requests: u64,
    pub ddos_detections: u64,
}

impl Default for RequestMetrics {
    fn default() -> Self {
        Self {
            total_requests: 0,
            blocked_requests: 0,
            rate_limited_requests: 0,
            ddos_detections: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use tokio;

    #[tokio::test]
    async fn test_global_rate_limiting() {
        let manager = RateLimitingManager::new(1, 100, 100, 10, 1000).unwrap();
        
        assert!(manager.check_global_limit().await.is_ok());
        
        for _ in 0..60 {
            let _ = manager.check_global_limit().await;
        }
        
        assert!(manager.check_global_limit().await.is_err());
    }

    #[tokio::test]
    async fn test_ip_rate_limiting() {
        let manager = RateLimitingManager::new(1000, 2, 100, 10, 1000).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        
        assert!(manager.check_ip_limit(ip).await.is_ok());
        assert!(manager.check_ip_limit(ip).await.is_ok());
        
        for _ in 0..60 {
            let _ = manager.check_ip_limit(ip).await;
        }
        
        assert!(manager.check_ip_limit(ip).await.is_err());
    }

    #[test]
    fn test_connection_limiting() {
        let manager = RateLimitingManager::new(1000, 100, 100, 2, 10).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        
        assert!(manager.add_connection(ip).is_ok());
        assert!(manager.add_connection(ip).is_ok());
        assert!(manager.add_connection(ip).is_err());
        
        manager.remove_connection(ip);
        assert!(manager.add_connection(ip).is_ok());
    }

    #[test]
    fn test_ip_blocking() {
        let manager = RateLimitingManager::new(1000, 100, 100, 10, 1000).unwrap();
        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        
        assert!(!manager.is_ip_blocked(ip).unwrap());
        
        manager.block_ip(ip, Duration::from_secs(60), "Test block".to_string());
        assert!(manager.is_ip_blocked(ip).unwrap());
        
        manager.unblock_ip(ip);
        assert!(!manager.is_ip_blocked(ip).unwrap());
    }
}