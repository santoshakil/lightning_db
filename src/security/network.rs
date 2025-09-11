use crate::security::{SecurityError, SecurityResult};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

pub struct NetworkSecurityManager {
    connection_tracker: Arc<RwLock<ConnectionTracker>>,
    firewall_rules: Arc<RwLock<FirewallRules>>,
}

struct ConnectionTracker {
    active_connections: HashMap<SocketAddr, ConnectionInfo>,
    max_connections_per_ip: usize,
    connection_timeout: Duration,
}

struct ConnectionInfo {
    connected_at: SystemTime,
    last_activity: SystemTime,
    bytes_sent: u64,
    bytes_received: u64,
    is_secure: bool,
}

struct FirewallRules {
    allowed_ips: Vec<IpAddr>,
    blocked_ips: Vec<IpAddr>,
    allowed_ports: Vec<u16>,
}

impl NetworkSecurityManager {
    pub fn new() -> SecurityResult<Self> {
        let connection_tracker = ConnectionTracker {
            active_connections: HashMap::new(),
            max_connections_per_ip: 100,
            connection_timeout: Duration::from_secs(300),
        };

        let firewall_rules = FirewallRules {
            allowed_ips: Vec::new(),
            blocked_ips: Vec::new(),
            allowed_ports: vec![8080, 8443],
        };

        Ok(Self {
            connection_tracker: Arc::new(RwLock::new(connection_tracker)),
            firewall_rules: Arc::new(RwLock::new(firewall_rules)),
        })
    }

    pub fn validate_incoming_connection(&self, addr: SocketAddr) -> SecurityResult<()> {
        let firewall = self.firewall_rules.read().unwrap();

        if !firewall.blocked_ips.is_empty() && firewall.blocked_ips.contains(&addr.ip()) {
            return Err(SecurityError::NetworkSecurityViolation(format!(
                "IP address {} is blocked",
                addr.ip()
            )));
        }

        if !firewall.allowed_ips.is_empty() && !firewall.allowed_ips.contains(&addr.ip()) {
            return Err(SecurityError::NetworkSecurityViolation(format!(
                "IP address {} is not in allow list",
                addr.ip()
            )));
        }

        if !firewall.allowed_ports.contains(&addr.port()) {
            return Err(SecurityError::NetworkSecurityViolation(format!(
                "Port {} is not allowed",
                addr.port()
            )));
        }

        let tracker = self.connection_tracker.read().unwrap();
        let ip_connections = tracker
            .active_connections
            .iter()
            .filter(|(socket_addr, _)| socket_addr.ip() == addr.ip())
            .count();

        if ip_connections >= tracker.max_connections_per_ip {
            return Err(SecurityError::NetworkSecurityViolation(format!(
                "Too many connections from IP {}",
                addr.ip()
            )));
        }

        Ok(())
    }

    pub fn register_connection(&self, addr: SocketAddr, is_secure: bool) -> SecurityResult<()> {
        let mut tracker = self.connection_tracker.write().unwrap();
        let now = SystemTime::now();

        let connection_info = ConnectionInfo {
            connected_at: now,
            last_activity: now,
            bytes_sent: 0,
            bytes_received: 0,
            is_secure,
        };

        tracker.active_connections.insert(addr, connection_info);
        Ok(())
    }

    pub fn unregister_connection(&self, addr: SocketAddr) {
        let mut tracker = self.connection_tracker.write().unwrap();
        tracker.active_connections.remove(&addr);
    }

    pub fn update_connection_activity(
        &self,
        addr: SocketAddr,
        bytes_sent: u64,
        bytes_received: u64,
    ) {
        let mut tracker = self.connection_tracker.write().unwrap();
        if let Some(conn_info) = tracker.active_connections.get_mut(&addr) {
            conn_info.last_activity = SystemTime::now();
            conn_info.bytes_sent += bytes_sent;
            conn_info.bytes_received += bytes_received;
        }
    }

    pub fn add_firewall_rule(&self, rule: FirewallRule) -> SecurityResult<()> {
        let mut firewall = self.firewall_rules.write().unwrap();

        match rule {
            FirewallRule::AllowIp(ip) => {
                if !firewall.allowed_ips.contains(&ip) {
                    firewall.allowed_ips.push(ip);
                }
            }
            FirewallRule::BlockIp(ip) => {
                if !firewall.blocked_ips.contains(&ip) {
                    firewall.blocked_ips.push(ip);
                }
            }
            FirewallRule::AllowPort(port) => {
                if !firewall.allowed_ports.contains(&port) {
                    firewall.allowed_ports.push(port);
                }
            }
        }

        Ok(())
    }

    pub fn get_connection_stats(&self) -> ConnectionStats {
        let tracker = self.connection_tracker.read().unwrap();
        let total_connections = tracker.active_connections.len();
        let secure_connections = tracker
            .active_connections
            .values()
            .filter(|conn| conn.is_secure)
            .count();

        let total_bytes_sent: u64 = tracker
            .active_connections
            .values()
            .map(|conn| conn.bytes_sent)
            .sum();

        let total_bytes_received: u64 = tracker
            .active_connections
            .values()
            .map(|conn| conn.bytes_received)
            .sum();

        ConnectionStats {
            total_connections,
            tls_connections: secure_connections,
            total_bytes_sent,
            total_bytes_received,
        }
    }

    pub fn cleanup_stale_connections(&self) {
        let mut tracker = self.connection_tracker.write().unwrap();
        let now = SystemTime::now();
        let connection_timeout = tracker.connection_timeout;

        tracker.active_connections.retain(|_, conn_info| {
            now.duration_since(conn_info.last_activity)
                .unwrap_or(Duration::ZERO)
                <= connection_timeout
        });
    }
}

#[derive(Debug)]
pub enum FirewallRule {
    AllowIp(IpAddr),
    BlockIp(IpAddr),
    AllowPort(u16),
}

#[derive(Debug, Clone)]
pub struct ConnectionStats {
    pub total_connections: usize,
    pub tls_connections: usize,
    pub total_bytes_sent: u64,
    pub total_bytes_received: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn test_firewall_rules() {
        let manager = NetworkSecurityManager::new().unwrap();

        let ip = IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1));
        manager
            .add_firewall_rule(FirewallRule::BlockIp(ip))
            .unwrap();

        let addr = SocketAddr::new(ip, 8080);
        assert!(manager.validate_incoming_connection(addr).is_err());
    }

    #[test]
    fn test_connection_tracking() {
        let manager = NetworkSecurityManager::new().unwrap();

        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        manager.register_connection(addr, true).unwrap();

        let stats = manager.get_connection_stats();
        assert_eq!(stats.total_connections, 1);
        assert_eq!(stats.tls_connections, 1);
    }
}
