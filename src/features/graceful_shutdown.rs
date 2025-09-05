use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::{RwLock, Notify, Semaphore};
use tokio::task::JoinHandle;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ShutdownError {
    #[error("Shutdown timeout exceeded")]
    Timeout,
    #[error("Component shutdown failed: {0}")]
    ComponentFailed(String),
    #[error("Already shutting down")]
    AlreadyShuttingDown,
    #[error("Force shutdown required")]
    ForceShutdownRequired,
}

pub type ShutdownResult = Result<(), ShutdownError>;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ShutdownPriority {
    Critical = 0,
    High = 1,
    Normal = 2,
    Low = 3,
    Background = 4,
}

#[derive(Debug, Clone)]
pub struct ShutdownConfig {
    pub graceful_timeout: Duration,
    pub drain_timeout: Duration,
    pub force_after: Option<Duration>,
    pub max_concurrent_shutdowns: usize,
    pub save_state: bool,
    pub notify_clients: bool,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            graceful_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(10),
            force_after: Some(Duration::from_secs(60)),
            max_concurrent_shutdowns: 10,
            save_state: true,
            notify_clients: true,
        }
    }
}

#[async_trait::async_trait]
pub trait Shutdownable: Send + Sync {
    fn name(&self) -> &str;
    
    fn priority(&self) -> ShutdownPriority {
        ShutdownPriority::Normal
    }
    
    async fn prepare_shutdown(&self) -> ShutdownResult {
        Ok(())
    }
    
    async fn shutdown(&self) -> ShutdownResult;
    
    async fn force_shutdown(&self) -> ShutdownResult {
        self.shutdown().await
    }
    
    fn is_critical(&self) -> bool {
        matches!(self.priority(), ShutdownPriority::Critical)
    }
}

pub struct ShutdownCoordinator {
    config: ShutdownConfig,
    components: Arc<RwLock<HashMap<String, Arc<dyn Shutdownable>>>>,
    shutdown_signal: Arc<AtomicBool>,
    active_operations: Arc<AtomicUsize>,
    shutdown_notify: Arc<Notify>,
    shutdown_semaphore: Arc<Semaphore>,
    shutdown_status: Arc<RwLock<HashMap<String, ComponentShutdownStatus>>>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ComponentShutdownStatus {
    pub name: String,
    pub priority: ShutdownPriority,
    pub state: ShutdownState,
    #[serde(skip)]
    pub started_at: Option<Instant>,
    #[serde(skip)]
    pub completed_at: Option<Instant>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ShutdownState {
    Active,
    Preparing,
    Draining,
    Shutting,
    Completed,
    Failed,
}

impl ShutdownCoordinator {
    pub fn new(config: ShutdownConfig) -> Self {
        let max_concurrent = config.max_concurrent_shutdowns;
        
        Self {
            config,
            components: Arc::new(RwLock::new(HashMap::new())),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            active_operations: Arc::new(AtomicUsize::new(0)),
            shutdown_notify: Arc::new(Notify::new()),
            shutdown_semaphore: Arc::new(Semaphore::new(max_concurrent)),
            shutdown_status: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub async fn register_component(&self, component: Arc<dyn Shutdownable>) {
        let name = component.name().to_string();
        let priority = component.priority();
        
        let mut components = self.components.write().await;
        components.insert(name.clone(), component);
        
        let mut status = self.shutdown_status.write().await;
        status.insert(name.clone(), ComponentShutdownStatus {
            name,
            priority,
            state: ShutdownState::Active,
            started_at: None,
            completed_at: None,
            error: None,
        });
    }
    
    pub async fn unregister_component(&self, name: &str) {
        let mut components = self.components.write().await;
        components.remove(name);
        
        let mut status = self.shutdown_status.write().await;
        status.remove(name);
    }
    
    pub fn is_shutting_down(&self) -> bool {
        self.shutdown_signal.load(Ordering::Acquire)
    }
    
    pub fn increment_active_operations(&self) {
        self.active_operations.fetch_add(1, Ordering::Release);
    }
    
    pub fn decrement_active_operations(&self) {
        self.active_operations.fetch_sub(1, Ordering::Release);
    }
    
    pub fn get_active_operations(&self) -> usize {
        self.active_operations.load(Ordering::Acquire)
    }
    
    pub async fn initiate_shutdown(&self) -> ShutdownResult {
        if self.shutdown_signal.compare_exchange(
            false,
            true,
            Ordering::SeqCst,
            Ordering::SeqCst
        ).is_err() {
            return Err(ShutdownError::AlreadyShuttingDown);
        }
        
        self.shutdown_notify.notify_waiters();
        
        if self.config.notify_clients {
            self.notify_clients_shutdown().await;
        }
        
        let components = self.components.read().await;
        let mut sorted_components: Vec<_> = components.iter().collect();
        sorted_components.sort_by_key(|(_, c)| c.priority() as u8);
        
        let mut handles = Vec::new();
        
        for priority in 0..=4 {
            let priority_components: Vec<_> = sorted_components
                .iter()
                .filter(|(_, c)| c.priority() as u8 == priority)
                .cloned()
                .collect();
            
            if priority_components.is_empty() {
                continue;
            }
            
            for (name, component) in priority_components {
                let name = name.clone();
                let component = component.clone();
                let status = self.shutdown_status.clone();
                let semaphore = self.shutdown_semaphore.clone();
                let config = self.config.clone();
                
                let handle = tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    Self::shutdown_component(name, component, status, config).await
                });
                
                handles.push(handle);
            }
            
            for handle in &handles {
                if !handle.is_finished() {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        
        let timeout = tokio::time::timeout(
            self.config.graceful_timeout,
            Self::wait_for_all(handles)
        ).await;
        
        match timeout {
            Ok(results) => {
                let mut had_error = false;
                for result in results {
                    if result.is_err() {
                        had_error = true;
                    }
                }
                
                if had_error {
                    return Err(ShutdownError::ComponentFailed("Some components failed".to_string()));
                }
                
                Ok(())
            }
            Err(_) => {
                if let Some(force_after) = self.config.force_after {
                    tokio::time::sleep(force_after).await;
                    self.force_shutdown_all().await
                } else {
                    Err(ShutdownError::Timeout)
                }
            }
        }
    }
    
    async fn shutdown_component(
        name: String,
        component: Arc<dyn Shutdownable>,
        status: Arc<RwLock<HashMap<String, ComponentShutdownStatus>>>,
        config: ShutdownConfig,
    ) -> ShutdownResult {
        let started_at = Instant::now();
        
        {
            let mut status_map = status.write().await;
            if let Some(status) = status_map.get_mut(&name) {
                status.state = ShutdownState::Preparing;
                status.started_at = Some(started_at);
            }
        }
        
        if let Err(e) = component.prepare_shutdown().await {
            let mut status_map = status.write().await;
            if let Some(status) = status_map.get_mut(&name) {
                status.state = ShutdownState::Failed;
                status.error = Some(e.to_string());
            }
            return Err(e);
        }
        
        {
            let mut status_map = status.write().await;
            if let Some(status) = status_map.get_mut(&name) {
                status.state = ShutdownState::Draining;
            }
        }
        
        tokio::time::sleep(config.drain_timeout).await;
        
        {
            let mut status_map = status.write().await;
            if let Some(status) = status_map.get_mut(&name) {
                status.state = ShutdownState::Shutting;
            }
        }
        
        let shutdown_result = tokio::time::timeout(
            config.graceful_timeout,
            component.shutdown()
        ).await;
        
        let result = match shutdown_result {
            Ok(Ok(())) => {
                let mut status_map = status.write().await;
                if let Some(status) = status_map.get_mut(&name) {
                    status.state = ShutdownState::Completed;
                    status.completed_at = Some(Instant::now());
                }
                Ok(())
            }
            Ok(Err(e)) => {
                let mut status_map = status.write().await;
                if let Some(status) = status_map.get_mut(&name) {
                    status.state = ShutdownState::Failed;
                    status.error = Some(e.to_string());
                    status.completed_at = Some(Instant::now());
                }
                Err(e)
            }
            Err(_) => {
                let force_result = component.force_shutdown().await;
                
                let mut status_map = status.write().await;
                if let Some(status) = status_map.get_mut(&name) {
                    if force_result.is_ok() {
                        status.state = ShutdownState::Completed;
                    } else {
                        status.state = ShutdownState::Failed;
                        status.error = Some("Force shutdown failed".to_string());
                    }
                    status.completed_at = Some(Instant::now());
                }
                
                force_result
            }
        };
        
        result
    }
    
    async fn force_shutdown_all(&self) -> ShutdownResult {
        let components = self.components.read().await;
        let mut handles = Vec::new();
        
        for (name, component) in components.iter() {
            let component = component.clone();
            let handle = tokio::spawn(async move {
                component.force_shutdown().await
            });
            handles.push(handle);
        }
        
        Self::wait_for_all(handles).await;
        Err(ShutdownError::ForceShutdownRequired)
    }
    
    async fn wait_for_all(handles: Vec<JoinHandle<ShutdownResult>>) -> Vec<ShutdownResult> {
        let mut results = Vec::new();
        
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(_) => results.push(Err(ShutdownError::ComponentFailed("Join error".to_string()))),
            }
        }
        
        results
    }
    
    async fn notify_clients_shutdown(&self) {
        
    }
    
    pub async fn wait_for_shutdown(&self) {
        self.shutdown_notify.notified().await;
    }
    
    pub async fn wait_for_operations(&self, timeout: Duration) -> bool {
        let start = Instant::now();
        
        while self.active_operations.load(Ordering::Acquire) > 0 {
            if start.elapsed() > timeout {
                return false;
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        
        true
    }
    
    pub async fn get_shutdown_status(&self) -> Vec<ComponentShutdownStatus> {
        let status = self.shutdown_status.read().await;
        status.values().cloned().collect()
    }
    
    pub async fn save_shutdown_state(&self) -> ShutdownResult {
        if !self.config.save_state {
            return Ok(());
        }
        
        let status = self.get_shutdown_status().await;
        let state_json = serde_json::to_string_pretty(&status)
            .map_err(|e| ShutdownError::ComponentFailed(e.to_string()))?;
        
        tokio::fs::write("shutdown_state.json", state_json).await
            .map_err(|e| ShutdownError::ComponentFailed(e.to_string()))?;
        
        Ok(())
    }
}

pub struct ShutdownGuard {
    coordinator: Arc<ShutdownCoordinator>,
}

impl ShutdownGuard {
    pub fn new(coordinator: Arc<ShutdownCoordinator>) -> Self {
        coordinator.increment_active_operations();
        Self { coordinator }
    }
}

impl Drop for ShutdownGuard {
    fn drop(&mut self) {
        self.coordinator.decrement_active_operations();
    }
}

#[macro_export]
macro_rules! check_shutdown {
    ($coordinator:expr) => {
        if $coordinator.is_shutting_down() {
            return Err("System is shutting down".into());
        }
    };
}

pub struct GracefulShutdownHandler {
    coordinator: Arc<ShutdownCoordinator>,
    signal_handlers: Vec<JoinHandle<()>>,
}

impl GracefulShutdownHandler {
    pub fn new(coordinator: Arc<ShutdownCoordinator>) -> Self {
        Self {
            coordinator,
            signal_handlers: Vec::new(),
        }
    }
    
    pub fn install_signal_handlers(&mut self) {
        let coordinator = self.coordinator.clone();
        
        let ctrl_c_handler = tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            println!("\nReceived Ctrl+C, initiating graceful shutdown...");
            coordinator.initiate_shutdown().await.unwrap();
        });
        
        self.signal_handlers.push(ctrl_c_handler);
        
        #[cfg(unix)]
        {
            let coordinator = self.coordinator.clone();
            let sigterm_handler = tokio::spawn(async move {
                let mut stream = tokio::signal::unix::signal(
                    tokio::signal::unix::SignalKind::terminate()
                ).unwrap();
                
                stream.recv().await;
                println!("\nReceived SIGTERM, initiating graceful shutdown...");
                coordinator.initiate_shutdown().await.unwrap();
            });
            
            self.signal_handlers.push(sigterm_handler);
        }
    }
    
    pub async fn wait(&self) {
        self.coordinator.wait_for_shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    struct TestComponent {
        name: String,
        priority: ShutdownPriority,
        shutdown_time: Duration,
    }
    
    #[async_trait::async_trait]
    impl Shutdownable for TestComponent {
        fn name(&self) -> &str {
            &self.name
        }
        
        fn priority(&self) -> ShutdownPriority {
            self.priority.clone()
        }
        
        async fn shutdown(&self) -> ShutdownResult {
            tokio::time::sleep(self.shutdown_time).await;
            Ok(())
        }
    }
    
    #[tokio::test]
    async fn test_graceful_shutdown() {
        let config = ShutdownConfig::default();
        let coordinator = Arc::new(ShutdownCoordinator::new(config));
        
        let component1 = Arc::new(TestComponent {
            name: "component1".to_string(),
            priority: ShutdownPriority::High,
            shutdown_time: Duration::from_millis(100),
        });
        
        let component2 = Arc::new(TestComponent {
            name: "component2".to_string(),
            priority: ShutdownPriority::Low,
            shutdown_time: Duration::from_millis(200),
        });
        
        coordinator.register_component(component1).await;
        coordinator.register_component(component2).await;
        
        let result = coordinator.initiate_shutdown().await;
        assert!(result.is_ok());
        
        let status = coordinator.get_shutdown_status().await;
        assert_eq!(status.len(), 2);
        
        for component_status in status {
            assert_eq!(component_status.state, ShutdownState::Completed);
        }
    }
}