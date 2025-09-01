use crate::core::error::{Error, Result};
use super::{CompactionConfig, CompactionType};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct ScheduledTask {
    pub task_id: u64,
    pub compaction_type: CompactionType,
    pub scheduled_at: Instant,
    pub priority: TaskPriority,
    pub estimated_duration: Duration,
    pub retry_count: usize,
    pub max_retries: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct SchedulerStats {
    pub tasks_scheduled: u64,
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub tasks_retried: u64,
    pub avg_task_duration: Duration,
    pub scheduler_uptime: Duration,
    pub next_scheduled_task: Option<Instant>,
}

#[derive(Debug)]
pub struct MaintenanceScheduler {
    config: Arc<RwLock<CompactionConfig>>,
    is_running: Arc<RwLock<bool>>,
    scheduled_tasks: Arc<RwLock<std::collections::BinaryHeap<PriorityTask>>>,
    stats: Arc<RwLock<SchedulerStats>>,
    next_task_id: Arc<std::sync::atomic::AtomicU64>,
    shutdown_signal: Arc<tokio::sync::Notify>,
    scheduler_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

#[derive(Debug)]
struct PriorityTask {
    task: ScheduledTask,
    next_run: Instant,
}

impl PartialEq for PriorityTask {
    fn eq(&self, other: &Self) -> bool {
        self.next_run == other.next_run
    }
}

impl Eq for PriorityTask {}

impl PartialOrd for PriorityTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse order for min-heap behavior (earliest first)
        other.next_run.cmp(&self.next_run)
            .then_with(|| other.task.priority.cmp(&self.task.priority))
    }
}

impl MaintenanceScheduler {
    pub fn new(config: Arc<RwLock<CompactionConfig>>) -> Result<Self> {
        Ok(Self {
            config,
            is_running: Arc::new(RwLock::new(false)),
            scheduled_tasks: Arc::new(RwLock::new(std::collections::BinaryHeap::new())),
            stats: Arc::new(RwLock::new(SchedulerStats {
                tasks_scheduled: 0,
                tasks_completed: 0,
                tasks_failed: 0,
                tasks_retried: 0,
                avg_task_duration: Duration::from_secs(0),
                scheduler_uptime: Duration::from_secs(0),
                next_scheduled_task: None,
            })),
            next_task_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            shutdown_signal: Arc::new(tokio::sync::Notify::new()),
            scheduler_handle: Arc::new(RwLock::new(None)),
        })
    }
    
    pub async fn start(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if *is_running {
            return Ok(()); // Already running
        }
        
        *is_running = true;
        drop(is_running);
        
        // Schedule initial tasks
        self.schedule_initial_tasks().await?;
        
        // Start the scheduler loop
        let scheduler = Arc::new(self.clone());
        let handle = tokio::spawn(async move {
            scheduler.run_scheduler_loop().await;
        });
        
        {
            let mut handle_guard = self.scheduler_handle.write().await;
            *handle_guard = Some(handle);
        }
        
        Ok(())
    }
    
    pub async fn stop(&self) -> Result<()> {
        let mut is_running = self.is_running.write().await;
        if !*is_running {
            return Ok(()); // Already stopped
        }
        
        *is_running = false;
        drop(is_running);
        
        // Signal shutdown
        self.shutdown_signal.notify_one();
        
        // Wait for scheduler to stop
        let handle = {
            let mut handle_guard = self.scheduler_handle.write().await;
            handle_guard.take()
        };
        
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        
        Ok(())
    }
    
    pub async fn schedule_task(&self, compaction_type: CompactionType, priority: TaskPriority, delay: Option<Duration>) -> Result<u64> {
        let task_id = self.next_task_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let scheduled_at = Instant::now() + delay.unwrap_or(Duration::from_secs(0));
        
        let task = ScheduledTask {
            task_id,
            compaction_type: compaction_type.clone(),
            scheduled_at,
            priority: priority.clone(),
            estimated_duration: self.estimate_task_duration(&compaction_type).await,
            retry_count: 0,
            max_retries: self.get_max_retries(&priority).await,
        };
        
        let priority_task = PriorityTask {
            next_run: scheduled_at,
            task,
        };
        
        {
            let mut scheduled_tasks = self.scheduled_tasks.write().await;
            scheduled_tasks.push(priority_task);
        }
        
        {
            let mut stats = self.stats.write().await;
            stats.tasks_scheduled += 1;
            stats.next_scheduled_task = Some(scheduled_at);
        }
        
        Ok(task_id)
    }
    
    pub async fn cancel_task(&self, task_id: u64) -> Result<()> {
        let mut scheduled_tasks = self.scheduled_tasks.write().await;
        let mut tasks_vec: Vec<_> = scheduled_tasks.drain().collect();
        
        tasks_vec.retain(|pt| pt.task.task_id != task_id);
        
        for task in tasks_vec {
            scheduled_tasks.push(task);
        }
        
        Ok(())
    }
    
    pub async fn get_scheduled_tasks(&self) -> Vec<ScheduledTask> {
        let scheduled_tasks = self.scheduled_tasks.read().await;
        scheduled_tasks.iter().map(|pt| pt.task.clone()).collect()
    }
    
    pub async fn get_stats(&self) -> SchedulerStats {
        let mut stats = self.stats.read().await.clone();
        
        // Update next scheduled task
        let scheduled_tasks = self.scheduled_tasks.read().await;
        stats.next_scheduled_task = scheduled_tasks.peek().map(|pt| pt.next_run);
        
        stats
    }
    
    async fn run_scheduler_loop(&self) {
        let start_time = Instant::now();
        
        while *self.is_running.read().await {
            // Check for tasks to execute
            if let Some(task) = self.get_next_ready_task().await {
                let task_start = Instant::now();
                
                match self.execute_task(&task).await {
                    Ok(_) => {
                        let duration = task_start.elapsed();
                        self.update_stats_on_completion(duration).await;
                    },
                    Err(e) => {
                        eprintln!("Task execution failed: {}", e);
                        
                        // Retry if possible
                        if task.retry_count < task.max_retries {
                            self.reschedule_task_with_retry(task).await.ok();
                            self.update_stats_on_retry().await;
                        } else {
                            self.update_stats_on_failure().await;
                        }
                    }
                }
            }
            
            // Update uptime
            {
                let mut stats = self.stats.write().await;
                stats.scheduler_uptime = start_time.elapsed();
            }
            
            // Wait for next iteration or shutdown signal
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                _ = self.shutdown_signal.notified() => break,
            }
        }
    }
    
    async fn get_next_ready_task(&self) -> Option<ScheduledTask> {
        let mut scheduled_tasks = self.scheduled_tasks.write().await;
        
        if let Some(priority_task) = scheduled_tasks.peek() {
            if priority_task.next_run <= Instant::now() {
                return scheduled_tasks.pop().map(|pt| pt.task);
            }
        }
        
        None
    }
    
    async fn execute_task(&self, task: &ScheduledTask) -> Result<()> {
        // Integration point for actual compaction manager
        // For now, simulate task execution
        
        match task.compaction_type {
            CompactionType::Online => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
            CompactionType::Offline => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            },
            CompactionType::Incremental => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            },
            CompactionType::Major => {
                tokio::time::sleep(Duration::from_millis(1000)).await;
            },
            CompactionType::Minor => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            },
        }
        
        // Simulate occasional failures based on task type
        let failure_rate = match task.compaction_type {
            CompactionType::Major => 0.05,  // 5% failure rate
            CompactionType::Offline => 0.03, // 3% failure rate
            _ => 0.01, // 1% failure rate for others
        };
        
        if rand::random::<f64>() < failure_rate {
            return Err(Error::Internal("Simulated task failure".into()));
        }
        
        Ok(())
    }
    
    async fn reschedule_task_with_retry(&self, mut task: ScheduledTask) -> Result<()> {
        task.retry_count += 1;
        
        // Exponential backoff for retries
        let backoff_duration = Duration::from_secs(2u64.pow(task.retry_count as u32).min(300)); // Max 5 minutes
        
        let priority_task = PriorityTask {
            next_run: Instant::now() + backoff_duration,
            task,
        };
        
        {
            let mut scheduled_tasks = self.scheduled_tasks.write().await;
            scheduled_tasks.push(priority_task);
        }
        
        Ok(())
    }
    
    async fn schedule_initial_tasks(&self) -> Result<()> {
        let config = self.config.read().await;
        if !config.auto_compaction_enabled {
            return Ok(());
        }
        
        let interval = config.compaction_interval;
        drop(config);
        
        // Schedule periodic maintenance tasks
        
        // Minor compaction every interval
        self.schedule_task(
            CompactionType::Minor,
            TaskPriority::Medium,
            Some(interval),
        ).await?;
        
        // Incremental compaction every interval/2
        self.schedule_task(
            CompactionType::Incremental,
            TaskPriority::Medium,
            Some(interval / 2),
        ).await?;
        
        // Online compaction every interval * 2
        self.schedule_task(
            CompactionType::Online,
            TaskPriority::Low,
            Some(interval * 2),
        ).await?;
        
        // Major compaction daily
        self.schedule_task(
            CompactionType::Major,
            TaskPriority::Low,
            Some(Duration::from_secs(86400)),
        ).await?;
        
        Ok(())
    }
    
    async fn estimate_task_duration(&self, compaction_type: &CompactionType) -> Duration {
        match compaction_type {
            CompactionType::Minor => Duration::from_secs(30),
            CompactionType::Incremental => Duration::from_secs(120),
            CompactionType::Online => Duration::from_secs(300),
            CompactionType::Offline => Duration::from_secs(900),
            CompactionType::Major => Duration::from_secs(3600),
        }
    }
    
    async fn get_max_retries(&self, priority: &TaskPriority) -> usize {
        match priority {
            TaskPriority::Critical => 5,
            TaskPriority::High => 3,
            TaskPriority::Medium => 2,
            TaskPriority::Low => 1,
        }
    }
    
    async fn update_stats_on_completion(&self, duration: Duration) {
        let mut stats = self.stats.write().await;
        stats.tasks_completed += 1;
        
        // Update average duration
        let total_duration = stats.avg_task_duration.as_secs_f64() * (stats.tasks_completed - 1) as f64 + duration.as_secs_f64();
        stats.avg_task_duration = Duration::from_secs_f64(total_duration / stats.tasks_completed as f64);
    }
    
    async fn update_stats_on_retry(&self) {
        let mut stats = self.stats.write().await;
        stats.tasks_retried += 1;
    }
    
    async fn update_stats_on_failure(&self) {
        let mut stats = self.stats.write().await;
        stats.tasks_failed += 1;
    }
    
    pub async fn is_running(&self) -> bool {
        *self.is_running.read().await
    }
    
    pub async fn get_task_count(&self) -> usize {
        let scheduled_tasks = self.scheduled_tasks.read().await;
        scheduled_tasks.len()
    }
    
    pub async fn schedule_urgent_task(&self, compaction_type: CompactionType) -> Result<u64> {
        self.schedule_task(compaction_type, TaskPriority::Critical, Some(Duration::from_secs(0))).await
    }
    
    pub async fn reschedule_all_tasks(&self, delay: Duration) -> Result<()> {
        let mut scheduled_tasks = self.scheduled_tasks.write().await;
        let mut tasks_vec: Vec<_> = scheduled_tasks.drain().collect();
        
        let new_time = Instant::now() + delay;
        for task in &mut tasks_vec {
            task.next_run = new_time;
        }
        
        for task in tasks_vec {
            scheduled_tasks.push(task);
        }
        
        Ok(())
    }
}

impl Clone for MaintenanceScheduler {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            is_running: self.is_running.clone(),
            scheduled_tasks: self.scheduled_tasks.clone(),
            stats: self.stats.clone(),
            next_task_id: self.next_task_id.clone(),
            shutdown_signal: self.shutdown_signal.clone(),
            scheduler_handle: self.scheduler_handle.clone(),
        }
    }
}

// We need to add rand dependency
use rand;