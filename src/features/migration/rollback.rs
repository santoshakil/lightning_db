use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;
use crate::core::error::{DatabaseResult, DatabaseError};
use super::{
    MigrationVersion, Migration, MigrationContext, MigrationStatus,
    HistoryManager,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollbackStrategy {
    Immediate,
    Staged,
    Graceful,
    Emergency,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackPlan {
    pub strategy: RollbackStrategy,
    pub target_version: MigrationVersion,
    pub migrations_to_rollback: Vec<MigrationVersion>,
    pub estimated_duration: Duration,
    pub backup_required: bool,
    pub pre_rollback_checks: Vec<String>,
    pub post_rollback_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackState {
    pub current_phase: RollbackPhase,
    pub progress: u8,
    pub started_at: SystemTime,
    pub last_checkpoint: Option<MigrationVersion>,
    pub errors: Vec<String>,
    pub can_continue: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RollbackPhase {
    Planning,
    PreChecks,
    Backup,
    Executing,
    PostChecks,
    Cleanup,
    Completed,
    Failed,
}

pub struct RollbackManager {
    history_manager: HistoryManager,
    active_rollbacks: Arc<RwLock<HashMap<String, RollbackState>>>,
    rollback_plans: Arc<RwLock<HashMap<String, RollbackPlan>>>,
}

impl Default for RollbackManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RollbackManager {
    pub fn new() -> Self {
        Self {
            history_manager: HistoryManager::new(),
            active_rollbacks: Arc::new(RwLock::new(HashMap::new())),
            rollback_plans: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub fn create_rollback_plan(
        &self,
        db_path: &str,
        target_version: MigrationVersion,
        strategy: RollbackStrategy,
        migrations: &HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<RollbackPlan> {
        let current_version = self.get_current_version(db_path)?;
        
        if target_version >= current_version {
            return Err(DatabaseError::MigrationError(
                format!("Target version {} must be less than current version {}", 
                    target_version, current_version)
            ));
        }
        
        let applied_migrations = self.get_applied_migrations_since(
            db_path, target_version, current_version
        )?;
        
        let migrations_to_rollback = self.order_rollback_migrations(
            &applied_migrations, migrations
        )?;
        
        let estimated_duration = self.estimate_rollback_duration(
            &migrations_to_rollback, migrations
        );
        
        let backup_required = self.requires_backup(&strategy, &migrations_to_rollback, migrations);
        
        let pre_rollback_checks = self.generate_pre_rollback_checks(
            &migrations_to_rollback, migrations
        );
        
        let post_rollback_actions = self.generate_post_rollback_actions(
            &migrations_to_rollback, migrations
        );
        
        Ok(RollbackPlan {
            strategy,
            target_version,
            migrations_to_rollback,
            estimated_duration,
            backup_required,
            pre_rollback_checks,
            post_rollback_actions,
        })
    }
    
    pub fn execute_rollback(
        &mut self,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<()> {
        let rollback_id = format!("rollback_{}_{}", 
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
            plan.target_version
        );
        
        let mut state = RollbackState {
            current_phase: RollbackPhase::Planning,
            progress: 0,
            started_at: SystemTime::now(),
            last_checkpoint: None,
            errors: Vec::new(),
            can_continue: true,
        };
        
        {
            let mut active = self.active_rollbacks.write();
            active.insert(rollback_id.clone(), state.clone());
        }
        
        let result = self.execute_rollback_phases(ctx, plan, migrations, &rollback_id, &mut state);
        
        {
            let mut active = self.active_rollbacks.write();
            if let Some(final_state) = active.get_mut(&rollback_id) {
                match result {
                    Ok(()) => final_state.current_phase = RollbackPhase::Completed,
                    Err(ref e) => {
                        final_state.current_phase = RollbackPhase::Failed;
                        final_state.errors.push(e.to_string());
                        final_state.can_continue = false;
                    }
                }
            }
        }
        
        result
    }
    
    fn execute_rollback_phases(
        &mut self,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
        rollback_id: &str,
        state: &mut RollbackState,
    ) -> DatabaseResult<()> {
        self.update_rollback_state(rollback_id, RollbackPhase::PreChecks, 10);
        self.execute_pre_rollback_checks(ctx, plan)?;
        
        if plan.backup_required {
            self.update_rollback_state(rollback_id, RollbackPhase::Backup, 20);
            self.create_rollback_backup(ctx, plan)?;
        }
        
        self.update_rollback_state(rollback_id, RollbackPhase::Executing, 30);
        
        match plan.strategy {
            RollbackStrategy::Immediate => {
                self.execute_immediate_rollback(ctx, plan, migrations, rollback_id, state)?;
            },
            RollbackStrategy::Staged => {
                self.execute_staged_rollback(ctx, plan, migrations, rollback_id, state)?;
            },
            RollbackStrategy::Graceful => {
                self.execute_graceful_rollback(ctx, plan, migrations, rollback_id, state)?;
            },
            RollbackStrategy::Emergency => {
                self.execute_emergency_rollback(ctx, plan, migrations, rollback_id, state)?;
            },
        }
        
        self.update_rollback_state(rollback_id, RollbackPhase::PostChecks, 90);
        self.execute_post_rollback_actions(ctx, plan)?;
        
        self.update_rollback_state(rollback_id, RollbackPhase::Cleanup, 95);
        self.cleanup_rollback_artifacts(ctx, plan)?;
        
        self.update_rollback_state(rollback_id, RollbackPhase::Completed, 100);
        
        Ok(())
    }
    
    fn execute_immediate_rollback(
        &mut self,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
        rollback_id: &str,
        state: &mut RollbackState,
    ) -> DatabaseResult<()> {
        let total_migrations = plan.migrations_to_rollback.len();
        
        for (index, version) in plan.migrations_to_rollback.iter().enumerate() {
            if let Some(migration) = migrations.get(version) {
                self.rollback_single_migration(ctx, migration)?;
                
                state.last_checkpoint = Some(*version);
                let progress = 30 + ((index + 1) * 50 / total_migrations.max(1)) as u8;
                self.update_rollback_state(rollback_id, RollbackPhase::Executing, progress);
            }
        }
        
        Ok(())
    }
    
    fn execute_staged_rollback(
        &mut self,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
        rollback_id: &str,
        state: &mut RollbackState,
    ) -> DatabaseResult<()> {
        let chunk_size = 5;
        let chunks: Vec<&[MigrationVersion]> = plan.migrations_to_rollback
            .chunks(chunk_size)
            .collect();
        
        for (chunk_index, chunk) in chunks.iter().enumerate() {
            for version in chunk.iter() {
                if let Some(migration) = migrations.get(version) {
                    self.rollback_single_migration(ctx, migration)?;
                    state.last_checkpoint = Some(*version);
                }
            }
            
            let progress = 30 + ((chunk_index + 1) * 50 / chunks.len().max(1)) as u8;
            self.update_rollback_state(rollback_id, RollbackPhase::Executing, progress);
            
            if chunk_index < chunks.len() - 1 {
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        
        Ok(())
    }
    
    fn execute_graceful_rollback(
        &mut self,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
        rollback_id: &str,
        state: &mut RollbackState,
    ) -> DatabaseResult<()> {
        let total_migrations = plan.migrations_to_rollback.len();
        
        for (index, version) in plan.migrations_to_rollback.iter().enumerate() {
            if let Some(migration) = migrations.get(version) {
                self.validate_rollback_safety(migration)?;
                
                self.rollback_single_migration(ctx, migration)?;
                
                self.verify_rollback_success(ctx, migration)?;
                
                state.last_checkpoint = Some(*version);
                let progress = 30 + ((index + 1) * 50 / total_migrations.max(1)) as u8;
                self.update_rollback_state(rollback_id, RollbackPhase::Executing, progress);
                
                std::thread::sleep(Duration::from_millis(100));
            }
        }
        
        Ok(())
    }
    
    fn execute_emergency_rollback(
        &mut self,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
        rollback_id: &str,
        state: &mut RollbackState,
    ) -> DatabaseResult<()> {
        for version in &plan.migrations_to_rollback {
            if let Some(migration) = migrations.get(version) {
                if let Err(e) = self.rollback_single_migration(ctx, migration) {
                    state.errors.push(format!("Failed to rollback {}: {}", version, e));
                    continue;
                }
                state.last_checkpoint = Some(*version);
            }
        }
        
        self.update_rollback_state(rollback_id, RollbackPhase::Executing, 80);
        Ok(())
    }
    
    fn rollback_single_migration(
        &mut self,
        ctx: &mut MigrationContext,
        migration: &Migration,
    ) -> DatabaseResult<()> {
        if let Some(ref down_script) = migration.down_script {
            if !ctx.dry_run {
                self.execute_rollback_script(down_script, ctx)?;
                
                self.history_manager.record_rollback_success(
                    &ctx.database_path,
                    migration.metadata.version,
                    Duration::from_secs(1),
                )?;
            }
            
            Ok(())
        } else {
            Err(DatabaseError::MigrationError(
                format!("Migration {} is not reversible", migration.metadata.version)
            ))
        }
    }
    
    fn execute_rollback_script(&self, script: &str, _ctx: &MigrationContext) -> DatabaseResult<()> {
        let statements: Vec<&str> = script.split(';')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty() && !s.starts_with("--"))
            .collect();
        
        for statement in statements {
            if !statement.trim().is_empty() {
            }
        }
        
        Ok(())
    }
    
    fn validate_rollback_safety(&self, migration: &Migration) -> DatabaseResult<()> {
        if migration.down_script.is_none() {
            return Err(DatabaseError::MigrationError(
                format!("Migration {} is not reversible", migration.metadata.version)
            ));
        }
        
        Ok(())
    }
    
    fn verify_rollback_success(&self, _ctx: &MigrationContext, _migration: &Migration) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn execute_pre_rollback_checks(
        &self,
        _ctx: &MigrationContext,
        plan: &RollbackPlan,
    ) -> DatabaseResult<()> {
        for check in &plan.pre_rollback_checks {
            match check.as_str() {
                "verify_backup_exists" => self.verify_backup_exists()?,
                "check_disk_space" => self.check_disk_space()?,
                "validate_permissions" => self.validate_permissions()?,
                _ => {}
            }
        }
        Ok(())
    }
    
    fn create_rollback_backup(&self, _ctx: &MigrationContext, _plan: &RollbackPlan) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn execute_post_rollback_actions(
        &self,
        _ctx: &MigrationContext,
        plan: &RollbackPlan,
    ) -> DatabaseResult<()> {
        for action in &plan.post_rollback_actions {
            match action.as_str() {
                "rebuild_indexes" => self.rebuild_indexes()?,
                "update_statistics" => self.update_statistics()?,
                "verify_data_integrity" => self.verify_data_integrity()?,
                _ => {}
            }
        }
        Ok(())
    }
    
    fn cleanup_rollback_artifacts(&self, _ctx: &MigrationContext, _plan: &RollbackPlan) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn get_current_version(&self, db_path: &str) -> DatabaseResult<MigrationVersion> {
        self.history_manager.get_current_version(db_path)
    }
    
    fn get_applied_migrations_since(
        &self,
        db_path: &str,
        from_version: MigrationVersion,
        to_version: MigrationVersion,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        let history = self.history_manager.get_history(db_path)?;
        
        let applied: Vec<MigrationVersion> = history
            .iter()
            .filter(|entry| {
                entry.version > from_version && 
                entry.version <= to_version &&
                entry.status == MigrationStatus::Completed
            })
            .map(|entry| entry.version)
            .collect();
        
        Ok(applied)
    }
    
    fn order_rollback_migrations(
        &self,
        applied_migrations: &[MigrationVersion],
        migrations: &HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        let mut ordered = applied_migrations.to_vec();
        ordered.sort_by(|a, b| b.cmp(a));
        
        for version in &ordered {
            if let Some(migration) = migrations.get(version) {
                if migration.down_script.is_none() {
                    return Err(DatabaseError::MigrationError(
                        format!("Migration {} is not reversible", version)
                    ));
                }
            }
        }
        
        Ok(ordered)
    }
    
    fn estimate_rollback_duration(
        &self,
        migrations_to_rollback: &[MigrationVersion],
        migrations: &HashMap<MigrationVersion, Migration>,
    ) -> Duration {
        let total_estimate: Duration = migrations_to_rollback
            .iter()
            .filter_map(|version| migrations.get(version))
            .filter_map(|migration| migration.metadata.estimated_duration)
            .sum();
        
        total_estimate + Duration::from_secs(migrations_to_rollback.len() as u64 * 5)
    }
    
    fn requires_backup(
        &self,
        strategy: &RollbackStrategy,
        _migrations_to_rollback: &[MigrationVersion],
        _migrations: &HashMap<MigrationVersion, Migration>,
    ) -> bool {
        matches!(strategy, RollbackStrategy::Graceful | RollbackStrategy::Staged)
    }
    
    fn generate_pre_rollback_checks(
        &self,
        _migrations_to_rollback: &[MigrationVersion],
        _migrations: &HashMap<MigrationVersion, Migration>,
    ) -> Vec<String> {
        vec![
            "verify_backup_exists".to_string(),
            "check_disk_space".to_string(),
            "validate_permissions".to_string(),
        ]
    }
    
    fn generate_post_rollback_actions(
        &self,
        _migrations_to_rollback: &[MigrationVersion],
        _migrations: &HashMap<MigrationVersion, Migration>,
    ) -> Vec<String> {
        vec![
            "rebuild_indexes".to_string(),
            "update_statistics".to_string(),
            "verify_data_integrity".to_string(),
        ]
    }
    
    fn update_rollback_state(&self, rollback_id: &str, phase: RollbackPhase, progress: u8) {
        let mut active = self.active_rollbacks.write();
        if let Some(state) = active.get_mut(rollback_id) {
            state.current_phase = phase;
            state.progress = progress;
        }
    }
    
    fn verify_backup_exists(&self) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn check_disk_space(&self) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn validate_permissions(&self) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn rebuild_indexes(&self) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn update_statistics(&self) -> DatabaseResult<()> {
        Ok(())
    }
    
    fn verify_data_integrity(&self) -> DatabaseResult<()> {
        Ok(())
    }
    
    pub fn get_rollback_state(&self, rollback_id: &str) -> Option<RollbackState> {
        let active = self.active_rollbacks.read();
        active.get(rollback_id).cloned()
    }
    
    pub fn abort_rollback(&self, rollback_id: &str) -> DatabaseResult<()> {
        let mut active = self.active_rollbacks.write();
        if let Some(state) = active.get_mut(rollback_id) {
            if matches!(state.current_phase, RollbackPhase::Executing) {
                state.current_phase = RollbackPhase::Failed;
                state.can_continue = false;
                state.errors.push("Rollback aborted by user".to_string());
                Ok(())
            } else {
                Err(DatabaseError::MigrationError(
                    "Cannot abort rollback in current phase".to_string()
                ))
            }
        } else {
            Err(DatabaseError::MigrationError(
                format!("Rollback {} not found", rollback_id)
            ))
        }
    }
    
    pub fn resume_rollback(
        &mut self,
        rollback_id: &str,
        ctx: &mut MigrationContext,
        plan: &RollbackPlan,
        migrations: &HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<()> {
        let state = {
            let active = self.active_rollbacks.read();
            active.get(rollback_id).cloned()
        };
        
        if let Some(mut state) = state {
            if !state.can_continue {
                return Err(DatabaseError::MigrationError(
                    "Rollback cannot be resumed due to previous errors".to_string()
                ));
            }
            
            let remaining_migrations = if let Some(checkpoint) = state.last_checkpoint {
                plan.migrations_to_rollback.iter()
                    .skip_while(|&&v| v != checkpoint)
                    .skip(1)
                    .copied()
                    .collect()
            } else {
                plan.migrations_to_rollback.clone()
            };
            
            let mut resumed_plan = plan.clone();
            resumed_plan.migrations_to_rollback = remaining_migrations;
            
            self.execute_rollback_phases(ctx, &resumed_plan, migrations, rollback_id, &mut state)
        } else {
            Err(DatabaseError::MigrationError(
                format!("Rollback {} not found", rollback_id)
            ))
        }
    }
}