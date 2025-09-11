use super::{
    HistoryManager, Migration, MigrationContext, MigrationStatus, MigrationStep,
    MigrationValidator, MigrationVersion,
};
use crate::core::error::{DatabaseError, DatabaseResult};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};

#[derive(Debug)]
pub struct MigrationRunner {
    history_manager: HistoryManager,
    validator: MigrationValidator,
    active_migrations: Arc<RwLock<HashMap<MigrationVersion, MigrationStatus>>>,
}

impl Default for MigrationRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationRunner {
    pub fn new() -> Self {
        Self {
            history_manager: HistoryManager::new(),
            validator: MigrationValidator::new(),
            active_migrations: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn load_migrations_from_dir<P: AsRef<Path>>(&mut self, dir: P) -> DatabaseResult<()> {
        let dir_path = dir.as_ref();
        if !dir_path.exists() {
            std::fs::create_dir_all(dir_path).map_err(|e| DatabaseError::IoError(e.to_string()))?;
            return Ok(());
        }

        for entry in
            std::fs::read_dir(dir_path).map_err(|e| DatabaseError::IoError(e.to_string()))?
        {
            let entry = entry.map_err(|e| DatabaseError::IoError(e.to_string()))?;
            let path = entry.path();

            if path.extension().is_some_and(|ext| ext == "sql") {
                self.load_migration_file(&path)?;
            }
        }

        Ok(())
    }

    fn load_migration_file(&mut self, path: &Path) -> DatabaseResult<()> {
        let content =
            std::fs::read_to_string(path).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let _migration = self.parse_migration_file(&content, path)?;
        Ok(())
    }

    fn parse_migration_file(&self, content: &str, path: &Path) -> DatabaseResult<Migration> {
        let filename = path.file_stem().and_then(|s| s.to_str()).ok_or_else(|| {
            DatabaseError::MigrationError("Invalid migration filename".to_string())
        })?;

        let parts: Vec<&str> = filename.split('_').collect();
        if parts.len() < 2 {
            return Err(DatabaseError::MigrationError(
                "Migration filename must be in format: VERSION_NAME.sql".to_string(),
            ));
        }

        let version_str = parts[0];
        let version = version_str.parse::<u64>().map_err(|_| {
            DatabaseError::MigrationError(format!("Invalid version number: {}", version_str))
        })?;

        let name = parts[1..].join("_");

        let (up_script, down_script) = self.parse_migration_content(content)?;

        let metadata = super::MigrationMetadata {
            version: MigrationVersion::new(version),
            name,
            description: format!("Migration {}", version),
            migration_type: super::MigrationType::Schema,
            mode: super::MigrationMode::Offline,
            author: "system".to_string(),
            created_at: SystemTime::now(),
            dependencies: Vec::new(),
            reversible: down_script.is_some(),
            estimated_duration: None,
            checksum: super::calculate_checksum(content),
        };

        Ok(Migration {
            metadata,
            up_script,
            down_script,
            validation_script: None,
        })
    }

    fn parse_migration_content(&self, content: &str) -> DatabaseResult<(String, Option<String>)> {
        let mut up_script = String::new();
        let mut down_script: Option<String> = None;
        let mut current_section = "up";

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.starts_with("-- +migrate Up") {
                current_section = "up";
                continue;
            } else if trimmed.starts_with("-- +migrate Down") {
                current_section = "down";
                down_script = Some(String::new());
                continue;
            }

            match current_section {
                "up" => {
                    up_script.push_str(line);
                    up_script.push('\n');
                }
                "down" => {
                    if let Some(ref mut down) = down_script {
                        down.push_str(line);
                        down.push('\n');
                    }
                }
                _ => {}
            }
        }

        Ok((
            up_script.trim().to_string(),
            down_script.map(|s| s.trim().to_string()),
        ))
    }

    pub fn migrate_up(
        &mut self,
        ctx: &mut MigrationContext,
        migrations: &HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        let pending = self.get_pending_migrations(ctx, migrations)?;
        let mut applied = Vec::new();

        for migration in pending {
            self.execute_migration_up(ctx, migration)?;
            applied.push(migration.metadata.version);
        }

        Ok(applied)
    }

    pub fn migrate_down(
        &mut self,
        ctx: &mut MigrationContext,
        migrations: &HashMap<MigrationVersion, Migration>,
        steps: usize,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        let applied_migrations = self.get_applied_migrations(ctx, migrations)?;
        let to_rollback = applied_migrations
            .into_iter()
            .rev()
            .take(steps)
            .collect::<Vec<_>>();

        let mut rolled_back = Vec::new();

        for migration in to_rollback {
            self.execute_migration_down(ctx, migration)?;
            rolled_back.push(migration.metadata.version);
        }

        Ok(rolled_back)
    }

    pub fn migrate_to_version(
        &mut self,
        ctx: &mut MigrationContext,
        migrations: &HashMap<MigrationVersion, Migration>,
        target_version: MigrationVersion,
    ) -> DatabaseResult<Vec<MigrationVersion>> {
        let current_version = ctx.current_version;
        let mut applied = Vec::new();

        if target_version > current_version {
            ctx.target_version = target_version;
            let pending = self.get_pending_migrations(ctx, migrations)?;

            for migration in pending {
                if migration.metadata.version <= target_version {
                    self.execute_migration_up(ctx, migration)?;
                    applied.push(migration.metadata.version);
                } else {
                    break;
                }
            }
        } else if target_version < current_version {
            let applied_migrations = self.get_applied_migrations(ctx, migrations)?;

            for migration in applied_migrations.into_iter().rev() {
                if migration.metadata.version > target_version {
                    self.execute_migration_down(ctx, migration)?;
                    applied.push(migration.metadata.version);
                } else {
                    break;
                }
            }
        }

        Ok(applied)
    }

    pub fn rollback_migration(
        &mut self,
        ctx: &mut MigrationContext,
        migrations: &HashMap<MigrationVersion, Migration>,
        version: MigrationVersion,
    ) -> DatabaseResult<()> {
        if let Some(migration) = migrations.get(&version) {
            self.execute_migration_down(ctx, migration)?;
            Ok(())
        } else {
            Err(DatabaseError::MigrationError(format!(
                "Migration version {} not found",
                version
            )))
        }
    }

    fn execute_migration_up(
        &mut self,
        ctx: &mut MigrationContext,
        migration: &Migration,
    ) -> DatabaseResult<()> {
        let version = migration.metadata.version;

        {
            let mut active = self.active_migrations.write();
            active.insert(version, MigrationStatus::Running);
        }

        self.update_progress(ctx, version, "Starting migration", 0)?;

        if !ctx.dry_run {
            self.validate_migration(ctx, migration)?;
        }

        let start_time = Instant::now();

        let result = self.execute_migration_script(ctx, &migration.up_script, true);

        let duration = start_time.elapsed();

        match result {
            Ok(()) => {
                if !ctx.dry_run {
                    self.history_manager.record_migration_success(
                        &ctx.database_path,
                        version,
                        duration,
                    )?;
                    ctx.current_version = version;
                }

                {
                    let mut active = self.active_migrations.write();
                    active.insert(version, MigrationStatus::Completed);
                }

                self.update_progress(ctx, version, "Migration completed", 100)?;
                Ok(())
            }
            Err(e) => {
                if !ctx.dry_run {
                    self.history_manager.record_migration_failure(
                        &ctx.database_path,
                        version,
                        duration,
                        &e.to_string(),
                    )?;
                }

                {
                    let mut active = self.active_migrations.write();
                    active.insert(version, MigrationStatus::Failed);
                }

                self.update_progress(ctx, version, &format!("Migration failed: {}", e), 0)?;
                Err(e)
            }
        }
    }

    fn execute_migration_down(
        &mut self,
        ctx: &mut MigrationContext,
        migration: &Migration,
    ) -> DatabaseResult<()> {
        let version = migration.metadata.version;

        if let Some(ref down_script) = migration.down_script {
            {
                let mut active = self.active_migrations.write();
                active.insert(version, MigrationStatus::Running);
            }

            self.update_progress(ctx, version, "Starting rollback", 0)?;

            let start_time = Instant::now();

            let result = self.execute_migration_script(ctx, down_script, false);

            let duration = start_time.elapsed();

            match result {
                Ok(()) => {
                    if !ctx.dry_run {
                        self.history_manager.record_rollback_success(
                            &ctx.database_path,
                            version,
                            duration,
                        )?;

                        if let Some(prev_version) = version.prev() {
                            ctx.current_version = prev_version;
                        } else {
                            ctx.current_version = MigrationVersion::INITIAL;
                        }
                    }

                    {
                        let mut active = self.active_migrations.write();
                        active.insert(version, MigrationStatus::RolledBack);
                    }

                    self.update_progress(ctx, version, "Rollback completed", 100)?;
                    Ok(())
                }
                Err(e) => {
                    if !ctx.dry_run {
                        self.history_manager.record_rollback_failure(
                            &ctx.database_path,
                            version,
                            duration,
                            &e.to_string(),
                        )?;
                    }

                    {
                        let mut active = self.active_migrations.write();
                        active.insert(version, MigrationStatus::Failed);
                    }

                    self.update_progress(ctx, version, &format!("Rollback failed: {}", e), 0)?;
                    Err(e)
                }
            }
        } else {
            Err(DatabaseError::MigrationError(format!(
                "Migration {} is not reversible",
                version
            )))
        }
    }

    fn execute_migration_script(
        &self,
        ctx: &MigrationContext,
        script: &str,
        is_up: bool,
    ) -> DatabaseResult<()> {
        if ctx.dry_run {
            return Ok(());
        }

        let script_step = MigrationScriptStep::new(script.to_string(), is_up);
        let mut ctx_mut = ctx.clone();
        script_step.execute(&mut ctx_mut)
    }

    fn validate_migration(
        &self,
        ctx: &MigrationContext,
        migration: &Migration,
    ) -> DatabaseResult<()> {
        let result = self.validator.validate_migration(migration, ctx)?;

        if !result.is_valid {
            return Err(DatabaseError::MigrationError(format!(
                "Migration validation failed: {:?}",
                result.errors
            )));
        }

        Ok(())
    }

    fn get_pending_migrations<'a>(
        &self,
        ctx: &MigrationContext,
        migrations: &'a HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<Vec<&'a Migration>> {
        let current_version = ctx.current_version;
        let target_version = ctx.target_version;

        let mut pending: Vec<_> = migrations
            .iter()
            .filter(|(v, _)| **v > current_version && **v <= target_version)
            .map(|(_, m)| m)
            .collect();

        pending.sort_by_key(|m| m.metadata.version);
        Ok(pending)
    }

    fn get_applied_migrations<'a>(
        &self,
        ctx: &MigrationContext,
        migrations: &'a HashMap<MigrationVersion, Migration>,
    ) -> DatabaseResult<Vec<&'a Migration>> {
        let current_version = ctx.current_version;

        let mut applied: Vec<_> = migrations
            .iter()
            .filter(|(v, _)| **v <= current_version)
            .map(|(_, m)| m)
            .collect();

        applied.sort_by_key(|m| m.metadata.version);
        Ok(applied)
    }

    fn update_progress(
        &self,
        ctx: &MigrationContext,
        version: MigrationVersion,
        message: &str,
        percentage: u8,
    ) -> DatabaseResult<()> {
        let mut progress = ctx.progress.write();
        progress.update_migration_progress(version, message, percentage);
        Ok(())
    }

    pub fn get_migration_status(&self, version: MigrationVersion) -> Option<MigrationStatus> {
        let active = self.active_migrations.read();
        active.get(&version).cloned()
    }

    pub fn abort_migration(&self, version: MigrationVersion) -> DatabaseResult<()> {
        let mut active = self.active_migrations.write();
        if let Some(status) = active.get(&version) {
            if *status == MigrationStatus::Running {
                active.insert(version, MigrationStatus::Failed);
                Ok(())
            } else {
                Err(DatabaseError::MigrationError(format!(
                    "Migration {} is not currently running",
                    version
                )))
            }
        } else {
            Err(DatabaseError::MigrationError(format!(
                "Migration {} not found",
                version
            )))
        }
    }
}

impl Clone for MigrationContext {
    fn clone(&self) -> Self {
        Self {
            database_path: self.database_path.clone(),
            current_version: self.current_version,
            target_version: self.target_version,
            dry_run: self.dry_run,
            force: self.force,
            batch_size: self.batch_size,
            timeout: self.timeout,
            progress: Arc::clone(&self.progress),
            temp_data: self.temp_data.clone(),
        }
    }
}

struct MigrationScriptStep {
    script: String,
    is_up: bool,
}

impl MigrationScriptStep {
    fn new(script: String, is_up: bool) -> Self {
        Self { script, is_up }
    }
}

impl MigrationStep for MigrationScriptStep {
    fn execute(&self, ctx: &mut MigrationContext) -> DatabaseResult<()> {
        if ctx.dry_run {
            return Ok(());
        }

        let statements = self.parse_sql_statements(&self.script)?;

        for statement in statements {
            self.execute_sql_statement(&statement, ctx)?;
        }

        Ok(())
    }

    fn rollback(&self, _ctx: &mut MigrationContext) -> DatabaseResult<()> {
        Ok(())
    }

    fn validate(&self, _ctx: &MigrationContext) -> DatabaseResult<bool> {
        let statements = self.parse_sql_statements(&self.script)?;
        Ok(!statements.is_empty())
    }

    fn estimate_duration(&self) -> Option<Duration> {
        let statement_count = self.script.split(';').count();
        Some(Duration::from_millis(statement_count as u64 * 10))
    }
}

impl MigrationScriptStep {
    fn parse_sql_statements(&self, script: &str) -> DatabaseResult<Vec<String>> {
        let statements: Vec<String> = script
            .split(';')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty() && !s.starts_with("--"))
            .collect();

        Ok(statements)
    }

    fn execute_sql_statement(
        &self,
        statement: &str,
        _ctx: &MigrationContext,
    ) -> DatabaseResult<()> {
        if statement.trim().is_empty() {
            return Ok(());
        }

        Ok(())
    }
}
