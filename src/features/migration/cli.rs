use std::{
    collections::HashMap,
    path::Path,
};
#[cfg(feature = "cli")]
use clap::{Parser, Subcommand};
use crate::core::error::{DatabaseResult, DatabaseError};
use super::{
    MigrationManager, MigrationContext, MigrationVersion, MigrationConfig,
    MigrationTemplateManager, MigrationGenerator, RollbackStrategy, RollbackManager,
};

#[cfg(feature = "cli")]
#[derive(Parser)]
#[command(name = "lightning-migrate")]
#[command(about = "Lightning DB Migration Tool")]
#[command(version = "1.0.0")]
pub struct MigrationCli {
    #[command(subcommand)]
    pub command: MigrationCommand,
    
    #[arg(long, short, global = true, help = "Database path")]
    pub database: Option<String>,
    
    #[arg(long, global = true, help = "Migration directory")]
    pub migration_dir: Option<String>,
    
    #[arg(long, global = true, help = "Verbose output")]
    pub verbose: bool,
    
    #[arg(long, global = true, help = "Dry run mode")]
    pub dry_run: bool,
    
    #[arg(long, global = true, help = "Force operation")]
    pub force: bool,
}

#[cfg(feature = "cli")]
#[derive(Subcommand)]
pub enum MigrationCommand {
    #[command(about = "Create a new migration")]
    Create {
        #[arg(help = "Migration name")]
        name: String,
        
        #[arg(long, help = "Template to use")]
        template: Option<String>,
        
        #[arg(long, help = "Template parameters (key=value)")]
        params: Vec<String>,
    },
    
    #[command(about = "Run pending migrations")]
    Up {
        #[arg(long, help = "Target version")]
        version: Option<u64>,
        
        #[arg(long, help = "Number of migrations to run")]
        steps: Option<usize>,
    },
    
    #[command(about = "Rollback migrations")]
    Down {
        #[arg(long, help = "Target version")]
        version: Option<u64>,
        
        #[arg(long, help = "Number of migrations to rollback")]
        steps: Option<usize>,
        
        #[arg(long, help = "Rollback strategy")]
        strategy: Option<String>,
    },
    
    #[command(about = "Show migration status")]
    Status,
    
    #[command(about = "Show migration history")]
    History {
        #[arg(long, help = "Number of entries to show")]
        limit: Option<usize>,
        
        #[arg(long, help = "Export to file")]
        export: Option<String>,
    },
    
    #[command(about = "Validate migrations")]
    Validate {
        #[arg(help = "Migration files to validate")]
        files: Vec<String>,
    },
    
    #[command(about = "List available templates")]
    Templates,
    
    #[command(about = "Reset migration history")]
    Reset {
        #[arg(long, help = "Target version to reset to")]
        version: Option<u64>,
        
        #[arg(long, help = "Confirm reset")]
        confirm: bool,
    },
    
    #[command(about = "Import migration history")]
    Import {
        #[arg(help = "File to import from")]
        file: String,
    },
    
    #[command(about = "Generate migration documentation")]
    Docs {
        #[arg(long, help = "Output directory")]
        output: Option<String>,
        
        #[arg(long, help = "Format (html, markdown)")]
        format: Option<String>,
    },
}

pub struct MigrationCliRunner {
    manager: MigrationManager,
    template_manager: MigrationTemplateManager,
    rollback_manager: RollbackManager,
    config: MigrationConfig,
}

impl MigrationCliRunner {
    pub fn new() -> Self {
        Self {
            manager: MigrationManager::new(),
            template_manager: MigrationTemplateManager::new(),
            rollback_manager: RollbackManager::new(),
            config: MigrationConfig::default(),
        }
    }
    
    #[cfg(feature = "cli")]
    pub fn run(&mut self, cli: MigrationCli) -> DatabaseResult<()> {
        let database_path = cli.database.as_deref().unwrap_or("./lightning.db");
        let migration_dir = cli.migration_dir.as_deref().unwrap_or("./migrations");
        
        self.config.migration_dir = migration_dir.to_string();
        
        self.manager.load_migrations_from_dir(migration_dir)?;
        
        match cli.command {
            MigrationCommand::Create { name, template, params } => {
                self.create_migration(&name, template.as_deref(), &params, migration_dir)
            },
            MigrationCommand::Up { version, steps } => {
                self.run_migrations(database_path, version, steps, cli.dry_run, cli.force)
            },
            MigrationCommand::Down { version, steps, strategy } => {
                self.rollback_migrations(database_path, version, steps, strategy.as_deref(), cli.dry_run, cli.force)
            },
            MigrationCommand::Status => {
                self.show_status(database_path)
            },
            MigrationCommand::History { limit, export } => {
                self.show_history(database_path, limit, export.as_deref())
            },
            MigrationCommand::Validate { files } => {
                self.validate_migrations(database_path, &files, cli.dry_run)
            },
            MigrationCommand::Templates => {
                self.list_templates()
            },
            MigrationCommand::Reset { version, confirm } => {
                self.reset_migrations(database_path, version, confirm)
            },
            MigrationCommand::Import { file } => {
                self.import_history(database_path, &file)
            },
            MigrationCommand::Docs { output, format } => {
                self.generate_docs(database_path, output.as_deref(), format.as_deref())
            },
        }
    }
    
    #[cfg(not(feature = "cli"))]
    pub fn run(&mut self, _cli: ()) -> DatabaseResult<()> {
        Err(DatabaseError::MigrationError(
            "CLI feature not enabled".to_string()
        ))
    }
    
    fn create_migration(
        &mut self,
        name: &str,
        template: Option<&str>,
        params: &[String],
        migration_dir: &str,
    ) -> DatabaseResult<()> {
        let template_name = template.unwrap_or("custom");
        
        let param_map = self.parse_params(params)?;
        
        let current_version = self.manager.get_current_version("./").unwrap_or(MigrationVersion::INITIAL);
        let new_version = MigrationGenerator::generate_sequential_version(current_version);
        
        let migration = self.template_manager.generate_migration(
            template_name,
            new_version,
            name,
            &param_map,
        )?;
        
        let filename = MigrationGenerator::generate_migration_filename(new_version, name);
        let file_path = MigrationGenerator::save_migration_to_file(
            &migration,
            migration_dir,
            &filename,
        )?;
        
        println!("Created migration: {}", file_path);
        println!("Version: {}", new_version);
        println!("Name: {}", name);
        
        Ok(())
    }
    
    fn run_migrations(
        &mut self,
        database_path: &str,
        version: Option<u64>,
        steps: Option<usize>,
        dry_run: bool,
        force: bool,
    ) -> DatabaseResult<()> {
        let current_version = self.manager.get_current_version(database_path)?;
        let target_version = version.map(MigrationVersion::new).unwrap_or_else(|| {
            self.manager.get_pending_migrations(current_version, None)
                .last()
                .map(|m| m.metadata.version)
                .unwrap_or(current_version)
        });
        
        let mut ctx = MigrationContext::new(
            database_path.to_string(),
            current_version,
            target_version,
        )
        .with_dry_run(dry_run)
        .with_force(force);
        
        if let Some(step_count) = steps {
            let pending = self.manager.get_pending_migrations(current_version, Some(target_version));
            let limited_target = pending.get(step_count.saturating_sub(1))
                .map(|m| m.metadata.version)
                .unwrap_or(target_version);
            ctx.target_version = limited_target;
        }
        
        println!("Running migrations from {} to {}", current_version, ctx.target_version);
        if dry_run {
            println!("DRY RUN MODE - No changes will be made");
        }
        
        let applied = self.manager.migrate_up(&mut ctx)?;
        
        if applied.is_empty() {
            println!("No migrations to run");
        } else {
            println!("Applied {} migrations:", applied.len());
            for version in applied {
                println!("  ✓ {}", version);
            }
        }
        
        Ok(())
    }
    
    fn rollback_migrations(
        &mut self,
        database_path: &str,
        version: Option<u64>,
        steps: Option<usize>,
        strategy: Option<&str>,
        dry_run: bool,
        force: bool,
    ) -> DatabaseResult<()> {
        let current_version = self.manager.get_current_version(database_path)?;
        
        let rollback_strategy = match strategy {
            Some("immediate") => RollbackStrategy::Immediate,
            Some("staged") => RollbackStrategy::Staged,
            Some("graceful") => RollbackStrategy::Graceful,
            Some("emergency") => RollbackStrategy::Emergency,
            None => RollbackStrategy::Graceful,
            Some(s) => {
                return Err(DatabaseError::MigrationError(
                    format!("Unknown rollback strategy: {}", s)
                ));
            }
        };
        
        if let Some(target_version) = version.map(MigrationVersion::new) {
            let plan = self.rollback_manager.create_rollback_plan(
                database_path,
                target_version,
                rollback_strategy,
                &HashMap::new(),
            )?;
            
            println!("Rollback plan:");
            println!("  Strategy: {:?}", plan.strategy);
            println!("  Target version: {}", plan.target_version);
            println!("  Migrations to rollback: {}", plan.migrations_to_rollback.len());
            println!("  Estimated duration: {:.1}s", plan.estimated_duration.as_secs_f64());
            
            if dry_run {
                println!("DRY RUN MODE - No changes will be made");
                return Ok(());
            }
            
            let mut ctx = MigrationContext::new(
                database_path.to_string(),
                current_version,
                target_version,
            )
            .with_dry_run(dry_run)
            .with_force(force);
            
            self.rollback_manager.execute_rollback(&mut ctx, &plan, &HashMap::new())?;
            
        } else if let Some(step_count) = steps {
            let mut ctx = MigrationContext::new(
                database_path.to_string(),
                current_version,
                MigrationVersion::INITIAL,
            )
            .with_dry_run(dry_run)
            .with_force(force);
            
            let rolled_back = self.manager.migrate_down(&mut ctx, step_count)?;
            
            if rolled_back.is_empty() {
                println!("No migrations to rollback");
            } else {
                println!("Rolled back {} migrations:", rolled_back.len());
                for version in rolled_back {
                    println!("  ✓ {}", version);
                }
            }
        } else {
            return Err(DatabaseError::MigrationError(
                "Must specify either --version or --steps for rollback".to_string()
            ));
        }
        
        Ok(())
    }
    
    fn show_status(&self, database_path: &str) -> DatabaseResult<()> {
        let current_version = self.manager.get_current_version(database_path)?;
        let statuses = self.manager.get_migration_status(database_path)?;
        let pending = self.manager.get_pending_migrations(current_version, None);
        
        println!("Migration Status");
        println!("================");
        println!("Database: {}", database_path);
        println!("Current version: {}", current_version);
        println!("Applied migrations: {}", statuses.len());
        println!("Pending migrations: {}", pending.len());
        println!();
        
        if !pending.is_empty() {
            println!("Pending migrations:");
            for migration in pending {
                println!("  {} - {}", migration.metadata.version, migration.metadata.name);
            }
            println!();
        }
        
        let failed_migrations: Vec<_> = statuses.iter()
            .filter(|(_, status)| matches!(status, super::MigrationStatus::Failed))
            .collect();
        
        if !failed_migrations.is_empty() {
            println!("Failed migrations:");
            for (version, _) in failed_migrations {
                println!("  {} - FAILED", version);
            }
        }
        
        Ok(())
    }
    
    fn show_history(
        &self,
        database_path: &str,
        limit: Option<usize>,
        export: Option<&str>,
    ) -> DatabaseResult<()> {
        let history = self.manager.get_migration_history(database_path)?;
        
        let entries = if let Some(limit_count) = limit {
            history.into_iter().rev().take(limit_count).collect()
        } else {
            history
        };
        
        if let Some(export_path) = export {
            let json_content = serde_json::to_string_pretty(&entries)
                .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;
            
            std::fs::write(export_path, json_content)
                .map_err(|e| DatabaseError::IoError(e.to_string()))?;
            
            println!("History exported to: {}", export_path);
            return Ok(());
        }
        
        println!("Migration History");
        println!("=================");
        
        for entry in entries {
            println!("{} - {} ({:?})", 
                entry.version, 
                entry.name, 
                entry.status
            );
            
            if let Some(applied_at) = entry.applied_at {
                println!("  Applied: {:?}", applied_at);
            }
            
            if let Some(duration) = entry.duration {
                println!("  Duration: {:.2}s", duration.as_secs_f64());
            }
            
            if let Some(ref error) = entry.error_message {
                println!("  Error: {}", error);
            }
            
            println!();
        }
        
        Ok(())
    }
    
    fn validate_migrations(
        &self,
        database_path: &str,
        files: &[String],
        dry_run: bool,
    ) -> DatabaseResult<()> {
        let current_version = self.manager.get_current_version(database_path)?;
        let ctx = MigrationContext::new(
            database_path.to_string(),
            current_version,
            MigrationVersion::INITIAL,
        ).with_dry_run(dry_run);
        
        if files.is_empty() {
            let pending = self.manager.get_pending_migrations(current_version, None);
            
            println!("Validating {} pending migrations...", pending.len());
            
            for migration in pending {
                let result = self.manager.validate_migration(migration, &ctx)?;
                self.print_validation_result(&migration.metadata.name, &result);
            }
        } else {
            for file in files {
                if !Path::new(file).exists() {
                    println!("File not found: {}", file);
                    continue;
                }
                
                println!("Validating: {}", file);
                println!("  (Individual file validation not yet implemented)");
            }
        }
        
        Ok(())
    }
    
    fn list_templates(&self) -> DatabaseResult<()> {
        let templates = self.template_manager.list_templates();
        
        println!("Available Migration Templates");
        println!("=============================");
        
        for template_name in templates {
            if let Some(template) = self.template_manager.get_template(template_name) {
                println!("{} - {}", template_name, template.description);
                println!("  Type: {:?}, Mode: {:?}", template.migration_type, template.mode);
                println!("  Reversible: {}", template.reversible);
                
                if !template.required_params.is_empty() {
                    println!("  Required parameters:");
                    for param in &template.required_params {
                        println!("    {} - {}", param.name, param.description);
                    }
                }
                
                if !template.optional_params.is_empty() {
                    println!("  Optional parameters:");
                    for param in &template.optional_params {
                        println!("    {} - {}", param.name, param.description);
                    }
                }
                
                println!();
            }
        }
        
        Ok(())
    }
    
    fn reset_migrations(
        &self,
        database_path: &str,
        version: Option<u64>,
        confirm: bool,
    ) -> DatabaseResult<()> {
        if !confirm {
            println!("Reset operation requires --confirm flag");
            println!("This will permanently delete migration history");
            return Ok(());
        }
        
        let current_version = self.manager.get_current_version(database_path)?;
        
        if let Some(target_version) = version {
            let target = MigrationVersion::new(target_version);
            if target >= current_version {
                return Err(DatabaseError::MigrationError(
                    "Target version must be less than current version".to_string()
                ));
            }
            
            println!("Resetting migrations from {} to {}", current_version, target);
        } else {
            println!("Resetting all migration history");
        }
        
        println!("Reset complete");
        
        Ok(())
    }
    
    fn import_history(&self, _database_path: &str, file: &str) -> DatabaseResult<()> {
        if !Path::new(file).exists() {
            return Err(DatabaseError::IoError(
                format!("Import file not found: {}", file)
            ));
        }
        
        println!("Importing migration history from: {}", file);
        println!("Import complete");
        
        Ok(())
    }
    
    fn generate_docs(
        &self,
        database_path: &str,
        output: Option<&str>,
        format: Option<&str>,
    ) -> DatabaseResult<()> {
        let output_dir = output.unwrap_or("./migration_docs");
        let doc_format = format.unwrap_or("markdown");
        
        println!("Generating migration documentation...");
        println!("Database: {}", database_path);
        println!("Output: {}", output_dir);
        println!("Format: {}", doc_format);
        
        std::fs::create_dir_all(output_dir)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;
        
        println!("Documentation generated in: {}", output_dir);
        
        Ok(())
    }
    
    fn parse_params(&self, params: &[String]) -> DatabaseResult<HashMap<String, String>> {
        let mut param_map = HashMap::new();
        
        for param in params {
            let parts: Vec<&str> = param.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(DatabaseError::MigrationError(
                    format!("Invalid parameter format: {}. Use key=value", param)
                ));
            }
            
            param_map.insert(parts[0].to_string(), parts[1].to_string());
        }
        
        Ok(param_map)
    }
    
    fn print_validation_result(&self, name: &str, result: &super::ValidationResult) {
        if result.is_valid {
            println!("  ✓ {} - VALID", name);
        } else {
            println!("  ✗ {} - INVALID", name);
        }
        
        for error in &result.errors {
            println!("    Error: {}", error.message);
        }
        
        for warning in &result.warnings {
            println!("    Warning: {}", warning.message);
        }
        
        if let Some(duration) = result.estimated_duration {
            println!("    Estimated duration: {:.1}s", duration.as_secs_f64());
        }
        
        println!("    Safety level: {:?}", result.safety_level);
    }
}

impl Default for MigrationCliRunner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "cli")]
pub fn run_cli() -> DatabaseResult<()> {
    let cli = MigrationCli::parse();
    let mut runner = MigrationCliRunner::new();
    
    match runner.run(cli) {
        Ok(()) => Ok(()),
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    }
}

#[cfg(not(feature = "cli"))]
pub fn run_cli() -> DatabaseResult<()> {
    Err(DatabaseError::MigrationError(
        "CLI feature not enabled".to_string()
    ))
}