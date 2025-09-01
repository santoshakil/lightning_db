use std::{
    collections::HashSet,
    time::Duration,
};
use serde::{Deserialize, Serialize};
use crate::core::error::DatabaseResult;
use super::{Migration, MigrationContext, MigrationType, MigrationMode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<ValidationWarning>,
    pub recommendations: Vec<String>,
    pub estimated_duration: Option<Duration>,
    pub safety_level: SafetyLevel,
}

impl ValidationResult {
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            recommendations: Vec::new(),
            estimated_duration: None,
            safety_level: SafetyLevel::Safe,
        }
    }
    
    pub fn invalid(errors: Vec<ValidationError>) -> Self {
        Self {
            is_valid: false,
            errors,
            warnings: Vec::new(),
            recommendations: Vec::new(),
            estimated_duration: None,
            safety_level: SafetyLevel::Unsafe,
        }
    }
    
    pub fn with_warnings(mut self, warnings: Vec<ValidationWarning>) -> Self {
        self.warnings = warnings;
        self
    }
    
    pub fn with_recommendations(mut self, recommendations: Vec<String>) -> Self {
        self.recommendations = recommendations;
        self
    }
    
    pub fn with_duration(mut self, duration: Duration) -> Self {
        self.estimated_duration = Some(duration);
        self
    }
    
    pub fn with_safety_level(mut self, level: SafetyLevel) -> Self {
        self.safety_level = level;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SafetyLevel {
    Safe,
    Caution,
    Risky,
    Unsafe,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub code: String,
    pub message: String,
    pub severity: ErrorSeverity,
    pub line: Option<usize>,
    pub column: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ErrorSeverity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationWarning {
    pub code: String,
    pub message: String,
    pub recommendation: Option<String>,
}

#[derive(Debug)]
pub struct MigrationValidator {
    rules: Vec<Box<dyn ValidationRule + Send + Sync>>,
    safety_checks: Vec<Box<dyn SafetyCheck + Send + Sync>>,
}

impl Default for MigrationValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl MigrationValidator {
    pub fn new() -> Self {
        let mut validator = Self {
            rules: Vec::new(),
            safety_checks: Vec::new(),
        };
        
        validator.add_default_rules();
        validator.add_default_safety_checks();
        validator
    }
    
    fn add_default_rules(&mut self) {
        self.rules.push(Box::new(SyntaxValidationRule));
        self.rules.push(Box::new(DependencyValidationRule));
        self.rules.push(Box::new(ChecksumValidationRule));
        self.rules.push(Box::new(VersionValidationRule));
        self.rules.push(Box::new(ReversibilityValidationRule));
    }
    
    fn add_default_safety_checks(&mut self) {
        self.safety_checks.push(Box::new(DataLossSafetyCheck));
        self.safety_checks.push(Box::new(PerformanceSafetyCheck));
        self.safety_checks.push(Box::new(CompatibilitySafetyCheck));
        self.safety_checks.push(Box::new(ResourceUsageSafetyCheck));
    }
    
    pub fn add_rule(&mut self, rule: Box<dyn ValidationRule + Send + Sync>) {
        self.rules.push(rule);
    }
    
    pub fn add_safety_check(&mut self, check: Box<dyn SafetyCheck + Send + Sync>) {
        self.safety_checks.push(check);
    }
    
    pub fn validate_migration(
        &self,
        migration: &Migration,
        ctx: &MigrationContext,
    ) -> DatabaseResult<ValidationResult> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();
        
        for rule in &self.rules {
            match rule.validate(migration, ctx) {
                Ok(rule_result) => {
                    errors.extend(rule_result.errors);
                    warnings.extend(rule_result.warnings);
                    recommendations.extend(rule_result.recommendations);
                },
                Err(e) => {
                    errors.push(ValidationError {
                        code: "VALIDATION_RULE_ERROR".to_string(),
                        message: format!("Validation rule failed: {}", e),
                        severity: ErrorSeverity::High,
                        line: None,
                        column: None,
                    });
                }
            }
        }
        
        let mut safety_level = SafetyLevel::Safe;
        
        for safety_check in &self.safety_checks {
            match safety_check.check(migration, ctx) {
                Ok(check_result) => {
                    if check_result.level > safety_level as u8 {
                        safety_level = match check_result.level {
                            1 => SafetyLevel::Caution,
                            2 => SafetyLevel::Risky,
                            3 => SafetyLevel::Unsafe,
                            _ => SafetyLevel::Safe,
                        };
                    }
                    
                    warnings.extend(check_result.warnings);
                    recommendations.extend(check_result.recommendations);
                },
                Err(e) => {
                    warnings.push(ValidationWarning {
                        code: "SAFETY_CHECK_ERROR".to_string(),
                        message: format!("Safety check failed: {}", e),
                        recommendation: Some("Review safety check configuration".to_string()),
                    });
                }
            }
        }
        
        let is_valid = errors.is_empty() || errors.iter().all(|e| 
            matches!(e.severity, ErrorSeverity::Low | ErrorSeverity::Medium)
        );
        
        let estimated_duration = self.estimate_migration_duration(migration, ctx);
        
        Ok(ValidationResult {
            is_valid,
            errors,
            warnings,
            recommendations,
            estimated_duration,
            safety_level,
        })
    }
    
    pub fn validate_migration_sequence(
        &self,
        migrations: &[&Migration],
        _ctx: &MigrationContext,
    ) -> DatabaseResult<ValidationResult> {
        let mut sequence_errors = Vec::new();
        let mut sequence_warnings = Vec::new();
        let sequence_recommendations = Vec::new();
        
        let mut versions = HashSet::new();
        for migration in migrations {
            if versions.contains(&migration.metadata.version) {
                sequence_errors.push(ValidationError {
                    code: "DUPLICATE_VERSION".to_string(),
                    message: format!("Duplicate migration version: {}", migration.metadata.version),
                    severity: ErrorSeverity::Critical,
                    line: None,
                    column: None,
                });
            }
            versions.insert(migration.metadata.version);
        }
        
        for (i, migration) in migrations.iter().enumerate() {
            for dep_version in &migration.metadata.dependencies {
                let dep_exists = migrations.iter().take(i).any(|m| m.metadata.version == *dep_version);
                if !dep_exists {
                    sequence_errors.push(ValidationError {
                        code: "MISSING_DEPENDENCY".to_string(),
                        message: format!(
                            "Migration {} depends on {} which is not found or comes later",
                            migration.metadata.version, dep_version
                        ),
                        severity: ErrorSeverity::Critical,
                        line: None,
                        column: None,
                    });
                }
            }
        }
        
        let total_duration = migrations.iter()
            .filter_map(|m| m.metadata.estimated_duration)
            .fold(Duration::ZERO, |acc, d| acc + d);
        
        if total_duration > Duration::from_secs(3600) {
            sequence_warnings.push(ValidationWarning {
                code: "LONG_MIGRATION_SEQUENCE".to_string(),
                message: format!("Migration sequence estimated to take {:.1} hours", 
                    total_duration.as_secs_f64() / 3600.0),
                recommendation: Some("Consider breaking into smaller batches".to_string()),
            });
        }
        
        let is_valid = sequence_errors.is_empty();
        
        Ok(ValidationResult {
            is_valid,
            errors: sequence_errors,
            warnings: sequence_warnings,
            recommendations: sequence_recommendations,
            estimated_duration: Some(total_duration),
            safety_level: SafetyLevel::Safe,
        })
    }
    
    fn estimate_migration_duration(&self, migration: &Migration, _ctx: &MigrationContext) -> Option<Duration> {
        if let Some(estimated) = migration.metadata.estimated_duration {
            return Some(estimated);
        }
        
        let script_lines = migration.up_script.lines().count();
        let base_duration = Duration::from_millis(script_lines as u64 * 10);
        
        let type_multiplier = match migration.metadata.migration_type {
            MigrationType::Schema => 1.0,
            MigrationType::Data => 5.0,
            MigrationType::Index => 3.0,
            MigrationType::Maintenance => 0.5,
        };
        
        let mode_multiplier = match migration.metadata.mode {
            MigrationMode::Online => 1.5,
            MigrationMode::Offline => 1.0,
            MigrationMode::ZeroDowntime => 2.0,
        };
        
        let total_millis = (base_duration.as_millis() as f64 * type_multiplier * mode_multiplier) as u64;
        Some(Duration::from_millis(total_millis))
    }
    
    pub fn dry_run_validation(
        &self,
        migration: &Migration,
        ctx: &MigrationContext,
    ) -> DatabaseResult<ValidationResult> {
        let mut dry_run_ctx = ctx.clone();
        dry_run_ctx.dry_run = true;
        
        self.validate_migration(migration, &dry_run_ctx)
    }
}

pub trait ValidationRule: std::fmt::Debug {
    fn validate(&self, migration: &Migration, ctx: &MigrationContext) -> DatabaseResult<ValidationResult>;
}

pub trait SafetyCheck: std::fmt::Debug {
    fn check(&self, migration: &Migration, ctx: &MigrationContext) -> DatabaseResult<SafetyCheckResult>;
}

#[derive(Debug)]
pub struct SafetyCheckResult {
    pub level: u8,
    pub warnings: Vec<ValidationWarning>,
    pub recommendations: Vec<String>,
}

#[derive(Debug)]
pub struct SyntaxValidationRule;

impl ValidationRule for SyntaxValidationRule {
    fn validate(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<ValidationResult> {
        let mut errors = Vec::new();
        
        if migration.up_script.trim().is_empty() {
            errors.push(ValidationError {
                code: "EMPTY_UP_SCRIPT".to_string(),
                message: "Migration up script is empty".to_string(),
                severity: ErrorSeverity::Critical,
                line: None,
                column: None,
            });
        }
        
        if migration.metadata.reversible && migration.down_script.as_ref().is_none_or(|s| s.trim().is_empty()) {
            errors.push(ValidationError {
                code: "EMPTY_DOWN_SCRIPT".to_string(),
                message: "Migration marked as reversible but down script is empty".to_string(),
                severity: ErrorSeverity::High,
                line: None,
                column: None,
            });
        }
        
        self.validate_sql_syntax(&migration.up_script, &mut errors);
        
        if let Some(ref down_script) = migration.down_script {
            self.validate_sql_syntax(down_script, &mut errors);
        }
        
        if errors.is_empty() {
            Ok(ValidationResult::valid())
        } else {
            Ok(ValidationResult::invalid(errors))
        }
    }
}

impl SyntaxValidationRule {
    fn validate_sql_syntax(&self, script: &str, errors: &mut Vec<ValidationError>) {
        let lines: Vec<&str> = script.lines().collect();
        
        for (line_no, line) in lines.iter().enumerate() {
            let trimmed = line.trim();
            
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }
            
            if trimmed.contains("DROP TABLE") && !trimmed.to_uppercase().contains("IF EXISTS") {
                errors.push(ValidationError {
                    code: "UNSAFE_DROP_TABLE".to_string(),
                    message: "DROP TABLE without IF EXISTS clause".to_string(),
                    severity: ErrorSeverity::Medium,
                    line: Some(line_no + 1),
                    column: None,
                });
            }
            
            if trimmed.contains("ALTER TABLE") && trimmed.contains("DROP COLUMN") {
                errors.push(ValidationError {
                    code: "DATA_LOSS_WARNING".to_string(),
                    message: "DROP COLUMN may cause data loss".to_string(),
                    severity: ErrorSeverity::Medium,
                    line: Some(line_no + 1),
                    column: None,
                });
            }
        }
    }
}

#[derive(Debug)]
pub struct DependencyValidationRule;

impl ValidationRule for DependencyValidationRule {
    fn validate(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<ValidationResult> {
        let mut errors = Vec::new();
        
        for dep_version in &migration.metadata.dependencies {
            if *dep_version >= migration.metadata.version {
                errors.push(ValidationError {
                    code: "INVALID_DEPENDENCY".to_string(),
                    message: format!(
                        "Migration {} cannot depend on version {} (same or later version)",
                        migration.metadata.version, dep_version
                    ),
                    severity: ErrorSeverity::Critical,
                    line: None,
                    column: None,
                });
            }
        }
        
        if errors.is_empty() {
            Ok(ValidationResult::valid())
        } else {
            Ok(ValidationResult::invalid(errors))
        }
    }
}

#[derive(Debug)]
pub struct ChecksumValidationRule;

impl ValidationRule for ChecksumValidationRule {
    fn validate(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<ValidationResult> {
        let calculated_checksum = super::calculate_checksum(&migration.up_script);
        
        if calculated_checksum != migration.metadata.checksum {
            let error = ValidationError {
                code: "CHECKSUM_MISMATCH".to_string(),
                message: "Migration checksum does not match content".to_string(),
                severity: ErrorSeverity::Critical,
                line: None,
                column: None,
            };
            Ok(ValidationResult::invalid(vec![error]))
        } else {
            Ok(ValidationResult::valid())
        }
    }
}

#[derive(Debug)]
pub struct VersionValidationRule;

impl ValidationRule for VersionValidationRule {
    fn validate(&self, migration: &Migration, ctx: &MigrationContext) -> DatabaseResult<ValidationResult> {
        let mut errors = Vec::new();
        
        if migration.metadata.version <= ctx.current_version && !ctx.force {
            errors.push(ValidationError {
                code: "VERSION_TOO_OLD".to_string(),
                message: format!(
                    "Migration version {} is not greater than current version {}",
                    migration.metadata.version, ctx.current_version
                ),
                severity: ErrorSeverity::High,
                line: None,
                column: None,
            });
        }
        
        if errors.is_empty() {
            Ok(ValidationResult::valid())
        } else {
            Ok(ValidationResult::invalid(errors))
        }
    }
}

#[derive(Debug)]
pub struct ReversibilityValidationRule;

impl ValidationRule for ReversibilityValidationRule {
    fn validate(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<ValidationResult> {
        let mut warnings = Vec::new();
        
        if !migration.metadata.reversible {
            warnings.push(ValidationWarning {
                code: "NON_REVERSIBLE_MIGRATION".to_string(),
                message: "Migration is not reversible".to_string(),
                recommendation: Some("Consider making migration reversible for safer rollbacks".to_string()),
            });
        }
        
        Ok(ValidationResult::valid().with_warnings(warnings))
    }
}

#[derive(Debug)]
pub struct DataLossSafetyCheck;

impl SafetyCheck for DataLossSafetyCheck {
    fn check(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<SafetyCheckResult> {
        let script = &migration.up_script.to_uppercase();
        let mut level = 0u8;
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();
        
        if script.contains("DROP TABLE") || script.contains("DROP COLUMN") {
            level = 3;
            warnings.push(ValidationWarning {
                code: "POTENTIAL_DATA_LOSS".to_string(),
                message: "Migration contains operations that may cause data loss".to_string(),
                recommendation: Some("Ensure data is backed up before running".to_string()),
            });
            recommendations.push("Create backup before migration".to_string());
        }
        
        if script.contains("TRUNCATE") || script.contains("DELETE FROM") {
            level = level.max(2);
            warnings.push(ValidationWarning {
                code: "DATA_DELETION".to_string(),
                message: "Migration contains data deletion operations".to_string(),
                recommendation: None,
            });
        }
        
        Ok(SafetyCheckResult {
            level,
            warnings,
            recommendations,
        })
    }
}

#[derive(Debug)]
pub struct PerformanceSafetyCheck;

impl SafetyCheck for PerformanceSafetyCheck {
    fn check(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<SafetyCheckResult> {
        let script = &migration.up_script.to_uppercase();
        let mut level = 0u8;
        let mut warnings = Vec::new();
        let recommendations = Vec::new();
        
        if script.contains("CREATE INDEX") && !script.contains("CONCURRENTLY") {
            level = 2;
            warnings.push(ValidationWarning {
                code: "BLOCKING_INDEX_CREATION".to_string(),
                message: "Index creation may block table access".to_string(),
                recommendation: Some("Consider using CREATE INDEX CONCURRENTLY".to_string()),
            });
        }
        
        if script.contains("ALTER TABLE") && script.contains("ADD COLUMN") {
            level = level.max(1);
            warnings.push(ValidationWarning {
                code: "TABLE_ALTERATION".to_string(),
                message: "Table alteration may impact performance".to_string(),
                recommendation: Some("Consider running during maintenance window".to_string()),
            });
        }
        
        Ok(SafetyCheckResult {
            level,
            warnings,
            recommendations,
        })
    }
}

#[derive(Debug)]
pub struct CompatibilitySafetyCheck;

impl SafetyCheck for CompatibilitySafetyCheck {
    fn check(&self, _migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<SafetyCheckResult> {
        Ok(SafetyCheckResult {
            level: 0,
            warnings: Vec::new(),
            recommendations: Vec::new(),
        })
    }
}

#[derive(Debug)]
pub struct ResourceUsageSafetyCheck;

impl SafetyCheck for ResourceUsageSafetyCheck {
    fn check(&self, migration: &Migration, _ctx: &MigrationContext) -> DatabaseResult<SafetyCheckResult> {
        let script_size = migration.up_script.len();
        let mut level = 0u8;
        let mut warnings = Vec::new();
        let mut recommendations = Vec::new();
        
        if script_size > 1_000_000 {
            level = 2;
            warnings.push(ValidationWarning {
                code: "LARGE_MIGRATION_SCRIPT".to_string(),
                message: "Migration script is very large".to_string(),
                recommendation: Some("Consider breaking into smaller migrations".to_string()),
            });
            recommendations.push("Monitor memory usage during migration".to_string());
        }
        
        Ok(SafetyCheckResult {
            level,
            warnings,
            recommendations,
        })
    }
}
