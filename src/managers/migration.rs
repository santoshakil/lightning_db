use crate::features::migration::MigrationVersion;
use crate::{Database, Result};
use std::collections::HashMap;
use std::sync::Arc;

pub struct MigrationManager {
    db: Arc<Database>,
}

impl MigrationManager {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn get_schema_version(&self) -> Result<MigrationVersion> {
        self.db.get_schema_version()
    }

    pub fn migrate_to_version(&self, target_version: u64) -> Result<Vec<u64>> {
        self.db.migrate_to_version(target_version)
    }

    pub fn migrate_up(&self) -> Result<Vec<u64>> {
        self.db.migrate_up()
    }

    pub fn migrate_down(&self, steps: usize) -> Result<Vec<u64>> {
        self.db.migrate_down(steps)
    }

    pub fn validate_migration(&self, migration_path: &str) -> Result<bool> {
        self.db.validate_migration(migration_path)
    }

    pub fn get_migration_status(&self) -> Result<HashMap<u64, String>> {
        self.db.get_migration_status()
    }

    pub fn backup_before_migration(&self, backup_path: &str) -> Result<()> {
        self.db.backup_before_migration(backup_path)
    }

    pub fn restore_from_backup(&self, backup_path: &str) -> Result<()> {
        self.db.restore_from_backup(backup_path)
    }

    pub fn load_migrations(&self, migration_dir: &str) -> Result<usize> {
        self.db.load_migrations(migration_dir)
    }
}
