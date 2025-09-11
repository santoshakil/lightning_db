use lightning_db::{Database, LightningDbConfig};
use std::sync::Arc;
use tempfile::{tempdir, TempDir};

pub fn setup_temp_db() -> (TempDir, Database) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), LightningDbConfig::default()).unwrap();
    (dir, db)
}

pub fn setup_temp_db_with_config(config: LightningDbConfig) -> (TempDir, Database) {
    let dir = tempdir().unwrap();
    let db = Database::create(dir.path(), config).unwrap();
    (dir, db)
}

pub fn setup_concurrent_db() -> (TempDir, Arc<Database>) {
    let dir = tempdir().unwrap();
    let db = Arc::new(Database::create(dir.path(), LightningDbConfig::default()).unwrap());
    (dir, db)
}