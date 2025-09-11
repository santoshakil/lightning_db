use lightning_db::features::backup::{BackupConfig, BackupManager};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: lightning-backup <db_path> <backup_dir>");
        std::process::exit(2);
    }

    let db_path = PathBuf::from(&args[1]);
    let backup_dir = PathBuf::from(&args[2]);

    let cfg = BackupConfig::default();
    let mgr = BackupManager::new(cfg);
    let meta = mgr.create_backup(db_path, backup_dir)?;

    println!(
        "Backup complete: files={}, size={} bytes, checksum={}",
        meta.file_count,
        meta.total_size,
        meta.checksum.unwrap_or_else(|| "none".to_string())
    );

    Ok(())
}
