use crate::{Error, Result};
use super::detector::DetectionResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantineEntry {
    pub id: u64,
    pub page_id: u64,
    pub corruption: DetectionResult,
    pub quarantine_time: SystemTime,
    pub status: QuarantineStatus,
    pub recovery_attempts: u32,
    pub data_backup: Vec<u8>,
    pub recovery_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum QuarantineStatus {
    Quarantined,
    RecoveryInProgress,
    RecoveryFailed,
    Recovered,
    PermanentlyLost,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantineStats {
    pub total_entries: usize,
    pub by_status: HashMap<QuarantineStatus, usize>,
    pub by_corruption_type: HashMap<String, usize>,
    pub oldest_entry: Option<SystemTime>,
    pub newest_entry: Option<SystemTime>,
    pub total_data_size: usize,
}

pub struct QuarantineManager {
    entries: Arc<RwLock<HashMap<u64, QuarantineEntry>>>,
    next_id: Arc<RwLock<u64>>,
    max_entries: usize,
    max_data_size: usize,
}

impl QuarantineManager {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1)),
            max_entries,
            max_data_size: 100 * 1024 * 1024, // 100MB default limit
        }
    }

    pub async fn quarantine_data(&self, corruption: &DetectionResult) -> Result<u64> {
        let entry_id = self.get_next_id().await;
        
        // Check if we need to make space
        self.cleanup_if_needed().await?;

        let entry = QuarantineEntry {
            id: entry_id,
            page_id: corruption.page_id,
            corruption: corruption.clone(),
            quarantine_time: SystemTime::now(),
            status: QuarantineStatus::Quarantined,
            recovery_attempts: 0,
            data_backup: corruption.affected_data.clone(),
            recovery_hint: corruption.recovery_hint.clone(),
        };

        self.entries.write().await.insert(entry_id, entry);
        
        eprintln!("Quarantined corrupted data: page {} (entry {})", corruption.page_id, entry_id);
        
        Ok(entry_id)
    }

    pub async fn get_entry(&self, entry_id: u64) -> Result<QuarantineEntry> {
        let entries = self.entries.read().await;
        entries.get(&entry_id)
            .cloned()
            .ok_or_else(|| Error::InvalidOperation {
                reason: format!("Quarantine entry {} not found", entry_id),
            })
    }

    pub async fn list_entries(&self) -> Result<Vec<QuarantineEntry>> {
        let entries = self.entries.read().await;
        Ok(entries.values().cloned().collect())
    }

    pub async fn list_entries_by_status(&self, status: QuarantineStatus) -> Result<Vec<QuarantineEntry>> {
        let entries = self.entries.read().await;
        Ok(entries.values()
            .filter(|entry| entry.status == status)
            .cloned()
            .collect())
    }

    pub async fn list_entries_by_page(&self, page_id: u64) -> Result<Vec<QuarantineEntry>> {
        let entries = self.entries.read().await;
        Ok(entries.values()
            .filter(|entry| entry.page_id == page_id)
            .cloned()
            .collect())
    }

    pub async fn update_entry_status(&self, entry_id: u64, status: QuarantineStatus) -> Result<()> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&entry_id) {
            entry.status = status;
            Ok(())
        } else {
            Err(Error::InvalidOperation {
                reason: format!("Quarantine entry {} not found", entry_id),
            })
        }
    }

    pub async fn increment_recovery_attempts(&self, entry_id: u64) -> Result<u32> {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&entry_id) {
            entry.recovery_attempts += 1;
            Ok(entry.recovery_attempts)
        } else {
            Err(Error::InvalidOperation {
                reason: format!("Quarantine entry {} not found", entry_id),
            })
        }
    }

    pub async fn remove_entry(&self, entry_id: u64) -> Result<QuarantineEntry> {
        let mut entries = self.entries.write().await;
        entries.remove(&entry_id)
            .ok_or_else(|| Error::InvalidOperation {
                reason: format!("Quarantine entry {} not found", entry_id),
            })
    }

    pub async fn get_stats(&self) -> Result<QuarantineStats> {
        let entries = self.entries.read().await;
        
        let mut by_status = HashMap::new();
        let mut by_corruption_type = HashMap::new();
        let mut oldest_entry = None;
        let mut newest_entry = None;
        let mut total_data_size = 0;

        for entry in entries.values() {
            // Count by status
            *by_status.entry(entry.status).or_insert(0) += 1;
            
            // Count by corruption type
            let corruption_type = format!("{:?}", entry.corruption.corruption_type);
            *by_corruption_type.entry(corruption_type).or_insert(0) += 1;
            
            // Track oldest and newest
            if oldest_entry.is_none() || entry.quarantine_time < oldest_entry.unwrap() {
                oldest_entry = Some(entry.quarantine_time);
            }
            if newest_entry.is_none() || entry.quarantine_time > newest_entry.unwrap() {
                newest_entry = Some(entry.quarantine_time);
            }
            
            // Sum data size
            total_data_size += entry.data_backup.len();
        }

        Ok(QuarantineStats {
            total_entries: entries.len(),
            by_status,
            by_corruption_type,
            oldest_entry,
            newest_entry,
            total_data_size,
        })
    }

    pub async fn cleanup_old_entries(&self, max_age: std::time::Duration) -> Result<usize> {
        let cutoff_time = SystemTime::now()
            .checked_sub(max_age)
            .unwrap_or(SystemTime::UNIX_EPOCH);

        let mut entries = self.entries.write().await;
        let initial_count = entries.len();

        // Remove old entries that are recovered or permanently lost
        entries.retain(|_, entry| {
            entry.quarantine_time > cutoff_time ||
            (entry.status != QuarantineStatus::Recovered &&
             entry.status != QuarantineStatus::PermanentlyLost)
        });

        let removed_count = initial_count - entries.len();
        
        if removed_count > 0 {
            eprintln!("Cleaned up {} old quarantine entries", removed_count);
        }

        Ok(removed_count)
    }

    pub async fn cleanup_if_needed(&self) -> Result<()> {
        let entries_count = self.entries.read().await.len();
        
        if entries_count >= self.max_entries {
            // Remove oldest recovered or permanently lost entries
            let mut entries = self.entries.write().await;
            let mut removable_entries: Vec<_> = entries.iter()
                .filter(|(_, entry)| {
                    entry.status == QuarantineStatus::Recovered ||
                    entry.status == QuarantineStatus::PermanentlyLost
                })
                .map(|(&id, entry)| (id, entry.quarantine_time))
                .collect();
            
            // Sort by quarantine time (oldest first)
            removable_entries.sort_by_key(|(_, time)| *time);
            
            // Remove oldest entries until we're under the limit
            let to_remove = (entries_count - self.max_entries) + (self.max_entries / 10); // Remove 10% extra
            
            for (id, _) in removable_entries.into_iter().take(to_remove) {
                entries.remove(&id);
            }
        }

        // Also check data size limit
        self.cleanup_by_data_size().await?;

        Ok(())
    }

    async fn cleanup_by_data_size(&self) -> Result<()> {
        let total_data_size = {
            let entries = self.entries.read().await;
            entries.values()
                .map(|entry| entry.data_backup.len())
                .sum::<usize>()
        };

        if total_data_size > self.max_data_size {
            let mut entries = self.entries.write().await;
            let mut entries_by_size: Vec<_> = entries.iter()
                .map(|(&id, entry)| (id, entry.data_backup.len(), entry.status))
                .collect();
            
            // Sort by data size (largest first) and prioritize non-critical entries
            entries_by_size.sort_by(|(_, size_a, status_a), (_, size_b, status_b)| {
                // First by status priority (recovered/lost entries first)
                let priority_a = match status_a {
                    QuarantineStatus::Recovered | QuarantineStatus::PermanentlyLost => 0,
                    QuarantineStatus::RecoveryFailed => 1,
                    _ => 2,
                };
                let priority_b = match status_b {
                    QuarantineStatus::Recovered | QuarantineStatus::PermanentlyLost => 0,
                    QuarantineStatus::RecoveryFailed => 1,
                    _ => 2,
                };
                
                priority_a.cmp(&priority_b).then(size_b.cmp(size_a))
            });

            let mut current_size = total_data_size;
            for (id, size, _) in entries_by_size {
                if current_size <= self.max_data_size {
                    break;
                }
                entries.remove(&id);
                current_size -= size;
            }
        }

        Ok(())
    }

    pub async fn export_entries(&self) -> Result<Vec<QuarantineEntry>> {
        let entries = self.entries.read().await;
        Ok(entries.values().cloned().collect())
    }

    pub async fn import_entries(&self, entries: Vec<QuarantineEntry>) -> Result<()> {
        let mut current_entries = self.entries.write().await;
        
        // Update next_id to avoid conflicts
        let max_id = entries.iter().map(|e| e.id).max().unwrap_or(0);
        {
            let mut next_id = self.next_id.write().await;
            *next_id = (*next_id).max(max_id + 1);
        }

        // Import entries
        for entry in entries {
            current_entries.insert(entry.id, entry);
        }

        Ok(())
    }

    pub async fn clear_all_entries(&self) -> Result<usize> {
        let mut entries = self.entries.write().await;
        let count = entries.len();
        entries.clear();
        Ok(count)
    }

    pub async fn get_entry_count(&self) -> usize {
        self.entries.read().await.len()
    }

    pub async fn has_quarantined_page(&self, page_id: u64) -> bool {
        let entries = self.entries.read().await;
        entries.values().any(|entry| {
            entry.page_id == page_id && 
            entry.status == QuarantineStatus::Quarantined
        })
    }

    pub async fn get_quarantined_pages(&self) -> Vec<u64> {
        let entries = self.entries.read().await;
        entries.values()
            .filter(|entry| entry.status == QuarantineStatus::Quarantined)
            .map(|entry| entry.page_id)
            .collect()
    }

    async fn get_next_id(&self) -> u64 {
        let mut next_id = self.next_id.write().await;
        let id = *next_id;
        *next_id += 1;
        id
    }

    pub async fn mark_entry_for_recovery(&self, entry_id: u64) -> Result<()> {
        self.update_entry_status(entry_id, QuarantineStatus::RecoveryInProgress).await
    }

    pub async fn mark_recovery_failed(&self, entry_id: u64) -> Result<()> {
        self.increment_recovery_attempts(entry_id).await?;
        
        let recovery_attempts = {
            let entries = self.entries.read().await;
            entries.get(&entry_id)
                .map(|entry| entry.recovery_attempts)
                .unwrap_or(0)
        };

        if recovery_attempts >= 5 {
            // After 5 failed attempts, mark as permanently lost
            self.update_entry_status(entry_id, QuarantineStatus::PermanentlyLost).await
        } else {
            self.update_entry_status(entry_id, QuarantineStatus::RecoveryFailed).await
        }
    }

    pub async fn mark_recovery_successful(&self, entry_id: u64) -> Result<()> {
        self.update_entry_status(entry_id, QuarantineStatus::Recovered).await
    }

    pub async fn get_recoverable_entries(&self) -> Result<Vec<QuarantineEntry>> {
        self.list_entries_by_status(QuarantineStatus::Quarantined).await
    }

    pub async fn get_failed_recovery_entries(&self) -> Result<Vec<QuarantineEntry>> {
        self.list_entries_by_status(QuarantineStatus::RecoveryFailed).await
    }
}