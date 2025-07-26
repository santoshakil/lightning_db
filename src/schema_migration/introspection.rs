//! Schema introspection and diff generation

use crate::{Database, Error, Result};
use super::{Schema, SchemaVersion, TableDefinition, IndexDefinition, ConstraintDefinition};
use std::sync::Arc;
use std::collections::{BTreeMap, HashSet};
use serde::{Serialize, Deserialize};
use tracing::{info, debug};

/// Schema difference representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiff {
    /// Version transition
    pub from_version: SchemaVersion,
    pub to_version: SchemaVersion,
    
    /// Table changes
    pub table_changes: Vec<TableChange>,
    
    /// Index changes
    pub index_changes: Vec<IndexChange>,
    
    /// Constraint changes
    pub constraint_changes: Vec<ConstraintChange>,
    
    /// Summary statistics
    pub summary: DiffSummary,
}

/// Table change types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableChange {
    /// Table added
    Added {
        table: TableDefinition,
    },
    
    /// Table removed
    Removed {
        table: TableDefinition,
    },
    
    /// Table modified
    Modified {
        table_name: String,
        column_changes: Vec<ColumnChange>,
        option_changes: Vec<OptionChange>,
    },
    
    /// Table renamed
    Renamed {
        old_name: String,
        new_name: String,
    },
}

/// Column change types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnChange {
    /// Column added
    Added {
        column: super::ColumnDefinition,
    },
    
    /// Column removed
    Removed {
        column: super::ColumnDefinition,
    },
    
    /// Column modified
    Modified {
        column_name: String,
        changes: Vec<ColumnModification>,
    },
    
    /// Column renamed
    Renamed {
        old_name: String,
        new_name: String,
    },
}

/// Column modification details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnModification {
    TypeChanged {
        old_type: super::DataType,
        new_type: super::DataType,
    },
    NullabilityChanged {
        old_nullable: bool,
        new_nullable: bool,
    },
    DefaultChanged {
        old_default: Option<super::Value>,
        new_default: Option<super::Value>,
    },
    ConstraintAdded {
        constraint: super::ColumnConstraint,
    },
    ConstraintRemoved {
        constraint: super::ColumnConstraint,
    },
}

/// Table option changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OptionChange {
    CompressionChanged {
        old_value: bool,
        new_value: bool,
    },
    EncryptionChanged {
        old_value: bool,
        new_value: bool,
    },
    StorageEngineChanged {
        old_engine: Option<String>,
        new_engine: Option<String>,
    },
    PartitioningChanged {
        old_strategy: Option<super::PartitioningStrategy>,
        new_strategy: Option<super::PartitioningStrategy>,
    },
}

/// Index change types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexChange {
    Added {
        index: IndexDefinition,
    },
    Removed {
        index: IndexDefinition,
    },
    Modified {
        index_name: String,
        changes: Vec<IndexModification>,
    },
}

/// Index modification details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexModification {
    ColumnsChanged,
    TypeChanged {
        old_type: super::IndexType,
        new_type: super::IndexType,
    },
    OptionsChanged,
}

/// Constraint change types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintChange {
    Added {
        constraint: ConstraintDefinition,
    },
    Removed {
        constraint: ConstraintDefinition,
    },
    Modified {
        constraint_name: String,
        changes: Vec<ConstraintModification>,
    },
}

/// Constraint modification details
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConstraintModification {
    TypeChanged,
    ColumnsChanged,
    ExpressionChanged,
}

/// Diff summary statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffSummary {
    /// Number of tables added
    pub tables_added: usize,
    
    /// Number of tables removed
    pub tables_removed: usize,
    
    /// Number of tables modified
    pub tables_modified: usize,
    
    /// Number of columns added
    pub columns_added: usize,
    
    /// Number of columns removed
    pub columns_removed: usize,
    
    /// Number of columns modified
    pub columns_modified: usize,
    
    /// Number of indexes added
    pub indexes_added: usize,
    
    /// Number of indexes removed
    pub indexes_removed: usize,
    
    /// Number of constraints added
    pub constraints_added: usize,
    
    /// Number of constraints removed
    pub constraints_removed: usize,
    
    /// Is this a breaking change?
    pub is_breaking: bool,
    
    /// Estimated migration complexity (1-10)
    pub complexity_score: u8,
}

/// Schema introspector
pub struct SchemaIntrospector {
    database: Arc<Database>,
}

impl SchemaIntrospector {
    /// Create a new schema introspector
    pub fn new(database: Arc<Database>) -> Result<Self> {
        Ok(Self { database })
    }
    
    /// Get the current schema from the database
    pub fn get_current_schema(&self) -> Result<Schema> {
        info!("Introspecting current database schema");
        
        let mut tables = BTreeMap::new();
        let mut indexes = BTreeMap::new();
        let constraints = BTreeMap::new();
        
        // Scan for table definitions
        let table_prefix = b"__table_def_";
        let scan_result = self.database.scan(
            Some(table_prefix.to_vec()),
            Some(next_prefix(table_prefix)),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            
            // Extract table name from key
            let table_name = String::from_utf8(key[table_prefix.len()..].to_vec())
                .map_err(|e| Error::Parse(format!("Invalid table name: {}", e)))?;
                
            // Deserialize table definition
            let table_def: TableDefinition = serde_json::from_slice(&value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
                
            tables.insert(table_name, table_def);
        }
        
        // Scan for index definitions
        let index_prefix = b"__index_def_";
        let scan_result = self.database.scan(
            Some(index_prefix.to_vec()),
            Some(next_prefix(index_prefix)),
        )?;
        
        for item in scan_result {
            let (key, value) = item?;
            
            // Extract index name from key
            let index_name = String::from_utf8(key[index_prefix.len()..].to_vec())
                .map_err(|e| Error::Parse(format!("Invalid index name: {}", e)))?;
                
            // Deserialize index definition
            let index_def: IndexDefinition = serde_json::from_slice(&value)
                .map_err(|e| Error::Serialization(e.to_string()))?;
                
            indexes.insert(index_name, index_def);
        }
        
        // Get current version
        let version_key = b"__schema_version__";
        let version = match self.database.get(version_key)? {
            Some(data) => bincode::decode_from_slice(&data, bincode::config::standard())
                .map_err(|e| Error::Serialization(e.to_string()))?.0,
            None => SchemaVersion::new(0, 0),
        };
        
        Ok(Schema {
            version,
            tables,
            indexes,
            constraints,
            metadata: BTreeMap::new(),
        })
    }
    
    /// Generate a diff between two schemas
    pub fn diff_schemas(&self, from: &Schema, to: &Schema) -> Result<SchemaDiff> {
        debug!("Generating schema diff from {} to {}", from.version, to.version);
        
        let mut table_changes = Vec::new();
        let mut index_changes = Vec::new();
        let mut constraint_changes = Vec::new();
        
        // Diff tables
        self.diff_tables(&from.tables, &to.tables, &mut table_changes)?;
        
        // Diff indexes
        self.diff_indexes(&from.indexes, &to.indexes, &mut index_changes)?;
        
        // Diff constraints
        self.diff_constraints(&from.constraints, &to.constraints, &mut constraint_changes)?;
        
        // Calculate summary
        let summary = self.calculate_summary(&table_changes, &index_changes, &constraint_changes);
        
        Ok(SchemaDiff {
            from_version: from.version.clone(),
            to_version: to.version.clone(),
            table_changes,
            index_changes,
            constraint_changes,
            summary,
        })
    }
    
    /// Diff table definitions
    fn diff_tables(
        &self,
        from_tables: &BTreeMap<String, TableDefinition>,
        to_tables: &BTreeMap<String, TableDefinition>,
        changes: &mut Vec<TableChange>,
    ) -> Result<()> {
        let from_names: HashSet<_> = from_tables.keys().cloned().collect();
        let to_names: HashSet<_> = to_tables.keys().cloned().collect();
        
        // Find added tables
        for name in to_names.difference(&from_names) {
            changes.push(TableChange::Added {
                table: to_tables[name].clone(),
            });
        }
        
        // Find removed tables
        for name in from_names.difference(&to_names) {
            changes.push(TableChange::Removed {
                table: from_tables[name].clone(),
            });
        }
        
        // Find modified tables
        for name in from_names.intersection(&to_names) {
            let from_table = &from_tables[name];
            let to_table = &to_tables[name];
            
            let mut column_changes = Vec::new();
            let mut option_changes = Vec::new();
            
            // Diff columns
            self.diff_columns(&from_table.columns, &to_table.columns, &mut column_changes)?;
            
            // Diff options
            self.diff_table_options(&from_table.options, &to_table.options, &mut option_changes)?;
            
            if !column_changes.is_empty() || !option_changes.is_empty() {
                changes.push(TableChange::Modified {
                    table_name: name.clone(),
                    column_changes,
                    option_changes,
                });
            }
        }
        
        Ok(())
    }
    
    /// Diff column definitions
    fn diff_columns(
        &self,
        from_columns: &[super::ColumnDefinition],
        to_columns: &[super::ColumnDefinition],
        changes: &mut Vec<ColumnChange>,
    ) -> Result<()> {
        let from_map: BTreeMap<_, _> = from_columns.iter()
            .map(|c| (c.name.clone(), c))
            .collect();
        let to_map: BTreeMap<_, _> = to_columns.iter()
            .map(|c| (c.name.clone(), c))
            .collect();
            
        let from_names: HashSet<_> = from_map.keys().cloned().collect();
        let to_names: HashSet<_> = to_map.keys().cloned().collect();
        
        // Find added columns
        for name in to_names.difference(&from_names) {
            changes.push(ColumnChange::Added {
                column: to_map[name].clone(),
            });
        }
        
        // Find removed columns
        for name in from_names.difference(&to_names) {
            changes.push(ColumnChange::Removed {
                column: from_map[name].clone(),
            });
        }
        
        // Find modified columns
        for name in from_names.intersection(&to_names) {
            let from_col = from_map[name];
            let to_col = to_map[name];
            
            let mut modifications = Vec::new();
            
            // Check type change
            if from_col.data_type != to_col.data_type {
                modifications.push(ColumnModification::TypeChanged {
                    old_type: from_col.data_type.clone(),
                    new_type: to_col.data_type.clone(),
                });
            }
            
            // Check nullability change
            if from_col.nullable != to_col.nullable {
                modifications.push(ColumnModification::NullabilityChanged {
                    old_nullable: from_col.nullable,
                    new_nullable: to_col.nullable,
                });
            }
            
            // Check default change
            if from_col.default != to_col.default {
                modifications.push(ColumnModification::DefaultChanged {
                    old_default: from_col.default.clone(),
                    new_default: to_col.default.clone(),
                });
            }
            
            if !modifications.is_empty() {
                changes.push(ColumnChange::Modified {
                    column_name: name.clone(),
                    changes: modifications,
                });
            }
        }
        
        Ok(())
    }
    
    /// Diff table options
    fn diff_table_options(
        &self,
        from_options: &super::TableOptions,
        to_options: &super::TableOptions,
        changes: &mut Vec<OptionChange>,
    ) -> Result<()> {
        if from_options.compression != to_options.compression {
            changes.push(OptionChange::CompressionChanged {
                old_value: from_options.compression,
                new_value: to_options.compression,
            });
        }
        
        if from_options.encryption != to_options.encryption {
            changes.push(OptionChange::EncryptionChanged {
                old_value: from_options.encryption,
                new_value: to_options.encryption,
            });
        }
        
        if from_options.storage_engine != to_options.storage_engine {
            changes.push(OptionChange::StorageEngineChanged {
                old_engine: from_options.storage_engine.clone(),
                new_engine: to_options.storage_engine.clone(),
            });
        }
        
        Ok(())
    }
    
    /// Diff index definitions
    fn diff_indexes(
        &self,
        from_indexes: &BTreeMap<String, IndexDefinition>,
        to_indexes: &BTreeMap<String, IndexDefinition>,
        changes: &mut Vec<IndexChange>,
    ) -> Result<()> {
        let from_names: HashSet<_> = from_indexes.keys().cloned().collect();
        let to_names: HashSet<_> = to_indexes.keys().cloned().collect();
        
        // Find added indexes
        for name in to_names.difference(&from_names) {
            changes.push(IndexChange::Added {
                index: to_indexes[name].clone(),
            });
        }
        
        // Find removed indexes
        for name in from_names.difference(&to_names) {
            changes.push(IndexChange::Removed {
                index: from_indexes[name].clone(),
            });
        }
        
        // Find modified indexes
        for name in from_names.intersection(&to_names) {
            let from_index = &from_indexes[name];
            let to_index = &to_indexes[name];
            
            let mut modifications = Vec::new();
            
            if from_index.columns != to_index.columns {
                modifications.push(IndexModification::ColumnsChanged);
            }
            
            if from_index.index_type != to_index.index_type {
                modifications.push(IndexModification::TypeChanged {
                    old_type: from_index.index_type.clone(),
                    new_type: to_index.index_type.clone(),
                });
            }
            
            if !modifications.is_empty() {
                changes.push(IndexChange::Modified {
                    index_name: name.clone(),
                    changes: modifications,
                });
            }
        }
        
        Ok(())
    }
    
    /// Diff constraint definitions
    fn diff_constraints(
        &self,
        from_constraints: &BTreeMap<String, ConstraintDefinition>,
        to_constraints: &BTreeMap<String, ConstraintDefinition>,
        changes: &mut Vec<ConstraintChange>,
    ) -> Result<()> {
        let from_names: HashSet<_> = from_constraints.keys().cloned().collect();
        let to_names: HashSet<_> = to_constraints.keys().cloned().collect();
        
        // Find added constraints
        for name in to_names.difference(&from_names) {
            changes.push(ConstraintChange::Added {
                constraint: to_constraints[name].clone(),
            });
        }
        
        // Find removed constraints
        for name in from_names.difference(&to_names) {
            changes.push(ConstraintChange::Removed {
                constraint: from_constraints[name].clone(),
            });
        }
        
        Ok(())
    }
    
    /// Calculate diff summary
    fn calculate_summary(
        &self,
        table_changes: &[TableChange],
        index_changes: &[IndexChange],
        constraint_changes: &[ConstraintChange],
    ) -> DiffSummary {
        let mut summary = DiffSummary {
            tables_added: 0,
            tables_removed: 0,
            tables_modified: 0,
            columns_added: 0,
            columns_removed: 0,
            columns_modified: 0,
            indexes_added: 0,
            indexes_removed: 0,
            constraints_added: 0,
            constraints_removed: 0,
            is_breaking: false,
            complexity_score: 1,
        };
        
        // Count table changes
        for change in table_changes {
            match change {
                TableChange::Added { .. } => summary.tables_added += 1,
                TableChange::Removed { .. } => {
                    summary.tables_removed += 1;
                    summary.is_breaking = true;
                }
                TableChange::Modified { column_changes, .. } => {
                    summary.tables_modified += 1;
                    
                    // Count column changes
                    for col_change in column_changes {
                        match col_change {
                            ColumnChange::Added { .. } => summary.columns_added += 1,
                            ColumnChange::Removed { .. } => {
                                summary.columns_removed += 1;
                                summary.is_breaking = true;
                            }
                            ColumnChange::Modified { .. } => summary.columns_modified += 1,
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        
        // Count index changes
        for change in index_changes {
            match change {
                IndexChange::Added { .. } => summary.indexes_added += 1,
                IndexChange::Removed { .. } => summary.indexes_removed += 1,
                _ => {}
            }
        }
        
        // Count constraint changes
        for change in constraint_changes {
            match change {
                ConstraintChange::Added { .. } => summary.constraints_added += 1,
                ConstraintChange::Removed { .. } => summary.constraints_removed += 1,
                _ => {}
            }
        }
        
        // Calculate complexity score
        let total_changes = summary.tables_added + summary.tables_removed + summary.tables_modified
            + summary.columns_added + summary.columns_removed + summary.columns_modified
            + summary.indexes_added + summary.indexes_removed
            + summary.constraints_added + summary.constraints_removed;
            
        summary.complexity_score = match total_changes {
            0 => 1,
            1..=3 => 2,
            4..=10 => 4,
            11..=20 => 6,
            21..=50 => 8,
            _ => 10,
        };
        
        summary
    }
}

/// Get the next prefix for range scans
fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut next = prefix.to_vec();
    for i in (0..next.len()).rev() {
        if next[i] < 255 {
            next[i] += 1;
            return next;
        }
        next[i] = 0;
    }
    next.push(0);
    next
}

#[cfg(test)]
mod tests {
    use super::*;
    
    
    #[test]
    fn test_diff_summary() {
        let summary = DiffSummary {
            tables_added: 2,
            tables_removed: 1,
            tables_modified: 3,
            columns_added: 5,
            columns_removed: 2,
            columns_modified: 4,
            indexes_added: 3,
            indexes_removed: 1,
            constraints_added: 2,
            constraints_removed: 0,
            is_breaking: true,
            complexity_score: 6,
        };
        
        assert!(summary.is_breaking);
        assert_eq!(summary.complexity_score, 6);
        assert_eq!(summary.tables_added + summary.tables_removed + summary.tables_modified, 6);
    }
}