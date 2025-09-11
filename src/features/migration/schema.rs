use super::{MigrationType, MigrationVersion};
use crate::core::error::{DatabaseError, DatabaseResult};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::Path, sync::Arc, time::SystemTime};

const SCHEMA_VERSION_PAGE_ID: u32 = 0;
const SCHEMA_METADATA_PAGE_ID: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub version: MigrationVersion,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub schema_hash: String,
    pub migration_count: u64,
    pub description: String,
}

impl Default for SchemaInfo {
    fn default() -> Self {
        Self {
            version: MigrationVersion::INITIAL,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
            schema_hash: String::new(),
            migration_count: 0,
            description: "Initial schema".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaElement {
    pub name: String,
    pub element_type: SchemaElementType,
    pub properties: HashMap<String, serde_json::Value>,
    pub created_in_version: MigrationVersion,
    pub modified_in_version: Option<MigrationVersion>,
    pub deprecated_in_version: Option<MigrationVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaElementType {
    Table,
    Index,
    View,
    Function,
    Trigger,
    Constraint,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDefinition {
    pub version: MigrationVersion,
    pub elements: HashMap<String, SchemaElement>,
    pub metadata: HashMap<String, serde_json::Value>,
}

impl SchemaDefinition {
    pub fn new(version: MigrationVersion) -> Self {
        Self {
            version,
            elements: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn add_element(&mut self, element: SchemaElement) {
        self.elements.insert(element.name.clone(), element);
    }

    pub fn remove_element(&mut self, name: &str) -> Option<SchemaElement> {
        self.elements.remove(name)
    }

    pub fn get_element(&self, name: &str) -> Option<&SchemaElement> {
        self.elements.get(name)
    }

    pub fn deprecate_element(
        &mut self,
        name: &str,
        version: MigrationVersion,
    ) -> DatabaseResult<()> {
        if let Some(element) = self.elements.get_mut(name) {
            element.deprecated_in_version = Some(version);
            Ok(())
        } else {
            Err(DatabaseError::MigrationError(format!(
                "Schema element '{}' not found",
                name
            )))
        }
    }

    pub fn calculate_hash(&self) -> String {
        let serialized = serde_json::to_string(self).unwrap_or_default();
        super::calculate_checksum(&serialized)
    }
}

#[derive(Debug)]
pub struct SchemaManager {
    schema_info: Arc<RwLock<Option<SchemaInfo>>>,
    current_schema: Arc<RwLock<Option<SchemaDefinition>>>,
    schema_history: Arc<RwLock<HashMap<MigrationVersion, SchemaDefinition>>>,
}

impl Default for SchemaManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaManager {
    pub fn new() -> Self {
        Self {
            schema_info: Arc::new(RwLock::new(None)),
            current_schema: Arc::new(RwLock::new(None)),
            schema_history: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn initialize_schema(&self, db_path: &str) -> DatabaseResult<()> {
        let schema_info = SchemaInfo::default();
        let schema_def = SchemaDefinition::new(MigrationVersion::INITIAL);

        self.save_schema_info(db_path, &schema_info)?;
        self.save_schema_definition(db_path, &schema_def)?;

        {
            let mut info_guard = self.schema_info.write();
            *info_guard = Some(schema_info);
        }

        {
            let mut schema_guard = self.current_schema.write();
            *schema_guard = Some(schema_def.clone());
        }

        {
            let mut history_guard = self.schema_history.write();
            history_guard.insert(MigrationVersion::INITIAL, schema_def);
        }

        Ok(())
    }

    pub fn load_schema_info(&self, db_path: &str) -> DatabaseResult<SchemaInfo> {
        let schema_file = format!("{}/schema_info.json", db_path);

        if Path::new(&schema_file).exists() {
            let content = std::fs::read_to_string(&schema_file)
                .map_err(|e| DatabaseError::IoError(e.to_string()))?;

            let schema_info: SchemaInfo = serde_json::from_str(&content)
                .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

            {
                let mut guard = self.schema_info.write();
                *guard = Some(schema_info.clone());
            }

            Ok(schema_info)
        } else {
            self.initialize_schema(db_path)?;
            Ok(SchemaInfo::default())
        }
    }

    pub fn save_schema_info(&self, db_path: &str, info: &SchemaInfo) -> DatabaseResult<()> {
        let schema_file = format!("{}/schema_info.json", db_path);

        std::fs::create_dir_all(db_path).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let content = serde_json::to_string_pretty(info)
            .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

        std::fs::write(&schema_file, content).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        Ok(())
    }

    pub fn load_schema_definition(
        &self,
        db_path: &str,
        version: MigrationVersion,
    ) -> DatabaseResult<SchemaDefinition> {
        let schema_file = format!("{}/schema_{}.json", db_path, version);

        if Path::new(&schema_file).exists() {
            let content = std::fs::read_to_string(&schema_file)
                .map_err(|e| DatabaseError::IoError(e.to_string()))?;

            let schema_def: SchemaDefinition = serde_json::from_str(&content)
                .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

            Ok(schema_def)
        } else {
            Ok(SchemaDefinition::new(version))
        }
    }

    pub fn save_schema_definition(
        &self,
        db_path: &str,
        schema: &SchemaDefinition,
    ) -> DatabaseResult<()> {
        let schema_file = format!("{}/schema_{}.json", db_path, schema.version);

        std::fs::create_dir_all(db_path).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let content = serde_json::to_string_pretty(schema)
            .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

        std::fs::write(&schema_file, content).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        {
            let mut history_guard = self.schema_history.write();
            history_guard.insert(schema.version, schema.clone());
        }

        Ok(())
    }

    pub fn get_current_version(&self, db_path: &str) -> DatabaseResult<MigrationVersion> {
        let info = self.load_schema_info(db_path)?;
        Ok(info.version)
    }

    pub fn update_schema_version(
        &self,
        db_path: &str,
        new_version: MigrationVersion,
    ) -> DatabaseResult<()> {
        let mut info = self.load_schema_info(db_path)?;
        info.version = new_version;
        info.updated_at = SystemTime::now();
        info.migration_count += 1;

        self.save_schema_info(db_path, &info)?;

        {
            let mut guard = self.schema_info.write();
            *guard = Some(info);
        }

        Ok(())
    }

    pub fn get_current_schema(&self, db_path: &str) -> DatabaseResult<SchemaDefinition> {
        let info = self.load_schema_info(db_path)?;
        self.load_schema_definition(db_path, info.version)
    }

    pub fn update_schema(&self, db_path: &str, schema: SchemaDefinition) -> DatabaseResult<()> {
        let mut info = self.load_schema_info(db_path)?;

        let schema_hash = schema.calculate_hash();
        if info.schema_hash != schema_hash {
            info.schema_hash = schema_hash;
            info.updated_at = SystemTime::now();
            info.version = schema.version;

            self.save_schema_info(db_path, &info)?;
            self.save_schema_definition(db_path, &schema)?;

            {
                let mut schema_guard = self.current_schema.write();
                *schema_guard = Some(schema);
            }
        }

        Ok(())
    }

    pub fn get_schema_diff(
        &self,
        db_path: &str,
        from_version: MigrationVersion,
        to_version: MigrationVersion,
    ) -> DatabaseResult<SchemaDiff> {
        let from_schema = self.load_schema_definition(db_path, from_version)?;
        let to_schema = self.load_schema_definition(db_path, to_version)?;

        Ok(SchemaDiff::calculate(&from_schema, &to_schema))
    }

    pub fn validate_schema_compatibility(
        &self,
        db_path: &str,
        target_version: MigrationVersion,
    ) -> DatabaseResult<bool> {
        let current_version = self.get_current_version(db_path)?;
        let current_schema = self.load_schema_definition(db_path, current_version)?;
        let target_schema = self.load_schema_definition(db_path, target_version)?;

        Ok(SchemaCompatibilityChecker::check_compatibility(
            &current_schema,
            &target_schema,
        ))
    }

    pub fn backup_schema(&self, db_path: &str, backup_path: &str) -> DatabaseResult<()> {
        let info = self.load_schema_info(db_path)?;
        let schema = self.load_schema_definition(db_path, info.version)?;

        std::fs::create_dir_all(backup_path).map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let backup_info_file = format!("{}/schema_info_backup.json", backup_path);
        let backup_schema_file = format!("{}/schema_{}_backup.json", backup_path, info.version);

        let info_content = serde_json::to_string_pretty(&info)
            .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;
        let schema_content = serde_json::to_string_pretty(&schema)
            .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

        std::fs::write(&backup_info_file, info_content)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;
        std::fs::write(&backup_schema_file, schema_content)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;

        Ok(())
    }

    pub fn restore_schema(&self, db_path: &str, backup_path: &str) -> DatabaseResult<()> {
        let backup_info_file = format!("{}/schema_info_backup.json", backup_path);
        let info_content = std::fs::read_to_string(&backup_info_file)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let info: SchemaInfo = serde_json::from_str(&info_content)
            .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

        let backup_schema_file = format!("{}/schema_{}_backup.json", backup_path, info.version);
        let schema_content = std::fs::read_to_string(&backup_schema_file)
            .map_err(|e| DatabaseError::IoError(e.to_string()))?;

        let schema: SchemaDefinition = serde_json::from_str(&schema_content)
            .map_err(|e| DatabaseError::SerializationError(e.to_string()))?;

        self.save_schema_info(db_path, &info)?;
        self.save_schema_definition(db_path, &schema)?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SchemaDiff {
    pub added_elements: Vec<SchemaElement>,
    pub removed_elements: Vec<SchemaElement>,
    pub modified_elements: Vec<(SchemaElement, SchemaElement)>,
}

impl SchemaDiff {
    pub fn calculate(from: &SchemaDefinition, to: &SchemaDefinition) -> Self {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        let mut modified = Vec::new();

        for (name, to_element) in &to.elements {
            if let Some(from_element) = from.elements.get(name) {
                if from_element != to_element {
                    modified.push((from_element.clone(), to_element.clone()));
                }
            } else {
                added.push(to_element.clone());
            }
        }

        for (name, from_element) in &from.elements {
            if !to.elements.contains_key(name) {
                removed.push(from_element.clone());
            }
        }

        Self {
            added_elements: added,
            removed_elements: removed,
            modified_elements: modified,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.added_elements.is_empty()
            && self.removed_elements.is_empty()
            && self.modified_elements.is_empty()
    }

    pub fn has_breaking_changes(&self) -> bool {
        !self.removed_elements.is_empty() || self.modified_elements.iter().any(|(_, _)| true)
    }
}

pub struct SchemaCompatibilityChecker;

impl SchemaCompatibilityChecker {
    pub fn check_compatibility(from: &SchemaDefinition, to: &SchemaDefinition) -> bool {
        let diff = SchemaDiff::calculate(from, to);

        !diff.has_breaking_changes()
    }

    pub fn check_migration_compatibility(
        from: &SchemaDefinition,
        to: &SchemaDefinition,
        migration_type: MigrationType,
    ) -> bool {
        match migration_type {
            MigrationType::Schema => Self::check_schema_migration_compatibility(from, to),
            MigrationType::Data => Self::check_data_migration_compatibility(from, to),
            MigrationType::Index => Self::check_index_migration_compatibility(from, to),
            MigrationType::Maintenance => true,
        }
    }

    fn check_schema_migration_compatibility(
        _from: &SchemaDefinition,
        _to: &SchemaDefinition,
    ) -> bool {
        true
    }

    fn check_data_migration_compatibility(from: &SchemaDefinition, to: &SchemaDefinition) -> bool {
        let diff = SchemaDiff::calculate(from, to);
        diff.removed_elements.is_empty()
    }

    fn check_index_migration_compatibility(
        _from: &SchemaDefinition,
        _to: &SchemaDefinition,
    ) -> bool {
        true
    }
}
