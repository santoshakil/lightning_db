use crate::core::index::IndexableRecord;
use crate::{Database, IndexConfig, IndexKey, IndexQuery, Result};
use std::sync::Arc;

pub struct IndexManager {
    db: Arc<Database>,
}

impl IndexManager {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn create(&self, name: &str, columns: Vec<String>) -> Result<()> {
        self.db.create_index(name, columns)
    }

    pub fn create_with_config(&self, config: IndexConfig) -> Result<()> {
        self.db.create_index_with_config(config)
    }

    pub fn drop(&self, index_name: &str) -> Result<()> {
        self.db.drop_index(index_name)
    }

    pub fn list(&self) -> Vec<String> {
        self.db.list_indexes()
    }

    pub fn query(&self, index_name: &str, key: &[u8]) -> Result<Vec<Vec<u8>>> {
        self.db.query_index(index_name, key)
    }

    pub fn query_advanced(&self, query: IndexQuery) -> Result<Vec<Vec<u8>>> {
        self.db.query_index_advanced(query)
    }

    pub fn get_by_index(&self, index_name: &str, index_key: &IndexKey) -> Result<Option<Vec<u8>>> {
        self.db.get_by_index(index_name, index_key)
    }

    pub fn put_indexed(
        &self,
        key: &[u8],
        value: &[u8],
        record: &dyn IndexableRecord,
    ) -> Result<()> {
        self.db.put_indexed(key, value, record)
    }

    pub fn delete_indexed(&self, key: &[u8], record: &dyn IndexableRecord) -> Result<()> {
        self.db.delete_indexed(key, record)
    }

    pub fn update_indexed(
        &self,
        key: &[u8],
        old_record: &dyn IndexableRecord,
        new_record: &dyn IndexableRecord,
        new_value: &[u8],
    ) -> Result<()> {
        self.db
            .update_indexed(key, new_value, old_record, new_record)
    }

    pub fn analyze(&self) -> Result<()> {
        self.db.analyze_indexes()
    }

    pub fn range(
        &self,
        index_name: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<Vec<u8>>> {
        self.db.range_index(index_name, start_key, end_key)
    }
}
