use crate::{
    Database, Result,
    core::index::{IndexConfig, IndexKey, IndexQuery, IndexableRecord},
};

impl Database {
    pub fn create_index_with_config(&self, config: IndexConfig) -> Result<()> {
        self.index_manager.create_index(config)
    }

    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        self.index_manager.drop_index(index_name)
    }

    pub fn list_indexes(&self) -> Vec<String> {
        self.index_manager.list_indexes()
    }

    pub fn query_index_advanced(&self, query: IndexQuery) -> Result<Vec<Vec<u8>>> {
        self.index_manager.query(query)
    }

    pub fn get_by_index(&self, index_name: &str, index_key: &IndexKey) -> Result<Option<Vec<u8>>> {
        // Query the index for the primary key
        let primary_keys = self.index_manager.get_keys(index_name, index_key)?;

        // If we found a match, get the value using the primary key
        if let Some(primary_key) = primary_keys.first() {
            self.get(primary_key)
        } else {
            Ok(None)
        }
    }

    pub fn put_indexed(
        &self,
        key: &[u8],
        value: &[u8],
        record: &dyn IndexableRecord,
    ) -> Result<()> {
        // First put the data
        self.put(key, value)?;

        // Then update indexes
        self.index_manager.update_indexes(key, record)?;

        Ok(())
    }

    pub fn delete_indexed(&self, key: &[u8], record: &dyn IndexableRecord) -> Result<()> {
        // First remove from indexes
        self.index_manager.remove_from_indexes(key, record)?;

        // Then delete the data
        self.delete(key)?;

        Ok(())
    }

    pub fn update_indexed(
        &self,
        key: &[u8],
        old_record: &dyn IndexableRecord,
        new_value: &[u8],
        new_record: &dyn IndexableRecord,
    ) -> Result<()> {
        // Remove old index entries
        self.index_manager.remove_from_indexes(key, old_record)?;

        // Update the data
        self.put(key, new_value)?;

        // Add new index entries
        self.index_manager.update_indexes(key, new_record)?;

        Ok(())
    }
}