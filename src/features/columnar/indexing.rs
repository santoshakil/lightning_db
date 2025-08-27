use std::collections::{BTreeMap, HashMap};
use crate::core::error::Error;
use super::engine::Value;

#[derive(Debug, Clone)]
pub enum ColumnIndex {
    BTree(BTreeIndex),
    Hash(HashIndex),
    Bitmap(BitmapIndex),
}

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    index: BTreeMap<Vec<u8>, Vec<u64>>,
}

#[derive(Debug, Clone)]
pub struct HashIndex {
    index: HashMap<Vec<u8>, Vec<u64>>,
}

#[derive(Debug, Clone)]
pub struct BitmapIndex {
    bitmaps: HashMap<Vec<u8>, roaring::RoaringBitmap>,
}

pub struct IndexBuilder {
    index_type: IndexType,
}

#[derive(Debug, Clone, Copy)]
pub enum IndexType {
    BTree,
    Hash,
    Bitmap,
}

impl IndexBuilder {
    pub fn new(index_type: IndexType) -> Self {
        Self { index_type }
    }

    pub fn build(&self, values: &[Value]) -> Result<ColumnIndex, Error> {
        match self.index_type {
            IndexType::BTree => {
                let mut index = BTreeMap::new();
                for (i, value) in values.iter().enumerate() {
                    let key = self.value_to_bytes(value)?;
                    index.entry(key)
                        .or_insert_with(Vec::new)
                        .push(i as u64);
                }
                Ok(ColumnIndex::BTree(BTreeIndex { index }))
            },
            IndexType::Hash => {
                let mut index = HashMap::new();
                for (i, value) in values.iter().enumerate() {
                    let key = self.value_to_bytes(value)?;
                    index.entry(key)
                        .or_insert_with(Vec::new)
                        .push(i as u64);
                }
                Ok(ColumnIndex::Hash(HashIndex { index }))
            },
            IndexType::Bitmap => {
                let mut bitmaps = HashMap::new();
                for (i, value) in values.iter().enumerate() {
                    let key = self.value_to_bytes(value)?;
                    bitmaps.entry(key)
                        .or_insert_with(roaring::RoaringBitmap::new)
                        .insert(i as u32);
                }
                Ok(ColumnIndex::Bitmap(BitmapIndex { bitmaps }))
            },
        }
    }

    fn value_to_bytes(&self, value: &Value) -> Result<Vec<u8>, Error> {
        match value {
            Value::Int32(v) => Ok(v.to_le_bytes().to_vec()),
            Value::Int64(v) => Ok(v.to_le_bytes().to_vec()),
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            _ => Ok(Vec::new()),
        }
    }
}

impl ColumnIndex {
    pub fn lookup(&self, key: &[u8]) -> Vec<u64> {
        match self {
            ColumnIndex::BTree(idx) => {
                idx.index.get(key).cloned().unwrap_or_default()
            },
            ColumnIndex::Hash(idx) => {
                idx.index.get(key).cloned().unwrap_or_default()
            },
            ColumnIndex::Bitmap(idx) => {
                idx.bitmaps.get(key)
                    .map(|bitmap| bitmap.iter().map(|v| v as u64).collect())
                    .unwrap_or_default()
            },
        }
    }
}