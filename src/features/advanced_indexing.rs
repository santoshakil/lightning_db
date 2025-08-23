use crate::core::error::{Error, Result};
use crate::core::index::{IndexKey, IndexType};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use std::collections::{HashMap, BTreeMap, HashSet};
use std::sync::Arc;
use tracing::{debug, info, warn};
use serde::{Serialize, Deserialize};
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdvancedIndexType {
    Bitmap,
    Hash,
    Spatial,
    FullText,
    Covering { columns: Vec<String> },
    Partial { predicate: String },
    Expression { expression: String },
    BloomFilter { false_positive_rate: f64 },
    TrieIndex,
    InvertedIndex,
}

pub trait AdvancedIndex: Send + Sync {
    fn index_type(&self) -> AdvancedIndexType;
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn search(&self, query: &IndexQuery) -> Result<Vec<Vec<u8>>>;
    fn delete(&mut self, key: &[u8]) -> Result<bool>;
    fn size(&self) -> usize;
    fn memory_usage(&self) -> usize;
    fn rebuild(&mut self) -> Result<()>;
}

#[derive(Debug, Clone)]
pub enum IndexQuery {
    Exact(Vec<u8>),
    Range { start: Vec<u8>, end: Vec<u8> },
    Prefix(Vec<u8>),
    Contains(Vec<u8>),
    Spatial(SpatialQuery),
    FullText(String),
    Expression(String),
    Bitmap(BitmapQuery),
}

#[derive(Debug, Clone)]
pub struct SpatialQuery {
    pub query_type: SpatialQueryType,
    pub coordinates: Vec<f64>,
    pub radius: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum SpatialQueryType {
    Point,
    Range,
    NearestNeighbor { k: usize },
    Within,
    Intersects,
}

#[derive(Debug, Clone)]
pub struct BitmapQuery {
    pub operation: BitmapOperation,
    pub operands: Vec<u32>,
}

#[derive(Debug, Clone)]
pub enum BitmapOperation {
    And,
    Or,
    Xor,
    Not,
}

pub struct BitmapIndex {
    bitmaps: HashMap<Vec<u8>, RoaringBitmap>,
    value_to_id: HashMap<Vec<u8>, u32>,
    id_to_value: HashMap<u32, Vec<u8>>,
    next_id: u32,
}

impl BitmapIndex {
    pub fn new() -> Self {
        Self {
            bitmaps: HashMap::new(),
            value_to_id: HashMap::new(),
            id_to_value: HashMap::new(),
            next_id: 0,
        }
    }
    
    fn get_or_create_id(&mut self, value: &[u8]) -> u32 {
        if let Some(&id) = self.value_to_id.get(value) {
            return id;
        }
        
        let id = self.next_id;
        self.next_id += 1;
        self.value_to_id.insert(value.to_vec(), id);
        self.id_to_value.insert(id, value.to_vec());
        id
    }
}

impl AdvancedIndex for BitmapIndex {
    fn index_type(&self) -> AdvancedIndexType {
        AdvancedIndexType::Bitmap
    }
    
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let id = self.get_or_create_id(&value);
        let bitmap = self.bitmaps.entry(key).or_insert_with(RoaringBitmap::new);
        bitmap.insert(id);
        Ok(())
    }
    
    fn search(&self, query: &IndexQuery) -> Result<Vec<Vec<u8>>> {
        match query {
            IndexQuery::Exact(key) => {
                if let Some(bitmap) = self.bitmaps.get(key) {
                    let results: Vec<Vec<u8>> = bitmap
                        .iter()
                        .filter_map(|id| self.id_to_value.get(&id).cloned())
                        .collect();
                    Ok(results)
                } else {
                    Ok(Vec::new())
                }
            }
            IndexQuery::Bitmap(bq) => {
                let mut result_bitmap = RoaringBitmap::new();
                
                match &bq.operation {
                    BitmapOperation::And => {
                        if !bq.operands.is_empty() {
                            result_bitmap.extend(&bq.operands);
                            for bitmap in self.bitmaps.values() {
                                result_bitmap &= bitmap;
                            }
                        }
                    }
                    BitmapOperation::Or => {
                        for bitmap in self.bitmaps.values() {
                            result_bitmap |= bitmap;
                        }
                    }
                    BitmapOperation::Xor => {
                        for bitmap in self.bitmaps.values() {
                            result_bitmap ^= bitmap;
                        }
                    }
                    BitmapOperation::Not => {
                        let all_ids: RoaringBitmap = (0..self.next_id).collect();
                        for bitmap in self.bitmaps.values() {
                            result_bitmap = &all_ids - bitmap;
                        }
                    }
                }
                
                let results: Vec<Vec<u8>> = result_bitmap
                    .iter()
                    .filter_map(|id| self.id_to_value.get(&id).cloned())
                    .collect();
                Ok(results)
            }
            _ => Ok(Vec::new()),
        }
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.bitmaps.remove(key).is_some())
    }
    
    fn size(&self) -> usize {
        self.bitmaps.len()
    }
    
    fn memory_usage(&self) -> usize {
        self.bitmaps.values()
            .map(|b| b.serialized_size())
            .sum::<usize>()
            + self.value_to_id.len() * 32
            + self.id_to_value.len() * 32
    }
    
    fn rebuild(&mut self) -> Result<()> {
        for bitmap in self.bitmaps.values_mut() {
            bitmap.run_optimize();
        }
        Ok(())
    }
}

pub struct HashIndex {
    buckets: Vec<RwLock<HashMap<u64, Vec<(Vec<u8>, Vec<u8>)>>>>,
    bucket_count: usize,
}

impl HashIndex {
    pub fn new(bucket_count: usize) -> Self {
        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(RwLock::new(HashMap::new()));
        }
        
        Self {
            buckets,
            bucket_count,
        }
    }
    
    fn hash_key(&self, key: &[u8]) -> (usize, u64) {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        let bucket_idx = (hash as usize) % self.bucket_count;
        (bucket_idx, hash)
    }
}

impl AdvancedIndex for HashIndex {
    fn index_type(&self) -> AdvancedIndexType {
        AdvancedIndexType::Hash
    }
    
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let (bucket_idx, hash) = self.hash_key(&key);
        let mut bucket = self.buckets[bucket_idx].write();
        
        bucket.entry(hash)
            .or_insert_with(Vec::new)
            .push((key, value));
        
        Ok(())
    }
    
    fn search(&self, query: &IndexQuery) -> Result<Vec<Vec<u8>>> {
        match query {
            IndexQuery::Exact(key) => {
                let (bucket_idx, hash) = self.hash_key(key);
                let bucket = self.buckets[bucket_idx].read();
                
                if let Some(entries) = bucket.get(&hash) {
                    let results = entries
                        .iter()
                        .filter(|(k, _)| k == key)
                        .map(|(_, v)| v.clone())
                        .collect();
                    Ok(results)
                } else {
                    Ok(Vec::new())
                }
            }
            _ => Ok(Vec::new()),
        }
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let (bucket_idx, hash) = self.hash_key(key);
        let mut bucket = self.buckets[bucket_idx].write();
        
        if let Some(entries) = bucket.get_mut(&hash) {
            let original_len = entries.len();
            entries.retain(|(k, _)| k != key);
            
            if entries.is_empty() {
                bucket.remove(&hash);
            }
            
            Ok(original_len > entries.len())
        } else {
            Ok(false)
        }
    }
    
    fn size(&self) -> usize {
        self.buckets.iter()
            .map(|b| b.read().len())
            .sum()
    }
    
    fn memory_usage(&self) -> usize {
        self.buckets.iter()
            .map(|b| {
                let bucket = b.read();
                bucket.values()
                    .map(|entries| entries.len() * 64)
                    .sum::<usize>()
            })
            .sum()
    }
    
    fn rebuild(&mut self) -> Result<()> {
        for bucket in &self.buckets {
            bucket.write().shrink_to_fit();
        }
        Ok(())
    }
}

pub struct FullTextIndex {
    documents: HashMap<u64, String>,
    inverted_index: HashMap<String, RoaringBitmap>,
    doc_count: u64,
}

impl FullTextIndex {
    pub fn new() -> Self {
        Self {
            documents: HashMap::new(),
            inverted_index: HashMap::new(),
            doc_count: 0,
        }
    }
    
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()))
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect()
    }
    
    fn calculate_tf_idf(&self, term: &str, doc_id: u64) -> f64 {
        let doc = &self.documents[&doc_id];
        let tokens = self.tokenize(doc);
        let term_freq = tokens.iter().filter(|t| *t == term).count() as f64;
        let doc_len = tokens.len() as f64;
        
        let tf = term_freq / doc_len;
        
        let doc_freq = self.inverted_index
            .get(term)
            .map(|b| b.len() as f64)
            .unwrap_or(1.0);
        let idf = ((self.doc_count as f64) / doc_freq).ln();
        
        tf * idf
    }
}

impl AdvancedIndex for FullTextIndex {
    fn index_type(&self) -> AdvancedIndexType {
        AdvancedIndexType::FullText
    }
    
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let doc_id = u64::from_le_bytes(key[..8].try_into().unwrap_or([0; 8]));
        let text = String::from_utf8_lossy(&value).to_string();
        
        self.documents.insert(doc_id, text.clone());
        
        for token in self.tokenize(&text) {
            self.inverted_index
                .entry(token)
                .or_insert_with(RoaringBitmap::new)
                .insert(doc_id as u32);
        }
        
        self.doc_count += 1;
        Ok(())
    }
    
    fn search(&self, query: &IndexQuery) -> Result<Vec<Vec<u8>>> {
        match query {
            IndexQuery::FullText(text) => {
                let tokens = self.tokenize(text);
                let mut doc_scores: HashMap<u64, f64> = HashMap::new();
                
                for token in &tokens {
                    if let Some(bitmap) = self.inverted_index.get(token) {
                        for doc_id in bitmap.iter() {
                            let doc_id = doc_id as u64;
                            let score = self.calculate_tf_idf(token, doc_id);
                            *doc_scores.entry(doc_id).or_default() += score;
                        }
                    }
                }
                
                let mut results: Vec<(u64, f64)> = doc_scores.into_iter().collect();
                results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                
                Ok(results
                    .into_iter()
                    .take(100)
                    .map(|(doc_id, _)| doc_id.to_le_bytes().to_vec())
                    .collect())
            }
            _ => Ok(Vec::new()),
        }
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let doc_id = u64::from_le_bytes(key[..8].try_into().unwrap_or([0; 8]));
        
        if let Some(text) = self.documents.remove(&doc_id) {
            for token in self.tokenize(&text) {
                if let Some(bitmap) = self.inverted_index.get_mut(&token) {
                    bitmap.remove(doc_id as u32);
                    if bitmap.is_empty() {
                        self.inverted_index.remove(&token);
                    }
                }
            }
            self.doc_count = self.doc_count.saturating_sub(1);
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    fn size(&self) -> usize {
        self.documents.len()
    }
    
    fn memory_usage(&self) -> usize {
        self.documents.values()
            .map(|s| s.len())
            .sum::<usize>()
            + self.inverted_index.values()
                .map(|b| b.serialized_size())
                .sum::<usize>()
    }
    
    fn rebuild(&mut self) -> Result<()> {
        for bitmap in self.inverted_index.values_mut() {
            bitmap.run_optimize();
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct SpatialPoint {
    x: f64,
    y: f64,
    data: Vec<u8>,
}

pub struct SpatialIndex {
    points: Vec<SpatialPoint>,
    rtree: Option<RTree>,
}

struct RTree {
    root: RTreeNode,
    max_entries: usize,
}

enum RTreeNode {
    Leaf {
        entries: Vec<(BoundingBox, usize)>,
    },
    Internal {
        entries: Vec<(BoundingBox, Box<RTreeNode>)>,
    },
}

#[derive(Debug, Clone, Copy)]
struct BoundingBox {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
}

impl BoundingBox {
    fn contains_point(&self, x: f64, y: f64) -> bool {
        x >= self.min_x && x <= self.max_x && y >= self.min_y && y <= self.max_y
    }
    
    fn intersects(&self, other: &BoundingBox) -> bool {
        !(self.max_x < other.min_x || self.min_x > other.max_x ||
          self.max_y < other.min_y || self.min_y > other.max_y)
    }
    
    fn distance_to_point(&self, x: f64, y: f64) -> f64 {
        let dx = if x < self.min_x {
            self.min_x - x
        } else if x > self.max_x {
            x - self.max_x
        } else {
            0.0
        };
        
        let dy = if y < self.min_y {
            self.min_y - y
        } else if y > self.max_y {
            y - self.max_y
        } else {
            0.0
        };
        
        (dx * dx + dy * dy).sqrt()
    }
}

impl SpatialIndex {
    pub fn new() -> Self {
        Self {
            points: Vec::new(),
            rtree: None,
        }
    }
    
    fn rebuild_rtree(&mut self) {
        if self.points.is_empty() {
            self.rtree = None;
            return;
        }
        
        let mut entries = Vec::new();
        for (idx, point) in self.points.iter().enumerate() {
            let bbox = BoundingBox {
                min_x: point.x,
                min_y: point.y,
                max_x: point.x,
                max_y: point.y,
            };
            entries.push((bbox, idx));
        }
        
        self.rtree = Some(RTree {
            root: RTreeNode::Leaf { entries },
            max_entries: 50,
        });
    }
}

impl AdvancedIndex for SpatialIndex {
    fn index_type(&self) -> AdvancedIndexType {
        AdvancedIndexType::Spatial
    }
    
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if key.len() < 16 {
            return Err(Error::InvalidArgument(
                "Spatial key must be at least 16 bytes (two f64 coordinates)".to_string()
            ));
        }
        
        let x = f64::from_le_bytes(key[0..8].try_into().unwrap());
        let y = f64::from_le_bytes(key[8..16].try_into().unwrap());
        
        self.points.push(SpatialPoint {
            x,
            y,
            data: value,
        });
        
        if self.points.len() % 100 == 0 {
            self.rebuild_rtree();
        }
        
        Ok(())
    }
    
    fn search(&self, query: &IndexQuery) -> Result<Vec<Vec<u8>>> {
        match query {
            IndexQuery::Spatial(sq) => {
                match sq.query_type {
                    SpatialQueryType::Point => {
                        if sq.coordinates.len() < 2 {
                            return Ok(Vec::new());
                        }
                        
                        let x = sq.coordinates[0];
                        let y = sq.coordinates[1];
                        
                        let results = self.points
                            .iter()
                            .filter(|p| (p.x - x).abs() < 1e-6 && (p.y - y).abs() < 1e-6)
                            .map(|p| p.data.clone())
                            .collect();
                        
                        Ok(results)
                    }
                    SpatialQueryType::Range => {
                        if sq.coordinates.len() < 4 {
                            return Ok(Vec::new());
                        }
                        
                        let bbox = BoundingBox {
                            min_x: sq.coordinates[0],
                            min_y: sq.coordinates[1],
                            max_x: sq.coordinates[2],
                            max_y: sq.coordinates[3],
                        };
                        
                        let results = self.points
                            .iter()
                            .filter(|p| bbox.contains_point(p.x, p.y))
                            .map(|p| p.data.clone())
                            .collect();
                        
                        Ok(results)
                    }
                    SpatialQueryType::NearestNeighbor { k } => {
                        if sq.coordinates.len() < 2 {
                            return Ok(Vec::new());
                        }
                        
                        let x = sq.coordinates[0];
                        let y = sq.coordinates[1];
                        
                        let mut distances: Vec<(f64, &SpatialPoint)> = self.points
                            .iter()
                            .map(|p| {
                                let dist = ((p.x - x).powi(2) + (p.y - y).powi(2)).sqrt();
                                (dist, p)
                            })
                            .collect();
                        
                        distances.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                        
                        let results = distances
                            .into_iter()
                            .take(k)
                            .map(|(_, p)| p.data.clone())
                            .collect();
                        
                        Ok(results)
                    }
                    SpatialQueryType::Within => {
                        if sq.coordinates.len() < 2 || sq.radius.is_none() {
                            return Ok(Vec::new());
                        }
                        
                        let x = sq.coordinates[0];
                        let y = sq.coordinates[1];
                        let radius = sq.radius.unwrap();
                        
                        let results = self.points
                            .iter()
                            .filter(|p| {
                                let dist = ((p.x - x).powi(2) + (p.y - y).powi(2)).sqrt();
                                dist <= radius
                            })
                            .map(|p| p.data.clone())
                            .collect();
                        
                        Ok(results)
                    }
                    _ => Ok(Vec::new()),
                }
            }
            _ => Ok(Vec::new()),
        }
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        if key.len() < 16 {
            return Ok(false);
        }
        
        let x = f64::from_le_bytes(key[0..8].try_into().unwrap());
        let y = f64::from_le_bytes(key[8..16].try_into().unwrap());
        
        let original_len = self.points.len();
        self.points.retain(|p| (p.x - x).abs() >= 1e-6 || (p.y - y).abs() >= 1e-6);
        
        if self.points.len() < original_len {
            self.rebuild_rtree();
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    fn size(&self) -> usize {
        self.points.len()
    }
    
    fn memory_usage(&self) -> usize {
        self.points.len() * (16 + 32)
    }
    
    fn rebuild(&mut self) -> Result<()> {
        self.rebuild_rtree();
        Ok(())
    }
}

pub struct BloomFilterIndex {
    filters: HashMap<Vec<u8>, BloomFilter>,
    false_positive_rate: f64,
}

struct BloomFilter {
    bits: Vec<bool>,
    hash_count: usize,
    size: usize,
}

impl BloomFilter {
    fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let size = Self::optimal_size(expected_items, false_positive_rate);
        let hash_count = Self::optimal_hash_count(size, expected_items);
        
        Self {
            bits: vec![false; size],
            hash_count,
            size,
        }
    }
    
    fn optimal_size(n: usize, p: f64) -> usize {
        let m = -(n as f64 * p.ln()) / (2.0_f64.ln().powi(2));
        m.ceil() as usize
    }
    
    fn optimal_hash_count(m: usize, n: usize) -> usize {
        let k = (m as f64 / n as f64) * 2.0_f64.ln();
        k.ceil() as usize
    }
    
    fn hash(&self, item: &[u8], seed: u64) -> usize {
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(seed);
        hasher.write(item);
        (hasher.finish() as usize) % self.size
    }
    
    fn add(&mut self, item: &[u8]) {
        for i in 0..self.hash_count {
            let idx = self.hash(item, i as u64);
            self.bits[idx] = true;
        }
    }
    
    fn contains(&self, item: &[u8]) -> bool {
        for i in 0..self.hash_count {
            let idx = self.hash(item, i as u64);
            if !self.bits[idx] {
                return false;
            }
        }
        true
    }
}

impl BloomFilterIndex {
    pub fn new(false_positive_rate: f64) -> Self {
        Self {
            filters: HashMap::new(),
            false_positive_rate,
        }
    }
}

impl AdvancedIndex for BloomFilterIndex {
    fn index_type(&self) -> AdvancedIndexType {
        AdvancedIndexType::BloomFilter {
            false_positive_rate: self.false_positive_rate,
        }
    }
    
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let filter = self.filters
            .entry(key.clone())
            .or_insert_with(|| BloomFilter::new(1000, self.false_positive_rate));
        
        filter.add(&value);
        Ok(())
    }
    
    fn search(&self, query: &IndexQuery) -> Result<Vec<Vec<u8>>> {
        match query {
            IndexQuery::Contains(value) => {
                let mut results = Vec::new();
                
                for (key, filter) in &self.filters {
                    if filter.contains(value) {
                        results.push(key.clone());
                    }
                }
                
                Ok(results)
            }
            _ => Ok(Vec::new()),
        }
    }
    
    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.filters.remove(key).is_some())
    }
    
    fn size(&self) -> usize {
        self.filters.len()
    }
    
    fn memory_usage(&self) -> usize {
        self.filters.values()
            .map(|f| f.size / 8)
            .sum()
    }
    
    fn rebuild(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct AdvancedIndexManager {
    indexes: Arc<RwLock<HashMap<String, Box<dyn AdvancedIndex>>>>,
    statistics: Arc<RwLock<IndexStatistics>>,
}

#[derive(Debug, Default)]
struct IndexStatistics {
    total_queries: u64,
    total_inserts: u64,
    total_deletes: u64,
    cache_hits: u64,
    cache_misses: u64,
}

impl AdvancedIndexManager {
    pub fn new() -> Self {
        Self {
            indexes: Arc::new(RwLock::new(HashMap::new())),
            statistics: Arc::new(RwLock::new(IndexStatistics::default())),
        }
    }
    
    pub fn create_index(&self, name: String, index_type: AdvancedIndexType) -> Result<()> {
        let index: Box<dyn AdvancedIndex> = match index_type {
            AdvancedIndexType::Bitmap => Box::new(BitmapIndex::new()),
            AdvancedIndexType::Hash => Box::new(HashIndex::new(1024)),
            AdvancedIndexType::Spatial => Box::new(SpatialIndex::new()),
            AdvancedIndexType::FullText => Box::new(FullTextIndex::new()),
            AdvancedIndexType::BloomFilter { false_positive_rate } => {
                Box::new(BloomFilterIndex::new(false_positive_rate))
            }
            _ => {
                return Err(Error::Generic(
                    "Index type not yet implemented".to_string()
                ));
            }
        };
        
        let mut indexes = self.indexes.write();
        indexes.insert(name.clone(), index);
        
        info!("Created {} index: {}", 
            match index_type {
                AdvancedIndexType::Bitmap => "bitmap",
                AdvancedIndexType::Hash => "hash",
                AdvancedIndexType::Spatial => "spatial",
                AdvancedIndexType::FullText => "full-text",
                AdvancedIndexType::BloomFilter { .. } => "bloom filter",
                _ => "unknown",
            },
            name
        );
        
        Ok(())
    }
    
    pub fn insert(&self, index_name: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let mut indexes = self.indexes.write();
        
        if let Some(index) = indexes.get_mut(index_name) {
            index.insert(key, value)?;
            
            let mut stats = self.statistics.write();
            stats.total_inserts += 1;
            
            Ok(())
        } else {
            Err(Error::Generic(format!("Index not found: {}", index_name)))
        }
    }
    
    pub fn search(&self, index_name: &str, query: &IndexQuery) -> Result<Vec<Vec<u8>>> {
        let indexes = self.indexes.read();
        
        if let Some(index) = indexes.get(index_name) {
            let results = index.search(query)?;
            
            let mut stats = self.statistics.write();
            stats.total_queries += 1;
            
            Ok(results)
        } else {
            Err(Error::Generic(format!("Index not found: {}", index_name)))
        }
    }
    
    pub fn delete(&self, index_name: &str, key: &[u8]) -> Result<bool> {
        let mut indexes = self.indexes.write();
        
        if let Some(index) = indexes.get_mut(index_name) {
            let deleted = index.delete(key)?;
            
            if deleted {
                let mut stats = self.statistics.write();
                stats.total_deletes += 1;
            }
            
            Ok(deleted)
        } else {
            Err(Error::Generic(format!("Index not found: {}", index_name)))
        }
    }
    
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        let mut indexes = self.indexes.write();
        
        if indexes.remove(index_name).is_some() {
            info!("Dropped index: {}", index_name);
            Ok(())
        } else {
            Err(Error::Generic(format!("Index not found: {}", index_name)))
        }
    }
    
    pub fn rebuild_index(&self, index_name: &str) -> Result<()> {
        let mut indexes = self.indexes.write();
        
        if let Some(index) = indexes.get_mut(index_name) {
            index.rebuild()?;
            info!("Rebuilt index: {}", index_name);
            Ok(())
        } else {
            Err(Error::Generic(format!("Index not found: {}", index_name)))
        }
    }
    
    pub fn get_index_info(&self) -> Vec<(String, AdvancedIndexType, usize, usize)> {
        let indexes = self.indexes.read();
        
        indexes.iter()
            .map(|(name, index)| {
                (
                    name.clone(),
                    index.index_type(),
                    index.size(),
                    index.memory_usage(),
                )
            })
            .collect()
    }
    
    pub fn optimize_all(&self) -> Result<()> {
        let mut indexes = self.indexes.write();
        
        for (name, index) in indexes.iter_mut() {
            index.rebuild()?;
            debug!("Optimized index: {}", name);
        }
        
        info!("Optimized all {} indexes", indexes.len());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bitmap_index() {
        let mut index = BitmapIndex::new();
        
        index.insert(b"color".to_vec(), b"red".to_vec()).unwrap();
        index.insert(b"color".to_vec(), b"blue".to_vec()).unwrap();
        index.insert(b"color".to_vec(), b"red".to_vec()).unwrap();
        
        let results = index.search(&IndexQuery::Exact(b"color".to_vec())).unwrap();
        assert_eq!(results.len(), 2);
    }
    
    #[test]
    fn test_hash_index() {
        let mut index = HashIndex::new(16);
        
        index.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
        index.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();
        
        let results = index.search(&IndexQuery::Exact(b"key1".to_vec())).unwrap();
        assert_eq!(results, vec![b"value1".to_vec()]);
    }
    
    #[test]
    fn test_full_text_index() {
        let mut index = FullTextIndex::new();
        
        let doc1 = "The quick brown fox jumps over the lazy dog";
        let doc2 = "The dog is sleeping";
        
        index.insert(1u64.to_le_bytes().to_vec(), doc1.as_bytes().to_vec()).unwrap();
        index.insert(2u64.to_le_bytes().to_vec(), doc2.as_bytes().to_vec()).unwrap();
        
        let results = index.search(&IndexQuery::FullText("dog".to_string())).unwrap();
        assert_eq!(results.len(), 2);
    }
    
    #[test]
    fn test_spatial_index() {
        let mut index = SpatialIndex::new();
        
        let mut key1 = Vec::new();
        key1.extend_from_slice(&1.0f64.to_le_bytes());
        key1.extend_from_slice(&2.0f64.to_le_bytes());
        
        let mut key2 = Vec::new();
        key2.extend_from_slice(&3.0f64.to_le_bytes());
        key2.extend_from_slice(&4.0f64.to_le_bytes());
        
        index.insert(key1, b"point1".to_vec()).unwrap();
        index.insert(key2, b"point2".to_vec()).unwrap();
        
        let query = IndexQuery::Spatial(SpatialQuery {
            query_type: SpatialQueryType::NearestNeighbor { k: 1 },
            coordinates: vec![0.0, 0.0],
            radius: None,
        });
        
        let results = index.search(&query).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], b"point1".to_vec());
    }
    
    #[test]
    fn test_bloom_filter_index() {
        let mut index = BloomFilterIndex::new(0.01);
        
        index.insert(b"set1".to_vec(), b"item1".to_vec()).unwrap();
        index.insert(b"set1".to_vec(), b"item2".to_vec()).unwrap();
        index.insert(b"set2".to_vec(), b"item3".to_vec()).unwrap();
        
        let results = index.search(&IndexQuery::Contains(b"item1".to_vec())).unwrap();
        assert!(results.contains(&b"set1".to_vec()));
        
        let results = index.search(&IndexQuery::Contains(b"item4".to_vec())).unwrap();
        assert_eq!(results.len(), 0);
    }
    
    #[test]
    fn test_index_manager() {
        let manager = AdvancedIndexManager::new();
        
        manager.create_index(
            "colors".to_string(),
            AdvancedIndexType::Bitmap,
        ).unwrap();
        
        manager.insert("colors", b"doc1".to_vec(), b"red".to_vec()).unwrap();
        manager.insert("colors", b"doc2".to_vec(), b"blue".to_vec()).unwrap();
        
        let results = manager.search(
            "colors",
            &IndexQuery::Exact(b"doc1".to_vec()),
        ).unwrap();
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], b"red".to_vec());
        
        manager.drop_index("colors").unwrap();
    }
}