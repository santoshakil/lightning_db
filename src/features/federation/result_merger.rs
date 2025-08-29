use std::sync::Arc;
use std::collections::{HashMap, BinaryHeap, VecDeque};
use std::cmp::Ordering;
use serde::{Serialize, Deserialize};
use crate::core::error::{Error, Result};
use tokio::sync::{mpsc, RwLock};
use futures::stream::{Stream, StreamExt};
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct ResultMerger {
    merge_strategy: MergeStrategy,
    buffer_size: usize,
    timeout_ms: u64,
    metrics: Arc<MergerMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeStrategy {
    Union,
    UnionAll,
    Intersection,
    Difference,
    Sort(Vec<SortKey>),
    Aggregate(Vec<AggregateSpec>),
    Join(JoinSpec),
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub column_index: usize,
    pub ascending: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateSpec {
    pub group_by: Vec<usize>,
    pub aggregates: Vec<AggregateFunction>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateFunction {
    pub column_index: usize,
    pub function: AggregateOp,
    pub alias: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateOp {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    First,
    Last,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinSpec {
    pub join_type: JoinType,
    pub left_keys: Vec<usize>,
    pub right_keys: Vec<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultSet {
    pub schema: Schema,
    pub rows: Vec<Row>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Binary,
    Timestamp,
    Date,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Binary(Vec<u8>),
    Timestamp(i64),
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Int32(a), Value::Int32(b)) => a.partial_cmp(b),
            (Value::Int64(a), Value::Int64(b)) => a.partial_cmp(b),
            (Value::Float32(a), Value::Float32(b)) => a.partial_cmp(b),
            (Value::Float64(a), Value::Float64(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

pub struct MergerMetrics {
    rows_processed: std::sync::atomic::AtomicU64,
    bytes_processed: std::sync::atomic::AtomicU64,
    merge_time_ms: std::sync::atomic::AtomicU64,
}

impl ResultMerger {
    pub fn new(strategy: MergeStrategy, buffer_size: usize) -> Self {
        Self {
            merge_strategy: strategy,
            buffer_size,
            timeout_ms: 30000,
            metrics: Arc::new(MergerMetrics {
                rows_processed: std::sync::atomic::AtomicU64::new(0),
                bytes_processed: std::sync::atomic::AtomicU64::new(0),
                merge_time_ms: std::sync::atomic::AtomicU64::new(0),
            }),
        }
    }
    
    pub async fn merge(&self, sources: Vec<ResultSet>) -> Result<ResultSet> {
        if sources.is_empty() {
            return Ok(ResultSet {
                schema: Schema { columns: vec![] },
                rows: vec![],
                metadata: HashMap::new(),
            });
        }
        
        let schema = sources[0].schema.clone();
        
        let merged_rows = match &self.merge_strategy {
            MergeStrategy::Union => self.merge_union(sources, false)?,
            MergeStrategy::UnionAll => self.merge_union(sources, true)?,
            MergeStrategy::Sort(keys) => self.merge_sort(sources, keys)?,
            MergeStrategy::Aggregate(spec) => self.merge_aggregate(sources, spec)?,
            _ => return Err(Error::UnsupportedFeature { 
                feature: format!("Merge strategy {:?}", self.merge_strategy) 
            }),
        };
        
        Ok(ResultSet {
            schema,
            rows: merged_rows,
            metadata: HashMap::new(),
        })
    }
    
    fn merge_union(&self, sources: Vec<ResultSet>, include_duplicates: bool) -> Result<Vec<Row>> {
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();
        
        for source in sources {
            for row in source.rows {
                let row_key = format!("{:?}", row.values);
                if include_duplicates || !seen.contains(&row_key) {
                    seen.insert(row_key.clone());
                    result.push(row);
                }
            }
        }
        
        self.metrics.rows_processed.store(
            result.len() as u64,
            std::sync::atomic::Ordering::Relaxed
        );
        
        Ok(result)
    }
    
    fn merge_sort(&self, sources: Vec<ResultSet>, keys: &[SortKey]) -> Result<Vec<Row>> {
        let mut all_rows = Vec::new();
        for source in sources {
            all_rows.extend(source.rows);
        }
        
        all_rows.sort_by(|a, b| {
            for key in keys {
                let a_val = &a.values[key.column_index];
                let b_val = &b.values[key.column_index];
                
                let ord = if key.ascending {
                    a_val.partial_cmp(b_val)
                } else {
                    b_val.partial_cmp(a_val)
                };
                
                if let Some(ordering) = ord {
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
            }
            Ordering::Equal
        });
        
        Ok(all_rows)
    }
    
    fn merge_aggregate(&self, sources: Vec<ResultSet>, spec: &[AggregateSpec]) -> Result<Vec<Row>> {
        let mut groups: HashMap<Vec<Value>, AggregateState> = HashMap::new();
        
        for source in sources {
            for row in source.rows {
                if let Some(agg_spec) = spec.first() {
                    let group_key: Vec<Value> = agg_spec.group_by.iter()
                        .map(|&idx| row.values[idx].clone())
                        .collect();
                    
                    let state = groups.entry(group_key).or_insert(AggregateState::new());
                    
                    for agg_func in &agg_spec.aggregates {
                        state.update(&agg_func.function, &row.values[agg_func.column_index]);
                    }
                }
            }
        }
        
        let mut result = Vec::new();
        for (group_key, state) in groups {
            let mut row_values = group_key;
            for agg_spec in spec {
                for agg_func in &agg_spec.aggregates {
                    row_values.push(state.finalize(&agg_func.function));
                }
            }
            result.push(Row { values: row_values });
        }
        
        Ok(result)
    }
}

struct AggregateState {
    sum: f64,
    count: u64,
    min: Option<Value>,
    max: Option<Value>,
}

impl AggregateState {
    fn new() -> Self {
        Self {
            sum: 0.0,
            count: 0,
            min: None,
            max: None,
        }
    }
    
    fn update(&mut self, op: &AggregateOp, value: &Value) {
        match op {
            AggregateOp::Sum => {
                if let Some(v) = self.to_f64(value) {
                    self.sum += v;
                }
            }
            AggregateOp::Count => {
                if !matches!(value, Value::Null) {
                    self.count += 1;
                }
            }
            AggregateOp::Min => {
                if self.min.is_none() || value < self.min.as_ref().unwrap() {
                    self.min = Some(value.clone());
                }
            }
            AggregateOp::Max => {
                if self.max.is_none() || value > self.max.as_ref().unwrap() {
                    self.max = Some(value.clone());
                }
            }
            AggregateOp::Avg => {
                if let Some(v) = self.to_f64(value) {
                    self.sum += v;
                    self.count += 1;
                }
            }
            _ => {}
        }
    }
    
    fn finalize(&self, op: &AggregateOp) -> Value {
        match op {
            AggregateOp::Sum => Value::Float64(self.sum),
            AggregateOp::Count => Value::Int64(self.count as i64),
            AggregateOp::Min => self.min.clone().unwrap_or(Value::Null),
            AggregateOp::Max => self.max.clone().unwrap_or(Value::Null),
            AggregateOp::Avg => {
                if self.count > 0 {
                    Value::Float64(self.sum / self.count as f64)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        }
    }
    
    fn to_f64(&self, value: &Value) -> Option<f64> {
        match value {
            Value::Int32(v) => Some(*v as f64),
            Value::Int64(v) => Some(*v as f64),
            Value::Float32(v) => Some(*v as f64),
            Value::Float64(v) => Some(*v),
            _ => None,
        }
    }
}

pub struct StreamingMerger {
    strategy: MergeStrategy,
    buffer_size: usize,
    channels: Vec<mpsc::Receiver<Row>>,
}

impl StreamingMerger {
    pub fn new(
        strategy: MergeStrategy,
        buffer_size: usize,
        channels: Vec<mpsc::Receiver<Row>>,
    ) -> Self {
        Self {
            strategy,
            buffer_size,
            channels,
        }
    }
    
    pub async fn merge_stream(&mut self) -> mpsc::Receiver<Row> {
        let (tx, rx) = mpsc::channel(self.buffer_size);
        
        let strategy = self.strategy.clone();
        let mut channels = std::mem::take(&mut self.channels);
        
        tokio::spawn(async move {
            match strategy {
                MergeStrategy::Sort(keys) => {
                    Self::merge_sorted_streams(channels, tx, keys).await;
                }
                _ => {
                    Self::merge_simple_streams(channels, tx).await;
                }
            }
        });
        
        rx
    }
    
    async fn merge_simple_streams(
        mut channels: Vec<mpsc::Receiver<Row>>,
        tx: mpsc::Sender<Row>,
    ) {
        loop {
            let mut any_active = false;
            
            for channel in &mut channels {
                if let Some(row) = channel.recv().await {
                    any_active = true;
                    if tx.send(row).await.is_err() {
                        return;
                    }
                }
            }
            
            if !any_active {
                break;
            }
        }
    }
    
    async fn merge_sorted_streams(
        mut channels: Vec<mpsc::Receiver<Row>>,
        tx: mpsc::Sender<Row>,
        keys: Vec<SortKey>,
    ) {
        struct HeapEntry {
            row: Row,
            channel_idx: usize,
        }
        
        impl PartialEq for HeapEntry {
            fn eq(&self, other: &Self) -> bool {
                self.row.values == other.row.values
            }
        }
        
        impl Eq for HeapEntry {}
        
        impl PartialOrd for HeapEntry {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        
        impl Ord for HeapEntry {
            fn cmp(&self, other: &Self) -> Ordering {
                Ordering::Equal
            }
        }
        
        let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
        
        for (idx, channel) in channels.iter_mut().enumerate() {
            if let Some(row) = channel.recv().await {
                heap.push(HeapEntry {
                    row,
                    channel_idx: idx,
                });
            }
        }
        
        while let Some(entry) = heap.pop() {
            if tx.send(entry.row).await.is_err() {
                break;
            }
            
            if let Some(next_row) = channels[entry.channel_idx].recv().await {
                heap.push(HeapEntry {
                    row: next_row,
                    channel_idx: entry.channel_idx,
                });
            }
        }
    }
}