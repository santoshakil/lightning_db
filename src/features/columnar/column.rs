use std::sync::Arc;
use bytes::{Bytes, BytesMut};
use crate::core::error::Error;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnType {
    Fixed(usize),
    Variable,
    Dictionary,
    Compressed,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub column_type: ColumnType,
    pub nullable: bool,
    pub data: ColumnData,
    pub statistics: Option<ColumnStats>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Binary,
    Timestamp,
    Date,
    Decimal(u8, u8),
}

#[derive(Debug, Clone)]
pub enum ColumnData {
    Bool(Vec<bool>),
    Int8(Vec<i8>),
    Int16(Vec<i16>),
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    UInt8(Vec<u8>),
    UInt16(Vec<u16>),
    UInt32(Vec<u32>),
    UInt64(Vec<u64>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
    Null(usize),
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub null_count: usize,
    pub distinct_count: Option<usize>,
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    pub sum: Option<f64>,
    pub mean: Option<f64>,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            column_type: ColumnType::Variable,
            nullable: true,
            data: ColumnData::Null(0),
            statistics: None,
        }
    }

    pub fn len(&self) -> usize {
        match &self.data {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int32(v) => v.len(),
            ColumnData::Int64(v) => v.len(),
            ColumnData::Float64(v) => v.len(),
            ColumnData::String(v) => v.len(),
            ColumnData::Binary(v) => v.len(),
            ColumnData::Null(n) => *n,
            _ => 0,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn memory_size(&self) -> usize {
        match &self.data {
            ColumnData::Bool(v) => v.len(),
            ColumnData::Int8(v) => v.len(),
            ColumnData::Int16(v) => v.len() * 2,
            ColumnData::Int32(v) => v.len() * 4,
            ColumnData::Int64(v) => v.len() * 8,
            ColumnData::Float32(v) => v.len() * 4,
            ColumnData::Float64(v) => v.len() * 8,
            ColumnData::String(v) => v.iter().map(|s| s.len()).sum(),
            ColumnData::Binary(v) => v.iter().map(|b| b.len()).sum(),
            _ => 0,
        }
    }
}