use std::sync::Arc;
use std::collections::HashMap;
use crate::core::error::Error;
use super::engine::{Value, ColumnarEngine};

#[derive(Debug, Clone)]
pub struct ColumnarQuery {
    pub table_name: String,
    pub columns: Vec<String>,
    pub predicate: Option<Predicate>,
    pub limit: Option<usize>,
    pub order_by: Vec<OrderBy>,
}

#[derive(Debug, Clone)]
pub enum Predicate {
    Equals(String, Value),
    NotEquals(String, Value),
    GreaterThan(String, Value),
    LessThan(String, Value),
    In(String, Vec<Value>),
    Between(String, Value, Value),
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub descending: bool,
}

pub struct QueryExecutor {
    engine: Arc<ColumnarEngine>,
}

impl QueryExecutor {
    pub fn new(engine: Arc<ColumnarEngine>) -> Self {
        Self { engine }
    }

    pub async fn execute(&self, query: ColumnarQuery) -> Result<QueryResult, Error> {
        self.engine.query(
            &query.table_name,
            query.columns,
            query.predicate,
        ).await.map(|r| QueryResult {
            columns: r.columns,
            row_count: r.row_count,
        })
    }
}

#[derive(Debug, Clone)]
pub struct QueryResult {
    pub columns: HashMap<String, Vec<Value>>,
    pub row_count: usize,
}