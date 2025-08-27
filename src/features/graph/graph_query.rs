use std::sync::Arc;
use crate::error::Result;

pub struct GraphQuery {
    query_text: String,
    parameters: Vec<QueryParameter>,
}

pub struct QueryBuilder {
    steps: Vec<QueryStep>,
}

pub struct QueryResult {
    pub rows: Vec<ResultRow>,
    pub columns: Vec<String>,
}

pub struct ResultRow {
    pub values: Vec<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct QueryParameter {
    pub name: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone)]
pub enum QueryStep {
    Match(String),
    Where(String),
    Return(Vec<String>),
    Create(String),
    Delete(String),
    Set(String),
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }
    
    pub fn match_pattern(mut self, pattern: &str) -> Self {
        self.steps.push(QueryStep::Match(pattern.to_string()));
        self
    }
    
    pub fn where_clause(mut self, condition: &str) -> Self {
        self.steps.push(QueryStep::Where(condition.to_string()));
        self
    }
    
    pub fn return_fields(mut self, fields: Vec<String>) -> Self {
        self.steps.push(QueryStep::Return(fields));
        self
    }
    
    pub fn build(self) -> GraphQuery {
        GraphQuery {
            query_text: String::new(),
            parameters: Vec::new(),
        }
    }
}