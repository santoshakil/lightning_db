use crate::error::Result;

pub struct QueryParser {
    default_field: String,
}

pub struct Query {
    pub clauses: Vec<QueryClause>,
}

pub struct QueryClause {
    pub field: String,
    pub term: QueryTerm,
    pub boost: f32,
}

#[derive(Debug, Clone)]
pub enum QueryTerm {
    Term(String),
    Phrase(Vec<String>),
    Wildcard(String),
    Fuzzy(String, u8),
    Range(Option<String>, Option<String>),
}

impl QueryParser {
    pub fn new(default_field: &str) -> Self {
        Self {
            default_field: default_field.to_string(),
        }
    }
    
    pub fn parse(&self, query_str: &str) -> Result<Query> {
        Ok(Query {
            clauses: vec![QueryClause {
                field: self.default_field.clone(),
                term: QueryTerm::Term(query_str.to_string()),
                boost: 1.0,
            }],
        })
    }
}