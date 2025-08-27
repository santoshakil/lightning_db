use std::sync::Arc;
use std::collections::{HashMap, BTreeMap};
use dashmap::DashMap;

pub struct InvertedIndex {
    term_dictionary: Arc<TermDictionary>,
    posting_lists: Arc<DashMap<String, PostingList>>,
    doc_count: Arc<std::sync::atomic::AtomicU64>,
}

pub struct TermDictionary {
    terms: BTreeMap<String, TermInfo>,
}

pub struct TermInfo {
    term: String,
    doc_freq: u64,
    total_freq: u64,
}

pub struct PostingList {
    term: String,
    postings: Vec<Posting>,
}

pub struct Posting {
    doc_id: u64,
    positions: Vec<u32>,
    term_freq: u32,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            term_dictionary: Arc::new(TermDictionary {
                terms: BTreeMap::new(),
            }),
            posting_lists: Arc::new(DashMap::new()),
            doc_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
    
    pub fn add_term(&self, term: &str, doc_id: u64, position: u32) {
        self.posting_lists
            .entry(term.to_string())
            .or_insert_with(|| PostingList {
                term: term.to_string(),
                postings: Vec::new(),
            })
            .postings
            .push(Posting {
                doc_id,
                positions: vec![position],
                term_freq: 1,
            });
    }
    
    pub fn get_postings(&self, term: &str) -> Option<PostingList> {
        self.posting_lists.get(term).map(|p| p.clone())
    }
}