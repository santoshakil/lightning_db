use std::sync::Arc;

pub trait Scorer: Send + Sync {
    fn score(&self, term_freq: f32, doc_freq: f32, doc_length: f32) -> f32;
}

pub struct BM25Scorer {
    k1: f32,
    b: f32,
    avg_doc_length: f32,
}

pub struct TFIDFScorer {
    total_docs: usize,
}

impl BM25Scorer {
    pub fn new(k1: f32, b: f32) -> Arc<dyn Scorer> {
        Arc::new(Self {
            k1,
            b,
            avg_doc_length: 100.0,
        })
    }
}

impl Scorer for BM25Scorer {
    fn score(&self, term_freq: f32, doc_freq: f32, doc_length: f32) -> f32 {
        let idf = ((self.avg_doc_length - doc_freq + 0.5) / (doc_freq + 0.5)).ln();
        let tf = (term_freq * (self.k1 + 1.0)) / 
                 (term_freq + self.k1 * (1.0 - self.b + self.b * doc_length / self.avg_doc_length));
        idf * tf
    }
}

impl Scorer for TFIDFScorer {
    fn score(&self, term_freq: f32, doc_freq: f32, _doc_length: f32) -> f32 {
        let tf = term_freq.sqrt();
        let idf = (self.total_docs as f32 / doc_freq).ln();
        tf * idf
    }
}