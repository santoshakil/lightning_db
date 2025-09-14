use std::sync::Arc;
use crate::{Database, Result, RangeIterator, JoinQuery, JoinResult, TransactionIterator};
use crate::core::index::MultiIndexQuery;
use std::collections::HashMap;
use crate::core::query_planner::{QueryPlan, QuerySpec, ExecutionPlan};

pub struct QueryEngine {
    db: Arc<Database>,
}

impl QueryEngine {
    pub(crate) fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
    
    pub fn scan(
        &self,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Result<RangeIterator> {
        self.db.scan(start_key, end_key)
    }
    
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<RangeIterator> {
        self.db.scan_prefix(prefix)
    }
    
    pub fn scan_reverse(
        &self,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Result<RangeIterator> {
        self.db.scan_reverse(start_key, end_key)
    }
    
    pub fn scan_limit(&self, start_key: Option<Vec<u8>>, limit: usize) -> Result<RangeIterator> {
        self.db.scan_limit(start_key, limit)
    }
    
    pub fn scan_tx(
        &self,
        tx_id: u64,
        start_key: Option<Vec<u8>>,
        end_key: Option<Vec<u8>>,
    ) -> Result<TransactionIterator> {
        self.db.scan_tx(tx_id, start_key, end_key)
    }
    
    pub fn keys(&self) -> Result<impl Iterator<Item = Result<Vec<u8>>>> {
        self.db.keys()
    }
    
    pub fn values(&self) -> Result<impl Iterator<Item = Result<Vec<u8>>>> {
        self.db.values()
    }
    
    pub fn count_range(&self, start_key: Option<Vec<u8>>, end_key: Option<Vec<u8>>) -> Result<usize> {
        self.db.count_range(start_key, end_key)
    }
    
    pub fn range(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.db.range(start_key, end_key)
    }
    
    pub fn btree_range(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.db.btree_range(start_key, end_key)
    }
    
    pub fn join_indexes(&self, join_query: JoinQuery) -> Result<Vec<JoinResult>> {
        self.db.join_indexes(join_query)
    }
    
    pub fn query_multi_index(&self, query: MultiIndexQuery) -> Result<Vec<HashMap<String, Vec<u8>>>> {
        self.db.query_multi_index(query)
    }
    
    pub fn inner_join(
        &self,
        left_index: &str,
        left_key: crate::core::index::IndexKey,
        right_index: &str,
        right_key: crate::core::index::IndexKey,
    ) -> Result<Vec<JoinResult>> {
        self.db.inner_join(left_index, left_key, right_index, right_key)
    }
    
    pub fn analyze_performance(&self) -> Result<()> {
        self.db.analyze_query_performance()
    }
    
    pub fn plan_advanced(&self, _spec: QuerySpec) -> Result<QueryPlan> {
        // Return a basic QueryPlan for now
        use crate::core::query_planner::planner::{PlanNode, CostEstimate, ScanNode, ScanType};
        let cost_estimate = CostEstimate {
            cpu_cost: 0.0,
            io_cost: 0.0,
            network_cost: 0.0,
            memory_cost: 0.0,
            total_cost: 0.0,
        };
        Ok(QueryPlan {
            root: Box::new(PlanNode::Scan(ScanNode {
                table_name: String::new(),
                columns: Vec::new(),
                predicate: None,
                estimated_rows: 0,
                estimated_cost: cost_estimate.clone(),
                scan_type: ScanType::FullTable,
            })),
            estimated_cost: cost_estimate,
            estimated_rows: 0,
            required_memory: 0,
            parallel_degree: 1,
        })
    }
    
    pub fn execute_planned(&self, plan: ExecutionPlan) -> Result<Vec<Vec<u8>>> {
        self.db.execute_planned_query(&plan)
    }
    
    pub fn query_optimized(
        &self,
        conditions: Vec<crate::core::query_planner::QueryCondition>,
        joins: Vec<crate::core::query_planner::QueryJoin>,
    ) -> Result<Vec<Vec<u8>>> {
        self.db.query_optimized(conditions, joins)
    }
    
    pub fn plan_query(&self, conditions: &[(&str, &str, &[u8])]) -> Result<ExecutionPlan> {
        self.db.plan_query(conditions)
    }
}