use std::collections::HashMap;
use crate::core::error::Error;
use super::planner::{PlanNode, CostEstimate, ScanType, JoinStrategy};
use super::statistics::{TableStatistics, ColumnStatistics};

const CPU_TUPLE_COST: f64 = 0.01;
const SEQ_PAGE_COST: f64 = 1.0;
const RANDOM_PAGE_COST: f64 = 4.0;
const NETWORK_TRANSFER_COST: f64 = 10.0;
const MEMORY_ALLOCATION_COST: f64 = 0.001;
const INDEX_SCAN_COST: f64 = 2.0;
const HASH_BUILD_COST: f64 = 0.05;
const SORT_COST_FACTOR: f64 = 0.02;

#[derive(Debug, Clone)]
pub struct CostModel {
    config: CostModelConfig,
    calibration_data: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct CostModelConfig {
    pub cpu_tuple_cost: f64,
    pub seq_page_cost: f64,
    pub random_page_cost: f64,
    pub network_cost: f64,
    pub memory_cost: f64,
    pub parallel_setup_cost: f64,
    pub parallel_tuple_cost: f64,
}

impl Default for CostModelConfig {
    fn default() -> Self {
        Self {
            cpu_tuple_cost: CPU_TUPLE_COST,
            seq_page_cost: SEQ_PAGE_COST,
            random_page_cost: RANDOM_PAGE_COST,
            network_cost: NETWORK_TRANSFER_COST,
            memory_cost: MEMORY_ALLOCATION_COST,
            parallel_setup_cost: 100.0,
            parallel_tuple_cost: 0.005,
        }
    }
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            config: CostModelConfig::default(),
            calibration_data: HashMap::new(),
        }
    }
}

impl CostModel {
    pub fn new(config: CostModelConfig) -> Self {
        Self {
            config,
            calibration_data: HashMap::new(),
        }
    }

    pub fn estimate_cost(&self, node: &PlanNode, stats: &TableStatistics) -> Result<CostEstimate, Error> {
        match node {
            PlanNode::Scan(scan) => self.estimate_scan_cost(scan, stats),
            PlanNode::Filter(filter) => self.estimate_filter_cost(filter, stats),
            PlanNode::Join(join) => self.estimate_join_cost(join, stats),
            PlanNode::Aggregate(agg) => self.estimate_aggregate_cost(agg, stats),
            PlanNode::Sort(sort) => self.estimate_sort_cost(sort, stats),
            PlanNode::Index(index) => self.estimate_index_cost(index, stats),
            PlanNode::HashAggregate(hash_agg) => self.estimate_hash_aggregate_cost(hash_agg, stats),
            _ => Ok(CostEstimate::zero()),
        }
    }

    fn estimate_scan_cost(
        &self,
        scan: &super::planner::ScanNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let table_stats = stats.get_table(&scan.table_name)
            .ok_or(Error::InvalidInput(format!("No stats for table {}", scan.table_name)))?;
        
        let (io_cost, cpu_cost) = match scan.scan_type {
            ScanType::FullTable => {
                let pages = table_stats.total_pages;
                let tuples = table_stats.row_count;
                (
                    pages as f64 * self.config.seq_page_cost,
                    tuples as f64 * self.config.cpu_tuple_cost,
                )
            },
            ScanType::Index => {
                let index_pages = table_stats.index_pages.unwrap_or(100);
                let tuples = scan.estimated_rows;
                (
                    index_pages as f64 * INDEX_SCAN_COST,
                    tuples as f64 * self.config.cpu_tuple_cost,
                )
            },
            ScanType::Range => {
                let selectivity = scan.estimated_rows as f64 / table_stats.row_count as f64;
                let pages = (table_stats.total_pages as f64 * selectivity).max(1.0);
                (
                    pages * self.config.random_page_cost,
                    scan.estimated_rows as f64 * self.config.cpu_tuple_cost,
                )
            },
            ScanType::Bitmap => {
                let pages = (scan.estimated_rows as f64 / 100.0).max(1.0);
                (
                    pages * self.config.random_page_cost * 0.5,
                    scan.estimated_rows as f64 * self.config.cpu_tuple_cost,
                )
            },
        };

        Ok(CostEstimate::new(cpu_cost, io_cost, 0.0, 0.0))
    }

    fn estimate_filter_cost(
        &self,
        filter: &super::planner::FilterNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let input_cost = self.estimate_cost(&filter.input, stats)?;
        
        let filter_cpu = filter.estimated_rows as f64 * self.config.cpu_tuple_cost * 2.0;
        
        Ok(input_cost.add(&CostEstimate::new(filter_cpu, 0.0, 0.0, 0.0)))
    }

    fn estimate_join_cost(
        &self,
        join: &super::planner::JoinNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let left_cost = self.estimate_cost(&join.left, stats)?;
        let right_cost = self.estimate_cost(&join.right, stats)?;
        
        let left_rows = self.estimate_rows(&join.left, stats)?;
        let right_rows = self.estimate_rows(&join.right, stats)?;
        
        let join_cost = match join.strategy {
            JoinStrategy::NestedLoop => {
                let cpu = left_rows as f64 * right_rows as f64 * self.config.cpu_tuple_cost;
                CostEstimate::new(cpu, 0.0, 0.0, 0.0)
            },
            JoinStrategy::HashJoin => {
                let build_cost = right_rows as f64 * HASH_BUILD_COST;
                let probe_cost = left_rows as f64 * self.config.cpu_tuple_cost * 2.0;
                let memory = right_rows as f64 * 100.0 * self.config.memory_cost;
                CostEstimate::new(build_cost + probe_cost, 0.0, 0.0, memory)
            },
            JoinStrategy::MergeJoin => {
                let sort_left = left_rows as f64 * (left_rows as f64).log2() * SORT_COST_FACTOR;
                let sort_right = right_rows as f64 * (right_rows as f64).log2() * SORT_COST_FACTOR;
                let merge = (left_rows + right_rows) as f64 * self.config.cpu_tuple_cost;
                CostEstimate::new(sort_left + sort_right + merge, 0.0, 0.0, 0.0)
            },
            JoinStrategy::IndexJoin => {
                let index_lookups = left_rows as f64;
                let cpu = index_lookups * INDEX_SCAN_COST * self.config.cpu_tuple_cost;
                let io = index_lookups * self.config.random_page_cost;
                CostEstimate::new(cpu, io, 0.0, 0.0)
            },
            JoinStrategy::BroadcastJoin => {
                let broadcast = right_rows as f64 * self.config.network_cost;
                let join_cpu = left_rows as f64 * self.config.cpu_tuple_cost * 3.0;
                CostEstimate::new(join_cpu, 0.0, broadcast, 0.0)
            },
        };
        
        Ok(left_cost.add(&right_cost).add(&join_cost))
    }

    fn estimate_aggregate_cost(
        &self,
        agg: &super::planner::AggregateNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let input_cost = self.estimate_cost(&agg.input, stats)?;
        let input_rows = self.estimate_rows(&agg.input, stats)?;
        
        let agg_cpu = input_rows as f64 * self.config.cpu_tuple_cost * 
                      (agg.aggregates.len() as f64 + 1.0);
        
        let memory = agg.estimated_rows as f64 * 100.0 * self.config.memory_cost;
        
        Ok(input_cost.add(&CostEstimate::new(agg_cpu, 0.0, 0.0, memory)))
    }

    fn estimate_sort_cost(
        &self,
        sort: &super::planner::SortNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let input_cost = self.estimate_cost(&sort.input, stats)?;
        let rows = sort.estimated_rows;
        
        let sort_cpu = rows as f64 * (rows as f64).log2() * SORT_COST_FACTOR;
        let memory = rows as f64 * 100.0 * self.config.memory_cost;
        
        let spill_io = if rows > 1000000 {
            rows as f64 * 0.01
        } else {
            0.0
        };
        
        Ok(input_cost.add(&CostEstimate::new(sort_cpu, spill_io, 0.0, memory)))
    }

    fn estimate_index_cost(
        &self,
        index: &super::planner::IndexNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let table_stats = stats.get_table(&index.table_name)
            .ok_or(Error::InvalidInput(format!("No stats for table {}", index.table_name)))?;
        
        let index_height = ((table_stats.row_count as f64).log2() / 100.0).max(1.0) as usize;
        let io_cost = index_height as f64 * self.config.random_page_cost;
        let cpu_cost = index.estimated_rows as f64 * self.config.cpu_tuple_cost;
        
        Ok(CostEstimate::new(cpu_cost, io_cost, 0.0, 0.0))
    }

    fn estimate_hash_aggregate_cost(
        &self,
        hash_agg: &super::planner::HashAggregateNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let input_cost = self.estimate_cost(&hash_agg.input, stats)?;
        let input_rows = self.estimate_rows(&hash_agg.input, stats)?;
        
        let hash_build = input_rows as f64 * HASH_BUILD_COST;
        let agg_compute = input_rows as f64 * self.config.cpu_tuple_cost * 
                         hash_agg.aggregates.len() as f64;
        
        let memory = hash_agg.estimated_groups as f64 * 200.0 * self.config.memory_cost;
        
        Ok(input_cost.add(&CostEstimate::new(hash_build + agg_compute, 0.0, 0.0, memory)))
    }

    fn estimate_rows(&self, node: &PlanNode, stats: &TableStatistics) -> Result<usize, Error> {
        match node {
            PlanNode::Scan(scan) => Ok(scan.estimated_rows),
            PlanNode::Filter(filter) => Ok(filter.estimated_rows),
            PlanNode::Join(join) => Ok(join.estimated_rows),
            PlanNode::Aggregate(agg) => Ok(agg.estimated_rows),
            PlanNode::Sort(sort) => Ok(sort.estimated_rows),
            PlanNode::Limit(limit) => Ok(limit.limit),
            _ => Ok(1000),
        }
    }

    pub fn calibrate(&mut self, operation: &str, actual_time: f64, estimated_cost: f64) {
        let ratio = actual_time / estimated_cost;
        self.calibration_data.insert(operation.to_string(), ratio);
        
        if self.calibration_data.len() > 100 {
            self.apply_calibration();
        }
    }

    fn apply_calibration(&mut self) {
        if let Some(&cpu_ratio) = self.calibration_data.get("cpu") {
            self.config.cpu_tuple_cost *= cpu_ratio;
        }
        
        if let Some(&io_ratio) = self.calibration_data.get("io") {
            self.config.seq_page_cost *= io_ratio;
            self.config.random_page_cost *= io_ratio;
        }
        
        self.calibration_data.clear();
    }

    pub fn adjust_for_parallelism(&self, cost: CostEstimate, parallel_degree: usize) -> CostEstimate {
        if parallel_degree <= 1 {
            return cost;
        }
        
        let speedup = 1.0 / (parallel_degree as f64).sqrt();
        let parallel_overhead = self.config.parallel_setup_cost + 
                               self.config.parallel_tuple_cost * parallel_degree as f64;
        
        CostEstimate::new(
            cost.cpu_cost * speedup + parallel_overhead,
            cost.io_cost,
            cost.network_cost + parallel_overhead,
            cost.memory_cost * parallel_degree as f64,
        )
    }
}