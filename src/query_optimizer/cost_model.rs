//! Cost Model for Query Optimization
//!
//! This module implements a comprehensive cost model that estimates the execution cost
//! of different query operations. It considers CPU, I/O, memory, and network costs.

use super::{
    ColumnStatistics, IndexStatistics, JoinType, OptimizerConfig, PhysicalOperator, TableStatistics,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Cost model for estimating query execution costs
#[derive(Debug)]
pub struct CostModel {
    /// Configuration parameters
    config: OptimizerConfig,
    /// Hardware characteristics
    hardware: HardwareProfile,
    /// Cached cost calculations
    cost_cache: HashMap<String, f64>,
}

/// Hardware profile for cost calculations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareProfile {
    /// CPU characteristics
    pub cpu: CpuProfile,
    /// Memory characteristics
    pub memory: MemoryProfile,
    /// Storage characteristics
    pub storage: StorageProfile,
    /// Network characteristics
    pub network: NetworkProfile,
}

/// CPU profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuProfile {
    /// CPU cores available
    pub cores: usize,
    /// CPU frequency in MHz
    pub frequency_mhz: u32,
    /// Instructions per cycle
    pub instructions_per_cycle: f64,
    /// Cache sizes (L1, L2, L3) in KB
    pub cache_sizes: Vec<u32>,
    /// SIMD width (number of elements processed per instruction)
    pub simd_width: usize,
}

/// Memory profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProfile {
    /// Total memory in MB
    pub total_mb: u32,
    /// Memory bandwidth in MB/s
    pub bandwidth_mb_per_sec: u32,
    /// Memory latency in nanoseconds
    pub latency_ns: u32,
    /// Page size in bytes
    pub page_size: u32,
}

/// Storage profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageProfile {
    /// Storage type
    pub storage_type: StorageType,
    /// Sequential read throughput in MB/s
    pub sequential_read_mb_per_sec: u32,
    /// Random read IOPS
    pub random_read_iops: u32,
    /// Sequential write throughput in MB/s
    pub sequential_write_mb_per_sec: u32,
    /// Random write IOPS
    pub random_write_iops: u32,
    /// Average seek time in milliseconds (for HDDs)
    pub avg_seek_time_ms: f64,
}

/// Storage types
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum StorageType {
    SSD,
    NVMe,
    HDD,
    Memory,
}

/// Network profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkProfile {
    /// Bandwidth in Mbps
    pub bandwidth_mbps: u32,
    /// Latency in milliseconds
    pub latency_ms: f64,
    /// Packet loss rate
    pub packet_loss_rate: f64,
}

/// Cost breakdown for an operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationCost {
    /// CPU cost
    pub cpu_cost: f64,
    /// I/O cost
    pub io_cost: f64,
    /// Memory cost
    pub memory_cost: f64,
    /// Network cost
    pub network_cost: f64,
    /// Total cost
    pub total_cost: f64,
    /// Estimated execution time in microseconds
    pub execution_time_us: u64,
    /// Memory requirement in bytes
    pub memory_required: u64,
}

impl CostModel {
    /// Create new cost model
    pub fn new(config: OptimizerConfig) -> Self {
        Self {
            config,
            hardware: HardwareProfile::default(),
            cost_cache: HashMap::new(),
        }
    }

    /// Calculate cost for a physical operator
    pub fn calculate_cost(
        &self,
        operator: &PhysicalOperator,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        match operator {
            PhysicalOperator::TableScan {
                table,
                predicate,
                columns,
            } => self.cost_table_scan(table, predicate.as_ref(), columns, table_stats),
            PhysicalOperator::IndexScan {
                table,
                index,
                key_conditions,
                predicate,
            } => self.cost_index_scan(
                table,
                index,
                key_conditions,
                predicate.as_ref(),
                table_stats,
                index_stats,
            ),
            PhysicalOperator::NestedLoopJoin {
                left,
                right,
                condition,
                join_type,
            } => self.cost_nested_loop_join(
                left,
                right,
                condition,
                *join_type,
                table_stats,
                column_stats,
                index_stats,
            ),
            PhysicalOperator::HashJoin {
                left,
                right,
                condition,
                join_type,
                build_side,
            } => self.cost_hash_join(
                left,
                right,
                condition,
                *join_type,
                *build_side,
                table_stats,
                column_stats,
                index_stats,
            ),
            PhysicalOperator::SortMergeJoin {
                left,
                right,
                condition,
                join_type,
            } => self.cost_sort_merge_join(
                left,
                right,
                condition,
                *join_type,
                table_stats,
                column_stats,
                index_stats,
            ),
            PhysicalOperator::Aggregation {
                input,
                group_by,
                aggregates,
                vectorized,
            } => self.cost_aggregation(
                input,
                group_by,
                aggregates,
                *vectorized,
                table_stats,
                column_stats,
                index_stats,
            ),
            PhysicalOperator::Sort {
                input,
                sort_keys,
                limit,
            } => self.cost_sort(
                input,
                sort_keys,
                *limit,
                table_stats,
                column_stats,
                index_stats,
            ),
            PhysicalOperator::Projection { input, columns } => {
                self.cost_projection(input, columns, table_stats, column_stats, index_stats)
            }
            PhysicalOperator::Filter {
                input,
                predicate,
                vectorized,
            } => self.cost_filter(
                input,
                predicate,
                *vectorized,
                table_stats,
                column_stats,
                index_stats,
            ),
            PhysicalOperator::Limit {
                input,
                count,
                offset,
            } => self.cost_limit(
                input,
                *count,
                *offset,
                table_stats,
                column_stats,
                index_stats,
            ),
        }
    }

    /// Cost table scan operation
    fn cost_table_scan(
        &self,
        table: &str,
        predicate: Option<&super::Predicate>,
        columns: &[String],
        table_stats: &HashMap<String, TableStatistics>,
    ) -> Result<OperationCost> {
        let stats = table_stats
            .get(table)
            .ok_or_else(|| Error::Generic(format!("No statistics for table {}", table)))?;

        // I/O cost: read all pages
        let pages_to_read = stats.page_count;
        let io_cost = self.calculate_sequential_read_cost(pages_to_read);

        // CPU cost: process all rows
        let rows_to_process = stats.row_count;
        let cpu_cost = self.calculate_cpu_cost_per_row() * rows_to_process as f64;

        // Apply selectivity if predicate exists
        let selectivity = if let Some(pred) = predicate {
            self.estimate_predicate_selectivity(pred, table, table_stats)
        } else {
            1.0
        };

        // Adjust costs based on selectivity
        let effective_cpu_cost = cpu_cost * selectivity;

        // Memory cost: buffer for processing
        let memory_required = (stats.avg_row_size * rows_to_process as f64) as u64;
        let memory_cost = self.calculate_memory_cost(memory_required);

        // Projection cost reduction
        let projection_factor = columns.len() as f64 / 10.0; // Assume 10 columns on average
        let adjusted_cpu_cost = effective_cpu_cost * projection_factor.min(1.0);

        let total_cost = adjusted_cpu_cost * self.config.cpu_cost_factor
            + io_cost * self.config.io_cost_factor
            + memory_cost * self.config.memory_cost_factor;

        let execution_time_us =
            self.estimate_execution_time(adjusted_cpu_cost, io_cost, memory_cost, 0.0);

        Ok(OperationCost {
            cpu_cost: adjusted_cpu_cost,
            io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required,
        })
    }

    /// Cost index scan operation
    fn cost_index_scan(
        &self,
        table: &str,
        index: &str,
        key_conditions: &[super::KeyCondition],
        predicate: Option<&super::Predicate>,
        table_stats: &HashMap<String, TableStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let table_stat = table_stats
            .get(table)
            .ok_or_else(|| Error::Generic(format!("No statistics for table {}", table)))?;

        let index_stat = index_stats
            .get(index)
            .ok_or_else(|| Error::Generic(format!("No statistics for index {}", index)))?;

        // Estimate selectivity from key conditions
        let selectivity = self.estimate_key_conditions_selectivity(key_conditions);

        // Index I/O cost
        let index_pages = (index_stat.page_count as f64 * selectivity) as u64;
        let index_io_cost = self.calculate_random_read_cost(index_pages);

        // Table I/O cost (for rows fetched)
        let rows_fetched = (table_stat.row_count as f64 * selectivity) as u64;
        let table_pages = (rows_fetched as f64 * table_stat.avg_row_size / 4096.0) as u64;
        let table_io_cost = self.calculate_random_read_cost(table_pages);

        // CPU cost for index lookup and row processing
        let cpu_cost = self.calculate_cpu_cost_per_row() * rows_fetched as f64 * 0.5; // Index lookup is faster

        // Memory cost
        let memory_required = rows_fetched * table_stat.avg_row_size as u64;
        let memory_cost = self.calculate_memory_cost(memory_required);

        let total_cost = cpu_cost * self.config.cpu_cost_factor
            + (index_io_cost + table_io_cost) * self.config.io_cost_factor
            + memory_cost * self.config.memory_cost_factor;

        let execution_time_us =
            self.estimate_execution_time(cpu_cost, index_io_cost + table_io_cost, memory_cost, 0.0);

        Ok(OperationCost {
            cpu_cost,
            io_cost: index_io_cost + table_io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required,
        })
    }

    /// Cost nested loop join
    fn cost_nested_loop_join(
        &self,
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        _condition: &super::JoinCondition,
        join_type: JoinType,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let left_cost = self.calculate_cost(left, table_stats, column_stats, index_stats)?;
        let right_cost = self.calculate_cost(right, table_stats, column_stats, index_stats)?;

        // Estimate cardinalities
        let left_cardinality = self.estimate_operator_cardinality(left, table_stats);
        let right_cardinality = self.estimate_operator_cardinality(right, table_stats);

        // Nested loop: for each row in left, scan right
        let join_cpu_cost =
            left_cardinality as f64 * right_cardinality as f64 * self.calculate_cpu_cost_per_row();

        // I/O cost: left read once, right read for each left row
        let join_io_cost = left_cost.io_cost + (left_cardinality as f64 * right_cost.io_cost);

        // Memory cost: keep left relation in memory
        let memory_required = left_cost.memory_required + 1024 * 1024; // 1MB buffer
        let memory_cost = self.calculate_memory_cost(memory_required);

        // Join type factor
        let join_factor = match join_type {
            JoinType::Inner => 1.0,
            JoinType::Left => 1.2,
            JoinType::Right => 1.2,
            JoinType::Full => 1.5,
            JoinType::Semi => 0.8,
            JoinType::Anti => 0.8,
        };

        let total_cost = (left_cost.total_cost
            + join_cpu_cost * self.config.cpu_cost_factor
            + join_io_cost * self.config.io_cost_factor
            + memory_cost * self.config.memory_cost_factor)
            * join_factor;

        let execution_time_us = self.estimate_execution_time(
            left_cost.cpu_cost + join_cpu_cost,
            join_io_cost,
            memory_cost,
            0.0,
        );

        Ok(OperationCost {
            cpu_cost: left_cost.cpu_cost + join_cpu_cost,
            io_cost: join_io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required,
        })
    }

    /// Cost hash join
    fn cost_hash_join(
        &self,
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        _condition: &super::JoinCondition,
        join_type: JoinType,
        build_side: super::BuildSide,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let left_cost = self.calculate_cost(left, table_stats, column_stats, index_stats)?;
        let right_cost = self.calculate_cost(right, table_stats, column_stats, index_stats)?;

        let left_cardinality = self.estimate_operator_cardinality(left, table_stats);
        let right_cardinality = self.estimate_operator_cardinality(right, table_stats);

        // Determine build and probe sides
        let (build_cardinality, probe_cardinality, build_cost, probe_cost) = match build_side {
            super::BuildSide::Left => (left_cardinality, right_cardinality, left_cost, right_cost),
            super::BuildSide::Right => (right_cardinality, left_cardinality, right_cost, left_cost),
        };

        // Hash table construction cost
        let build_cpu_cost = build_cardinality as f64 * self.calculate_cpu_cost_per_row() * 1.5; // Hashing overhead

        // Probe cost
        let probe_cpu_cost = probe_cardinality as f64 * self.calculate_cpu_cost_per_row() * 1.2; // Hash lookup

        // Memory cost: hash table size
        let hash_table_size = build_cardinality as u64 * 64; // Assume 64 bytes per entry
        let memory_required = build_cost.memory_required + hash_table_size;
        let memory_cost = self.calculate_memory_cost(memory_required);

        // I/O cost: read both sides once
        let io_cost = build_cost.io_cost + probe_cost.io_cost;

        let join_factor = match join_type {
            JoinType::Inner => 1.0,
            JoinType::Left => 1.1,
            JoinType::Right => 1.1,
            JoinType::Full => 1.3,
            JoinType::Semi => 0.9,
            JoinType::Anti => 0.9,
        };

        let total_cost = (build_cpu_cost + probe_cpu_cost) * self.config.cpu_cost_factor
            + io_cost * self.config.io_cost_factor
            + memory_cost * self.config.memory_cost_factor * join_factor;

        let execution_time_us = self.estimate_execution_time(
            build_cpu_cost + probe_cpu_cost,
            io_cost,
            memory_cost,
            0.0,
        );

        Ok(OperationCost {
            cpu_cost: build_cpu_cost + probe_cpu_cost,
            io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required,
        })
    }

    /// Cost sort-merge join
    fn cost_sort_merge_join(
        &self,
        left: &PhysicalOperator,
        right: &PhysicalOperator,
        _condition: &super::JoinCondition,
        join_type: JoinType,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let left_cost = self.calculate_cost(left, table_stats, column_stats, index_stats)?;
        let right_cost = self.calculate_cost(right, table_stats, column_stats, index_stats)?;

        let left_cardinality = self.estimate_operator_cardinality(left, table_stats);
        let right_cardinality = self.estimate_operator_cardinality(right, table_stats);

        // Sort costs (if not already sorted)
        let left_sort_cost = self.calculate_sort_cost(left_cardinality);
        let right_sort_cost = self.calculate_sort_cost(right_cardinality);

        // Merge cost
        let merge_cpu_cost =
            (left_cardinality + right_cardinality) as f64 * self.calculate_cpu_cost_per_row();

        // Memory cost for sorting
        let sort_memory = (left_cardinality + right_cardinality) as u64 * 64;
        let memory_cost = self.calculate_memory_cost(sort_memory);

        let join_factor = match join_type {
            JoinType::Inner => 1.0,
            JoinType::Left => 1.1,
            JoinType::Right => 1.1,
            JoinType::Full => 1.2,
            JoinType::Semi => 0.9,
            JoinType::Anti => 0.9,
        };

        let total_cost = (left_cost.total_cost
            + right_cost.total_cost
            + (left_sort_cost + right_sort_cost + merge_cpu_cost) * self.config.cpu_cost_factor
            + memory_cost * self.config.memory_cost_factor)
            * join_factor;

        let execution_time_us = self.estimate_execution_time(
            left_cost.cpu_cost
                + right_cost.cpu_cost
                + left_sort_cost
                + right_sort_cost
                + merge_cpu_cost,
            left_cost.io_cost + right_cost.io_cost,
            memory_cost,
            0.0,
        );

        Ok(OperationCost {
            cpu_cost: left_cost.cpu_cost
                + right_cost.cpu_cost
                + left_sort_cost
                + right_sort_cost
                + merge_cpu_cost,
            io_cost: left_cost.io_cost + right_cost.io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required: sort_memory,
        })
    }

    /// Cost aggregation operation
    fn cost_aggregation(
        &self,
        input: &PhysicalOperator,
        group_by: &[String],
        aggregates: &[super::AggregateFunction],
        vectorized: bool,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let input_cost = self.calculate_cost(input, table_stats, column_stats, index_stats)?;
        let input_cardinality = self.estimate_operator_cardinality(input, table_stats);

        // Aggregation CPU cost
        let agg_factor = if vectorized { 0.3 } else { 1.0 }; // Vectorization speedup
        let cpu_cost = input_cardinality as f64
            * aggregates.len() as f64
            * self.calculate_cpu_cost_per_row()
            * agg_factor;

        // Hash table for grouping
        let estimated_groups = if group_by.is_empty() {
            1
        } else {
            (input_cardinality as f64 * 0.1) as u64 // Estimate 10% unique groups
        };

        let hash_table_size = estimated_groups * 128; // 128 bytes per group
        let memory_cost = self.calculate_memory_cost(hash_table_size);

        let total_cost = input_cost.total_cost
            + cpu_cost * self.config.cpu_cost_factor
            + memory_cost * self.config.memory_cost_factor;

        let execution_time_us = self.estimate_execution_time(
            input_cost.cpu_cost + cpu_cost,
            input_cost.io_cost,
            memory_cost,
            0.0,
        );

        Ok(OperationCost {
            cpu_cost: input_cost.cpu_cost + cpu_cost,
            io_cost: input_cost.io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required: hash_table_size,
        })
    }

    /// Cost sort operation
    fn cost_sort(
        &self,
        input: &PhysicalOperator,
        _sort_keys: &[super::SortKey],
        limit: Option<usize>,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let input_cost = self.calculate_cost(input, table_stats, column_stats, index_stats)?;
        let input_cardinality = self.estimate_operator_cardinality(input, table_stats);

        let effective_cardinality = if let Some(lim) = limit {
            input_cardinality.min(lim as u64)
        } else {
            input_cardinality
        };

        let sort_cpu_cost = self.calculate_sort_cost(effective_cardinality);
        let memory_cost = self.calculate_memory_cost(effective_cardinality * 64);

        let total_cost = input_cost.total_cost
            + sort_cpu_cost * self.config.cpu_cost_factor
            + memory_cost * self.config.memory_cost_factor;

        let execution_time_us = self.estimate_execution_time(
            input_cost.cpu_cost + sort_cpu_cost,
            input_cost.io_cost,
            memory_cost,
            0.0,
        );

        Ok(OperationCost {
            cpu_cost: input_cost.cpu_cost + sort_cpu_cost,
            io_cost: input_cost.io_cost,
            memory_cost,
            network_cost: 0.0,
            total_cost,
            execution_time_us,
            memory_required: effective_cardinality * 64,
        })
    }

    /// Cost projection operation
    fn cost_projection(
        &self,
        input: &PhysicalOperator,
        columns: &[String],
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let input_cost = self.calculate_cost(input, table_stats, column_stats, index_stats)?;

        // Projection is typically very cheap
        let projection_factor = columns.len() as f64 / 10.0; // Assume 10 columns baseline
        let cpu_cost = input_cost.cpu_cost * 0.1 * projection_factor;

        Ok(OperationCost {
            cpu_cost: input_cost.cpu_cost + cpu_cost,
            io_cost: input_cost.io_cost,
            memory_cost: input_cost.memory_cost,
            network_cost: input_cost.network_cost,
            total_cost: input_cost.total_cost + cpu_cost * self.config.cpu_cost_factor,
            execution_time_us: input_cost.execution_time_us,
            memory_required: input_cost.memory_required,
        })
    }

    /// Cost filter operation
    fn cost_filter(
        &self,
        input: &PhysicalOperator,
        _predicate: &super::Predicate,
        vectorized: bool,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let input_cost = self.calculate_cost(input, table_stats, column_stats, index_stats)?;
        let input_cardinality = self.estimate_operator_cardinality(input, table_stats);

        let filter_factor = if vectorized { 0.2 } else { 1.0 }; // Vectorization speedup
        let cpu_cost =
            input_cardinality as f64 * self.calculate_cpu_cost_per_row() * 0.5 * filter_factor;

        Ok(OperationCost {
            cpu_cost: input_cost.cpu_cost + cpu_cost,
            io_cost: input_cost.io_cost,
            memory_cost: input_cost.memory_cost,
            network_cost: input_cost.network_cost,
            total_cost: input_cost.total_cost + cpu_cost * self.config.cpu_cost_factor,
            execution_time_us: input_cost.execution_time_us,
            memory_required: input_cost.memory_required,
        })
    }

    /// Cost limit operation
    fn cost_limit(
        &self,
        input: &PhysicalOperator,
        _count: usize,
        _offset: usize,
        table_stats: &HashMap<String, TableStatistics>,
        column_stats: &HashMap<String, ColumnStatistics>,
        index_stats: &HashMap<String, IndexStatistics>,
    ) -> Result<OperationCost> {
        let input_cost = self.calculate_cost(input, table_stats, column_stats, index_stats)?;

        // Limit is essentially free - just stops processing early
        Ok(input_cost)
    }

    /// Helper methods for cost calculations

    fn calculate_sequential_read_cost(&self, pages: u64) -> f64 {
        let mb_to_read = (pages * 4) as f64 / 1024.0; // 4KB pages
        mb_to_read / self.hardware.storage.sequential_read_mb_per_sec as f64
    }

    fn calculate_random_read_cost(&self, pages: u64) -> f64 {
        match self.hardware.storage.storage_type {
            StorageType::HDD => pages as f64 * self.hardware.storage.avg_seek_time_ms,
            _ => pages as f64 / self.hardware.storage.random_read_iops as f64,
        }
    }

    fn calculate_cpu_cost_per_row(&self) -> f64 {
        1.0 / (self.hardware.cpu.frequency_mhz as f64 * 1000.0) // Very simplified
    }

    fn calculate_memory_cost(&self, bytes: u64) -> f64 {
        bytes as f64 / (1024.0 * 1024.0) // Cost per MB
    }

    fn calculate_sort_cost(&self, cardinality: u64) -> f64 {
        if cardinality <= 1 {
            return 0.0;
        }

        // N log N complexity
        cardinality as f64 * (cardinality as f64).log2() * self.calculate_cpu_cost_per_row()
    }

    fn estimate_operator_cardinality(
        &self,
        operator: &PhysicalOperator,
        table_stats: &HashMap<String, TableStatistics>,
    ) -> u64 {
        match operator {
            PhysicalOperator::TableScan {
                table, predicate, ..
            } => {
                if let Some(stats) = table_stats.get(table) {
                    let selectivity = if let Some(_pred) = predicate {
                        0.1 // Default selectivity
                    } else {
                        1.0
                    };
                    (stats.row_count as f64 * selectivity) as u64
                } else {
                    1000 // Default estimate
                }
            }
            _ => 1000, // Default estimate for other operators
        }
    }

    fn estimate_predicate_selectivity(
        &self,
        _predicate: &super::Predicate,
        _table: &str,
        _table_stats: &HashMap<String, TableStatistics>,
    ) -> f64 {
        // Simplified selectivity estimation
        0.1 // Assume 10% selectivity by default
    }

    fn estimate_key_conditions_selectivity(&self, _conditions: &[super::KeyCondition]) -> f64 {
        // Simplified selectivity for index key conditions
        0.01 // Assume 1% selectivity for index lookups
    }

    fn estimate_execution_time(
        &self,
        cpu_cost: f64,
        io_cost: f64,
        memory_cost: f64,
        network_cost: f64,
    ) -> u64 {
        let total_time = cpu_cost + io_cost * 10.0 + memory_cost * 0.1 + network_cost * 100.0;
        (total_time * 1_000_000.0) as u64 // Convert to microseconds
    }
}

impl Default for HardwareProfile {
    fn default() -> Self {
        Self {
            cpu: CpuProfile {
                cores: 8,
                frequency_mhz: 3000,
                instructions_per_cycle: 2.0,
                cache_sizes: vec![32, 256, 8192], // L1, L2, L3 in KB
                simd_width: 8,
            },
            memory: MemoryProfile {
                total_mb: 8192,
                bandwidth_mb_per_sec: 25600,
                latency_ns: 100,
                page_size: 4096,
            },
            storage: StorageProfile {
                storage_type: StorageType::SSD,
                sequential_read_mb_per_sec: 500,
                random_read_iops: 50000,
                sequential_write_mb_per_sec: 450,
                random_write_iops: 40000,
                avg_seek_time_ms: 0.1, // SSD
            },
            network: NetworkProfile {
                bandwidth_mbps: 1000,
                latency_ms: 0.5,
                packet_loss_rate: 0.001,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cost_model_creation() {
        let config = OptimizerConfig::default();
        let cost_model = CostModel::new(config);
        assert_eq!(cost_model.hardware.cpu.cores, 8);
    }

    #[test]
    fn test_sequential_read_cost() {
        let config = OptimizerConfig::default();
        let cost_model = CostModel::new(config);

        let cost = cost_model.calculate_sequential_read_cost(1000); // 1000 pages = 4MB
        assert!(cost > 0.0);
    }

    #[test]
    fn test_sort_cost() {
        let config = OptimizerConfig::default();
        let cost_model = CostModel::new(config);

        let cost_small = cost_model.calculate_sort_cost(100);
        let cost_large = cost_model.calculate_sort_cost(10000);

        assert!(cost_large > cost_small);
    }

    #[test]
    fn test_hardware_profile_default() {
        let profile = HardwareProfile::default();
        assert!(matches!(profile.storage.storage_type, StorageType::SSD));
        assert_eq!(profile.cpu.cores, 8);
        assert_eq!(profile.memory.total_mb, 8192);
    }
}
