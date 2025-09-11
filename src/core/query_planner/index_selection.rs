use super::cost_model::CostModel;
use super::planner::{CostEstimate, Expression, FilterNode, PlanNode, Predicate, ScanNode};
use super::statistics::{IndexStatistics, IndexType, TableStatistics};
use crate::core::error::Error;
use std::collections::HashMap;
use std::sync::Arc;

const INDEX_SCAN_OVERHEAD: f64 = 1.2;
const COVERING_INDEX_BONUS: f64 = 0.5;
const PARTIAL_INDEX_BONUS: f64 = 0.8;
const CLUSTERED_INDEX_BONUS: f64 = 0.7;

#[derive(Debug, Clone)]
pub struct IndexSelector {
    cost_model: Arc<CostModel>,
    config: IndexSelectorConfig,
    available_indexes: Arc<HashMap<String, Vec<IndexInfo>>>,
}

#[derive(Debug, Clone)]
pub struct IndexSelectorConfig {
    pub enable_bitmap_scan: bool,
    pub enable_index_only_scan: bool,
    pub enable_partial_indexes: bool,
    pub enable_expression_indexes: bool,
    pub enable_multi_column_statistics: bool,
    pub index_scan_threshold: f64,
    pub bitmap_scan_threshold: f64,
}

impl Default for IndexSelectorConfig {
    fn default() -> Self {
        Self {
            enable_bitmap_scan: true,
            enable_index_only_scan: true,
            enable_partial_indexes: true,
            enable_expression_indexes: true,
            enable_multi_column_statistics: true,
            index_scan_threshold: 0.1,
            bitmap_scan_threshold: 0.05,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub unique: bool,
    pub clustered: bool,
    pub partial_predicate: Option<Predicate>,
    pub expression_columns: Vec<Expression>,
    pub statistics: IndexStatistics,
}

#[derive(Debug, Clone)]
pub struct IndexHint {
    pub index_name: String,
    pub hint_type: IndexHintType,
    pub force: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum IndexHintType {
    Use,
    Ignore,
    Force,
}

#[derive(Debug, Clone)]
pub struct IndexAccessPath {
    pub index: IndexInfo,
    pub access_type: IndexAccessType,
    pub predicates: Vec<Predicate>,
    pub cost: CostEstimate,
    pub selectivity: f64,
    pub estimated_rows: usize,
    pub is_covering: bool,
}

#[derive(Debug, Clone, Copy)]
pub enum IndexAccessType {
    FullScan,
    RangeScan,
    UniqueAccess,
    BitmapScan,
    IndexOnlyScan,
    SkipScan,
}

impl IndexSelector {
    pub fn new(cost_model: Arc<CostModel>) -> Self {
        Self {
            cost_model,
            config: IndexSelectorConfig::default(),
            available_indexes: Arc::new(HashMap::new()),
        }
    }

    pub fn select_indexes(
        &self,
        node: Box<PlanNode>,
        stats: &TableStatistics,
        hints: &[IndexHint],
    ) -> Result<Box<PlanNode>, Error> {
        match node.as_ref() {
            PlanNode::Scan(scan) => {
                let access_paths = self.generate_access_paths(scan, stats, hints)?;

                if let Some(best_path) = self.select_best_path(&access_paths, scan, stats)? {
                    self.create_index_scan_node(best_path, scan)
                } else {
                    Ok(node)
                }
            }
            PlanNode::Filter(filter) => {
                if let PlanNode::Scan(scan) = filter.input.as_ref() {
                    let combined_predicates =
                        self.combine_predicates(scan.predicate.as_ref(), &filter.predicate);

                    let modified_scan = ScanNode {
                        predicate: Some(combined_predicates),
                        ..scan.clone()
                    };

                    let access_paths = self.generate_access_paths(&modified_scan, stats, hints)?;

                    if let Some(best_path) =
                        self.select_best_path(&access_paths, &modified_scan, stats)?
                    {
                        self.create_index_scan_node(best_path, &modified_scan)
                    } else {
                        Ok(node)
                    }
                } else {
                    let optimized_input =
                        self.select_indexes(filter.input.clone(), stats, hints)?;
                    Ok(Box::new(PlanNode::Filter(FilterNode {
                        input: optimized_input,
                        ..filter.clone()
                    })))
                }
            }
            _ => Ok(node),
        }
    }

    fn generate_access_paths(
        &self,
        scan: &ScanNode,
        stats: &TableStatistics,
        hints: &[IndexHint],
    ) -> Result<Vec<IndexAccessPath>, Error> {
        let mut paths = Vec::new();

        let indexes = self.get_table_indexes(&scan.table_name)?;

        for index in indexes {
            if self.should_skip_index(&index, hints) {
                continue;
            }

            if let Some(path) = self.create_access_path(index, scan, stats)? {
                paths.push(path);
            }
        }

        if self.config.enable_bitmap_scan {
            paths.extend(self.generate_bitmap_paths(scan, stats)?);
        }

        Ok(paths)
    }

    fn create_access_path(
        &self,
        index: IndexInfo,
        scan: &ScanNode,
        stats: &TableStatistics,
    ) -> Result<Option<IndexAccessPath>, Error> {
        let matching_predicates = self.match_predicates_to_index(&index, &scan.predicate);

        if matching_predicates.is_empty() && !self.is_forced_index(&index) {
            return Ok(None);
        }

        let access_type = self.determine_access_type(&index, &matching_predicates);
        let selectivity = self.estimate_index_selectivity(&index, &matching_predicates, stats)?;
        let estimated_rows = (scan.estimated_rows as f64 * selectivity) as usize;

        let is_covering = self.is_covering_index(&index, &scan.columns);

        let cost =
            self.estimate_index_cost(&index, access_type, estimated_rows, is_covering, stats)?;

        Ok(Some(IndexAccessPath {
            index,
            access_type,
            predicates: matching_predicates,
            cost,
            selectivity,
            estimated_rows,
            is_covering,
        }))
    }

    fn match_predicates_to_index(
        &self,
        index: &IndexInfo,
        predicate: &Option<Predicate>,
    ) -> Vec<Predicate> {
        let mut matched = Vec::new();

        if let Some(pred) = predicate {
            self.match_predicate_recursive(pred, &index.columns, &mut matched);
        }

        matched
    }

    fn match_predicate_recursive(
        &self,
        predicate: &Predicate,
        columns: &[String],
        matched: &mut Vec<Predicate>,
    ) {
        match predicate {
            Predicate::Equals(expr, _) => {
                if self.expression_matches_column(expr, columns) {
                    matched.push(predicate.clone());
                }
            }
            Predicate::GreaterThan(expr, _)
            | Predicate::LessThan(expr, _)
            | Predicate::GreaterThanOrEqual(expr, _)
            | Predicate::LessThanOrEqual(expr, _) => {
                if self.expression_matches_column(expr, columns) {
                    matched.push(predicate.clone());
                }
            }
            Predicate::Between(expr, _, _) => {
                if self.expression_matches_column(expr, columns) {
                    matched.push(predicate.clone());
                }
            }
            Predicate::In(expr, _) => {
                if self.expression_matches_column(expr, columns) {
                    matched.push(predicate.clone());
                }
            }
            Predicate::And(left, right) => {
                self.match_predicate_recursive(left, columns, matched);
                self.match_predicate_recursive(right, columns, matched);
            }
            _ => {}
        }
    }

    fn expression_matches_column(&self, expr: &Expression, columns: &[String]) -> bool {
        match expr {
            Expression::Column(name) => columns.contains(name),
            _ => false,
        }
    }

    fn determine_access_type(
        &self,
        index: &IndexInfo,
        predicates: &[Predicate],
    ) -> IndexAccessType {
        if predicates.is_empty() {
            return IndexAccessType::FullScan;
        }

        let has_equality = predicates
            .iter()
            .any(|p| matches!(p, Predicate::Equals(_, _)));
        let has_range = predicates.iter().any(|p| {
            matches!(
                p,
                Predicate::GreaterThan(_, _)
                    | Predicate::LessThan(_, _)
                    | Predicate::Between(_, _, _)
            )
        });

        if index.unique && has_equality && self.covers_all_key_columns(&index, predicates) {
            IndexAccessType::UniqueAccess
        } else if has_range {
            IndexAccessType::RangeScan
        } else if self.can_use_skip_scan(&index, predicates) {
            IndexAccessType::SkipScan
        } else {
            IndexAccessType::RangeScan
        }
    }

    fn covers_all_key_columns(&self, index: &IndexInfo, predicates: &[Predicate]) -> bool {
        for column in &index.columns {
            let has_predicate = predicates.iter().any(|p| match p {
                Predicate::Equals(Expression::Column(col), _) => col == column,
                _ => false,
            });

            if !has_predicate {
                return false;
            }
        }

        true
    }

    fn can_use_skip_scan(&self, index: &IndexInfo, predicates: &[Predicate]) -> bool {
        if index.columns.len() < 2 {
            return false;
        }

        let first_column_has_predicate = predicates.iter().any(|p| match p {
            Predicate::Equals(Expression::Column(col), _) => col == &index.columns[0],
            _ => false,
        });

        !first_column_has_predicate && !predicates.is_empty()
    }

    fn is_covering_index(&self, index: &IndexInfo, required_columns: &[String]) -> bool {
        if !self.config.enable_index_only_scan {
            return false;
        }

        for column in required_columns {
            if !index.columns.contains(column) {
                return false;
            }
        }

        true
    }

    fn estimate_index_selectivity(
        &self,
        index: &IndexInfo,
        predicates: &[Predicate],
        stats: &TableStatistics,
    ) -> Result<f64, Error> {
        if predicates.is_empty() {
            return Ok(1.0);
        }

        let mut combined_selectivity = 1.0;

        for predicate in predicates {
            let selectivity =
                self.estimate_predicate_selectivity(predicate, &index.table_name, stats)?;
            combined_selectivity *= selectivity;
        }

        if index.unique && self.covers_all_key_columns(index, predicates) {
            combined_selectivity =
                combined_selectivity.min(1.0 / index.statistics.distinct_keys as f64);
        }

        Ok(combined_selectivity)
    }

    #[allow(clippy::only_used_in_recursion)]
    fn estimate_predicate_selectivity(
        &self,
        predicate: &Predicate,
        table_name: &str,
        stats: &TableStatistics,
    ) -> Result<f64, Error> {
        match predicate {
            Predicate::Equals(Expression::Column(col), _) => {
                if let Some(col_stats) = stats.get_column(table_name, col) {
                    Ok(1.0 / col_stats.distinct_count.unwrap_or(1000) as f64)
                } else {
                    Ok(0.1)
                }
            }
            Predicate::GreaterThan(_, _) | Predicate::LessThan(_, _) => Ok(0.3),
            Predicate::Between(_, _, _) => Ok(0.2),
            Predicate::In(_, values) => Ok((values.len() as f64 * 0.1).min(1.0)),
            Predicate::And(left, right) => {
                let left_sel = self.estimate_predicate_selectivity(left, table_name, stats)?;
                let right_sel = self.estimate_predicate_selectivity(right, table_name, stats)?;
                Ok(left_sel * right_sel)
            }
            _ => Ok(0.5),
        }
    }

    fn estimate_index_cost(
        &self,
        index: &IndexInfo,
        access_type: IndexAccessType,
        estimated_rows: usize,
        is_covering: bool,
        _stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        let index_height =
            ((index.statistics.distinct_keys as f64).log2() / 100.0).max(1.0) as usize;

        let (cpu_cost, io_cost) = match access_type {
            IndexAccessType::UniqueAccess => {
                let io = index_height as f64 * 4.0;
                let cpu = 10.0;
                (cpu, io)
            }
            IndexAccessType::RangeScan => {
                let leaf_pages =
                    (estimated_rows as f64 / index.statistics.avg_leaf_entries as f64).max(1.0);
                let io = index_height as f64 * 4.0 + leaf_pages * 1.0;
                let cpu = estimated_rows as f64 * 0.01;
                (cpu, io)
            }
            IndexAccessType::FullScan => {
                let io = index.statistics.leaf_pages as f64 * 1.0;
                let cpu = index.statistics.distinct_keys as f64 * 0.01;
                (cpu, io)
            }
            IndexAccessType::BitmapScan => {
                let io = (estimated_rows as f64 / 1000.0).max(1.0) * 2.0;
                let cpu = estimated_rows as f64 * 0.02;
                (cpu, io)
            }
            IndexAccessType::SkipScan => {
                let skip_factor = 10.0;
                let io = index.statistics.leaf_pages as f64 / skip_factor;
                let cpu = index.statistics.distinct_keys as f64 * 0.02;
                (cpu, io)
            }
            _ => (100.0, 100.0),
        };

        let mut final_io = io_cost;
        if !is_covering {
            final_io += estimated_rows as f64 * 4.0;
        } else {
            final_io *= COVERING_INDEX_BONUS;
        }

        if index.clustered {
            final_io *= CLUSTERED_INDEX_BONUS;
        }

        Ok(CostEstimate::new(cpu_cost, final_io, 0.0, 0.0))
    }

    fn select_best_path(
        &self,
        paths: &[IndexAccessPath],
        scan: &ScanNode,
        stats: &TableStatistics,
    ) -> Result<Option<IndexAccessPath>, Error> {
        if paths.is_empty() {
            return Ok(None);
        }

        let table_scan_cost = self.estimate_table_scan_cost(scan, stats)?;

        let mut best_path = None;
        let mut best_cost = table_scan_cost.total_cost;

        for path in paths {
            let path_cost = path.cost.total_cost;

            if path_cost < best_cost * (1.0 - self.config.index_scan_threshold) {
                best_cost = path_cost;
                best_path = Some(path.clone());
            }
        }

        Ok(best_path)
    }

    fn estimate_table_scan_cost(
        &self,
        scan: &ScanNode,
        stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        if let Some(table_stats) = stats.get_table(&scan.table_name) {
            let io_cost = table_stats.total_pages as f64 * 1.0;
            let cpu_cost = table_stats.row_count as f64 * 0.01;
            Ok(CostEstimate::new(cpu_cost, io_cost, 0.0, 0.0))
        } else {
            Ok(scan.estimated_cost)
        }
    }

    fn create_index_scan_node(
        &self,
        path: IndexAccessPath,
        scan: &ScanNode,
    ) -> Result<Box<PlanNode>, Error> {
        Ok(Box::new(PlanNode::Index(super::planner::IndexNode {
            table_name: scan.table_name.clone(),
            index_name: path.index.name.clone(),
            columns: scan.columns.clone(),
            predicate: scan.predicate.clone(),
            estimated_rows: path.estimated_rows,
            estimated_cost: path.cost,
        })))
    }

    fn generate_bitmap_paths(
        &self,
        scan: &ScanNode,
        stats: &TableStatistics,
    ) -> Result<Vec<IndexAccessPath>, Error> {
        let mut paths = Vec::new();

        if !self.config.enable_bitmap_scan {
            return Ok(paths);
        }

        let indexes = self.get_table_indexes(&scan.table_name)?;

        for combination in self.generate_index_combinations(&indexes, 2) {
            if let Some(path) = self.create_bitmap_path(combination, scan, stats)? {
                paths.push(path);
            }
        }

        Ok(paths)
    }

    fn create_bitmap_path(
        &self,
        _indexes: Vec<IndexInfo>,
        _scan: &ScanNode,
        _stats: &TableStatistics,
    ) -> Result<Option<IndexAccessPath>, Error> {
        Ok(None)
    }

    fn generate_index_combinations(
        &self,
        indexes: &[IndexInfo],
        max_size: usize,
    ) -> Vec<Vec<IndexInfo>> {
        let mut result = Vec::new();

        for size in 1..=max_size.min(indexes.len()) {
            self.generate_combinations_recursive(indexes, size, 0, Vec::new(), &mut result);
        }

        result
    }

    #[allow(clippy::only_used_in_recursion)]
    fn generate_combinations_recursive(
        &self,
        indexes: &[IndexInfo],
        remaining: usize,
        start: usize,
        current: Vec<IndexInfo>,
        result: &mut Vec<Vec<IndexInfo>>,
    ) {
        if remaining == 0 {
            result.push(current);
            return;
        }

        for i in start..indexes.len() {
            let mut new_current = current.clone();
            new_current.push(indexes[i].clone());
            self.generate_combinations_recursive(
                indexes,
                remaining - 1,
                i + 1,
                new_current,
                result,
            );
        }
    }

    fn get_table_indexes(&self, table_name: &str) -> Result<Vec<IndexInfo>, Error> {
        Ok(self
            .available_indexes
            .get(table_name)
            .cloned()
            .unwrap_or_default())
    }

    fn should_skip_index(&self, index: &IndexInfo, hints: &[IndexHint]) -> bool {
        for hint in hints {
            if hint.index_name == index.name {
                return matches!(hint.hint_type, IndexHintType::Ignore);
            }
        }
        false
    }

    fn is_forced_index(&self, _index: &IndexInfo) -> bool {
        false
    }

    fn combine_predicates(&self, pred1: Option<&Predicate>, pred2: &Predicate) -> Predicate {
        if let Some(p1) = pred1 {
            Predicate::And(Box::new(p1.clone()), Box::new(pred2.clone()))
        } else {
            pred2.clone()
        }
    }
}
