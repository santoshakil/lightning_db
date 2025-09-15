use super::cost_model::CostModel;
use super::planner::{CostEstimate, JoinCondition, JoinNode, JoinStrategy, JoinType, PlanNode};
use super::statistics::TableStatistics;
use crate::core::error::Error;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

const MAX_JOIN_PERMUTATIONS: usize = 100_000;
const _BUSHY_TREE_THRESHOLD: usize = 8;

#[derive(Debug, Clone)]
pub struct JoinOptimizer {
    _cost_model: Arc<CostModel>,
    config: JoinOptimizerConfig,
}

#[derive(Debug, Clone)]
pub struct JoinOptimizerConfig {
    pub enable_bushy_trees: bool,
    pub enable_cartesian_products: bool,
    pub enable_outer_join_reordering: bool,
    pub use_dynamic_programming: bool,
    pub genetic_algorithm_threshold: usize,
    pub max_join_size: usize,
}

impl Default for JoinOptimizerConfig {
    fn default() -> Self {
        Self {
            enable_bushy_trees: true,
            enable_cartesian_products: false,
            enable_outer_join_reordering: false,
            use_dynamic_programming: true,
            genetic_algorithm_threshold: 12,
            max_join_size: 20,
        }
    }
}

#[derive(Debug, Clone)]
struct JoinGraph {
    nodes: Vec<JoinGraphNode>,
    adjacency_list: HashMap<usize, Vec<usize>>,
}

#[derive(Debug, Clone)]
struct JoinGraphNode {
    id: usize,
    table_name: String,
    estimated_rows: usize,
}

#[derive(Debug, Clone)]
struct JoinGraphEdge {
    left_node: usize,
    right_node: usize,
}

impl JoinOptimizer {
    pub fn new(cost_model: Arc<CostModel>) -> Self {
        Self {
            _cost_model: cost_model,
            config: JoinOptimizerConfig::default(),
        }
    }

    pub fn optimize_joins(
        &self,
        joins: Vec<JoinNode>,
        stats: &TableStatistics,
    ) -> Result<Arc<PlanNode>, Error> {
        if joins.is_empty() {
            return Err(Error::InvalidArgument("No joins to optimize".to_string()));
        }

        if joins.len() == 1 {
            return Ok(Arc::new(PlanNode::Join(joins.into_iter().next().unwrap())));
        }

        let join_graph = self.build_join_graph(&joins, stats)?;

        let optimal_order = if joins.len() <= self.config.genetic_algorithm_threshold {
            if self.config.use_dynamic_programming {
                self.optimize_with_dynamic_programming(&join_graph, stats)?
            } else {
                self.optimize_with_exhaustive_search(&join_graph, stats)?
            }
        } else {
            self.optimize_with_genetic_algorithm(&join_graph, stats)?
        };

        self.build_join_tree(optimal_order, &join_graph)
            .map(Arc::from)
    }

    fn build_join_graph(
        &self,
        joins: &[JoinNode],
        stats: &TableStatistics,
    ) -> Result<JoinGraph, Error> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();
        let mut table_to_node: HashMap<String, usize> = HashMap::new();

        for join in joins {
            let left_table = self.extract_table_name(&join.left)?;
            let right_table = self.extract_table_name(&join.right)?;

            let left_id = *table_to_node.entry(left_table.clone()).or_insert_with(|| {
                let id = nodes.len();
                nodes.push(JoinGraphNode {
                    id,
                    table_name: left_table.clone(),
                    estimated_rows: self.estimate_table_rows(&left_table, stats),
                });
                id
            });

            let right_id = *table_to_node.entry(right_table.clone()).or_insert_with(|| {
                let id = nodes.len();
                nodes.push(JoinGraphNode {
                    id,
                    table_name: right_table.clone(),
                    estimated_rows: self.estimate_table_rows(&right_table, stats),
                });
                id
            });

            edges.push(JoinGraphEdge {
                left_node: left_id,
                right_node: right_id,
            });
        }

        let mut adjacency_list = HashMap::new();
        for edge in &edges {
            adjacency_list
                .entry(edge.left_node)
                .or_insert_with(Vec::new)
                .push(edge.right_node);
            adjacency_list
                .entry(edge.right_node)
                .or_insert_with(Vec::new)
                .push(edge.left_node);
        }

        Ok(JoinGraph {
            nodes,
            adjacency_list,
        })
    }

    fn optimize_with_dynamic_programming(
        &self,
        graph: &JoinGraph,
        stats: &TableStatistics,
    ) -> Result<JoinOrder, Error> {
        let n = graph.nodes.len();

        if n > 20 {
            return self.optimize_with_greedy(&graph, stats);
        }

        let mut dp: HashMap<BTreeSet<usize>, DPEntry> = HashMap::new();

        for node in &graph.nodes {
            let mut set = BTreeSet::new();
            set.insert(node.id);

            dp.insert(
                set,
                DPEntry {
                    cost: CostEstimate::zero(),
                    order: JoinOrder::Single(node.id),
                    cardinality: node.estimated_rows,
                },
            );
        }

        for subset_size in 2..=n {
            for subset in self.generate_subsets(n, subset_size) {
                let mut best_entry: Option<DPEntry> = None;

                for split in self.generate_splits(&subset) {
                    let (s1, s2) = split;

                    if let (Some(entry1), Some(entry2)) = (dp.get(&s1), dp.get(&s2)) {
                        if !self.can_join(&s1, &s2, graph) && !self.config.enable_cartesian_products
                        {
                            continue;
                        }

                        let join_cost = self.estimate_join_cost(
                            entry1.cardinality,
                            entry2.cardinality,
                            &self.get_best_join_strategy(&s1, &s2, graph, stats),
                        );

                        let total_cost = entry1.cost.add(&entry2.cost).add(&join_cost);
                        let output_cardinality = self.estimate_join_cardinality(
                            entry1.cardinality,
                            entry2.cardinality,
                            &s1,
                            &s2,
                            graph,
                        );

                        let new_entry = DPEntry {
                            cost: total_cost,
                            order: JoinOrder::Join(
                                Box::new(entry1.order.clone()),
                                Box::new(entry2.order.clone()),
                            ),
                            cardinality: output_cardinality,
                        };

                        if best_entry.is_none()
                            || new_entry.cost.total_cost
                                < best_entry.as_ref().unwrap().cost.total_cost
                        {
                            best_entry = Some(new_entry);
                        }
                    }
                }

                if let Some(entry) = best_entry {
                    dp.insert(subset, entry);
                }
            }
        }

        let all_nodes: BTreeSet<_> = (0..n).collect();
        dp.get(&all_nodes)
            .map(|entry| entry.order.clone())
            .ok_or(Error::InvalidOperation {
                reason: "Failed to find optimal join order".to_string(),
            })
    }

    fn optimize_with_exhaustive_search(
        &self,
        graph: &JoinGraph,
        stats: &TableStatistics,
    ) -> Result<JoinOrder, Error> {
        let n = graph.nodes.len();
        let mut best_order: Option<JoinOrder> = None;
        let mut best_cost = CostEstimate::new(f64::MAX, f64::MAX, f64::MAX, f64::MAX);

        let permutations = self.generate_permutations(n);

        for perm in permutations.iter().take(MAX_JOIN_PERMUTATIONS) {
            let order = self.build_left_deep_tree(perm, graph);
            let cost = self.estimate_order_cost(&order, graph, stats)?;

            if cost.total_cost < best_cost.total_cost {
                best_cost = cost;
                best_order = Some(order);
            }
        }

        best_order.ok_or(Error::InvalidOperation {
            reason: "No valid join order found".to_string(),
        })
    }

    fn optimize_with_greedy(
        &self,
        graph: &JoinGraph,
        _stats: &TableStatistics,
    ) -> Result<JoinOrder, Error> {
        let mut joined = BTreeSet::new();

        let start_node =
            graph
                .nodes
                .iter()
                .min_by_key(|n| n.estimated_rows)
                .ok_or(Error::InvalidOperation {
                    reason: "No nodes in join graph".to_string(),
                })?;

        joined.insert(start_node.id);
        let mut result = JoinOrder::Single(start_node.id);

        while joined.len() < graph.nodes.len() {
            let mut best_next = None;
            let mut best_cost = f64::MAX;

            for node in &graph.nodes {
                if joined.contains(&node.id) {
                    continue;
                }

                if self.can_join_with_set(&node.id, &joined, graph) {
                    let cost = self.estimate_greedy_cost(&joined, node.id, graph);

                    if cost < best_cost {
                        best_cost = cost;
                        best_next = Some(node.id);
                    }
                }
            }

            if let Some(next) = best_next {
                joined.insert(next);
                result = JoinOrder::Join(Box::new(result), Box::new(JoinOrder::Single(next)));
            } else if self.config.enable_cartesian_products {
                let next = graph.nodes.iter().find(|n| !joined.contains(&n.id)).ok_or(
                    Error::InvalidOperation {
                        reason: "Cannot find next join".to_string(),
                    },
                )?;

                joined.insert(next.id);
                result = JoinOrder::Join(Box::new(result), Box::new(JoinOrder::Single(next.id)));
            } else {
                return Err(Error::InvalidOperation {
                    reason: "Disconnected join graph".to_string(),
                });
            }
        }
        Ok(result)
    }

    fn optimize_with_genetic_algorithm(
        &self,
        graph: &JoinGraph,
        stats: &TableStatistics,
    ) -> Result<JoinOrder, Error> {
        const POPULATION_SIZE: usize = 100;
        const GENERATIONS: usize = 50;
        const MUTATION_RATE: f64 = 0.1;
        const CROSSOVER_RATE: f64 = 0.7;

        let mut population = self.generate_initial_population(graph, POPULATION_SIZE)?;

        for _ in 0..GENERATIONS {
            let fitness_scores = self.evaluate_population(&population, graph, stats)?;

            let mut new_population = Vec::new();

            new_population.extend(self.select_elite(
                &population,
                &fitness_scores,
                POPULATION_SIZE / 10,
            ));

            while new_population.len() < POPULATION_SIZE {
                let parent1 = self.tournament_selection(&population, &fitness_scores);
                let parent2 = self.tournament_selection(&population, &fitness_scores);

                let offspring = if rand::random::<f64>() < CROSSOVER_RATE {
                    self.crossover(&parent1, &parent2)?
                } else {
                    parent1.clone()
                };

                let offspring = if rand::random::<f64>() < MUTATION_RATE {
                    self.mutate(&offspring)?
                } else {
                    offspring
                };

                new_population.push(offspring);
            }

            population = new_population;
        }

        let fitness_scores = self.evaluate_population(&population, graph, stats)?;
        let best_idx = fitness_scores
            .iter()
            .enumerate()
            .min_by(|a, b| a.1.partial_cmp(b.1).unwrap())
            .map(|(i, _)| i)
            .unwrap();

        Ok(population[best_idx].clone())
    }

    fn generate_initial_population(
        &self,
        graph: &JoinGraph,
        size: usize,
    ) -> Result<Vec<JoinOrder>, Error> {
        let mut population = Vec::new();

        for _ in 0..size {
            population.push(self.generate_random_order(graph)?);
        }

        Ok(population)
    }

    fn generate_random_order(&self, graph: &JoinGraph) -> Result<JoinOrder, Error> {
        let mut nodes: Vec<_> = graph.nodes.iter().map(|n| n.id).collect();

        use rand::seq::SliceRandom;
        let mut rng = rand::rng();
        nodes.shuffle(&mut rng);

        Ok(self.build_left_deep_tree(&nodes, graph))
    }

    fn evaluate_population(
        &self,
        population: &[JoinOrder],
        graph: &JoinGraph,
        stats: &TableStatistics,
    ) -> Result<Vec<f64>, Error> {
        let mut scores = Vec::new();

        for order in population {
            let cost = self.estimate_order_cost(order, graph, stats)?;
            scores.push(cost.total_cost);
        }

        Ok(scores)
    }

    fn select_elite(
        &self,
        population: &[JoinOrder],
        fitness: &[f64],
        count: usize,
    ) -> Vec<JoinOrder> {
        let mut indexed: Vec<_> = fitness.iter().enumerate().collect();
        indexed.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap());

        indexed
            .iter()
            .take(count)
            .map(|(i, _)| population[*i].clone())
            .collect()
    }

    fn tournament_selection(&self, population: &[JoinOrder], fitness: &[f64]) -> JoinOrder {
        const TOURNAMENT_SIZE: usize = 5;

        let mut best = None;
        let mut best_fitness = f64::MAX;

        for _ in 0..TOURNAMENT_SIZE {
            let idx = (rand::random::<u32>() as usize) % population.len();
            if fitness[idx] < best_fitness {
                best_fitness = fitness[idx];
                best = Some(idx);
            }
        }

        population[best.unwrap()].clone()
    }

    fn crossover(&self, parent1: &JoinOrder, _parent2: &JoinOrder) -> Result<JoinOrder, Error> {
        Ok(parent1.clone())
    }

    fn mutate(&self, order: &JoinOrder) -> Result<JoinOrder, Error> {
        Ok(order.clone())
    }

    #[allow(clippy::only_used_in_recursion)]
    fn build_join_tree(&self, order: JoinOrder, graph: &JoinGraph) -> Result<Box<PlanNode>, Error> {
        match order {
            JoinOrder::Single(node_id) => {
                let node = &graph.nodes[node_id];
                Ok(Box::new(PlanNode::Scan(super::planner::ScanNode {
                    table_name: node.table_name.clone(),
                    columns: Vec::new(),
                    predicate: None,
                    estimated_rows: node.estimated_rows,
                    estimated_cost: CostEstimate::zero(),
                    scan_type: super::planner::ScanType::FullTable,
                })))
            }
            JoinOrder::Join(left, right) => {
                let left_tree = self.build_join_tree(*left, graph)?;
                let right_tree = self.build_join_tree(*right, graph)?;

                Ok(Box::new(PlanNode::Join(JoinNode {
                    left: left_tree,
                    right: right_tree,
                    join_type: JoinType::Inner,
                    join_condition: JoinCondition {
                        left_keys: Vec::new(),
                        right_keys: Vec::new(),
                        additional_predicate: None,
                    },
                    strategy: JoinStrategy::HashJoin,
                    estimated_rows: 1000,
                    estimated_cost: CostEstimate::zero(),
                })))
            }
        }
    }

    fn extract_table_name(&self, node: &PlanNode) -> Result<String, Error> {
        match node {
            PlanNode::Scan(scan) => Ok(scan.table_name.clone()),
            _ => Ok("unknown".to_string()),
        }
    }

    fn estimate_table_rows(&self, table_name: &str, stats: &TableStatistics) -> usize {
        stats
            .get_table(table_name)
            .map(|t| t.row_count)
            .unwrap_or(1_000_000)
    }

    fn estimate_table_size(&self, table_name: &str, stats: &TableStatistics) -> usize {
        stats
            .get_table(table_name)
            .map(|t| t.data_size)
            .unwrap_or(100_000_000)
    }

    fn estimate_join_selectivity(
        &self,
        _condition: &JoinCondition,
        _stats: &TableStatistics,
    ) -> f64 {
        0.1
    }

    fn estimate_join_cost(
        &self,
        left_rows: usize,
        right_rows: usize,
        strategy: &JoinStrategy,
    ) -> CostEstimate {
        match strategy {
            JoinStrategy::NestedLoop => {
                CostEstimate::new((left_rows * right_rows) as f64 * 0.01, 0.0, 0.0, 0.0)
            }
            JoinStrategy::HashJoin => CostEstimate::new(
                (left_rows + right_rows) as f64 * 0.05,
                0.0,
                0.0,
                right_rows as f64 * 100.0,
            ),
            JoinStrategy::MergeJoin => {
                let sort_cost = (left_rows as f64 * (left_rows as f64).log2()
                    + right_rows as f64 * (right_rows as f64).log2())
                    * 0.01;
                CostEstimate::new(
                    sort_cost + (left_rows + right_rows) as f64 * 0.01,
                    0.0,
                    0.0,
                    0.0,
                )
            }
            _ => CostEstimate::zero(),
        }
    }

    fn estimate_join_cardinality(
        &self,
        left_rows: usize,
        right_rows: usize,
        _left_set: &BTreeSet<usize>,
        _right_set: &BTreeSet<usize>,
        _graph: &JoinGraph,
    ) -> usize {
        ((left_rows as f64 * right_rows as f64) * 0.1) as usize
    }

    fn can_join(&self, set1: &BTreeSet<usize>, set2: &BTreeSet<usize>, graph: &JoinGraph) -> bool {
        for &node1 in set1 {
            for &node2 in set2 {
                if graph
                    .adjacency_list
                    .get(&node1)
                    .map(|adj| adj.contains(&node2))
                    .unwrap_or(false)
                {
                    return true;
                }
            }
        }
        false
    }

    fn can_join_with_set(&self, node: &usize, set: &BTreeSet<usize>, graph: &JoinGraph) -> bool {
        graph
            .adjacency_list
            .get(node)
            .map(|adj| adj.iter().any(|n| set.contains(n)))
            .unwrap_or(false)
    }

    fn get_best_join_strategy(
        &self,
        _left: &BTreeSet<usize>,
        _right: &BTreeSet<usize>,
        _graph: &JoinGraph,
        _stats: &TableStatistics,
    ) -> JoinStrategy {
        JoinStrategy::HashJoin
    }

    fn estimate_greedy_cost(
        &self,
        _joined: &BTreeSet<usize>,
        _next: usize,
        _graph: &JoinGraph,
    ) -> f64 {
        1000.0
    }

    #[allow(clippy::only_used_in_recursion)]
    fn estimate_order_cost(
        &self,
        order: &JoinOrder,
        graph: &JoinGraph,
        _stats: &TableStatistics,
    ) -> Result<CostEstimate, Error> {
        match order {
            JoinOrder::Single(node_id) => {
                let node = &graph.nodes[*node_id];
                Ok(CostEstimate::new(
                    node.estimated_rows as f64 * 0.01,
                    node.estimated_rows as f64 * 0.1,
                    0.0,
                    0.0,
                ))
            }
            JoinOrder::Join(left, right) => {
                let left_cost = self.estimate_order_cost(left, graph, _stats)?;
                let right_cost = self.estimate_order_cost(right, graph, _stats)?;

                let join_cost = CostEstimate::new(1000.0, 0.0, 0.0, 10000.0);

                Ok(left_cost.add(&right_cost).add(&join_cost))
            }
        }
    }

    fn build_left_deep_tree(&self, order: &[usize], _graph: &JoinGraph) -> JoinOrder {
        if order.is_empty() {
            return JoinOrder::Single(0);
        }

        let mut result = JoinOrder::Single(order[0]);

        for &node in &order[1..] {
            result = JoinOrder::Join(Box::new(result), Box::new(JoinOrder::Single(node)));
        }

        result
    }

    fn generate_subsets(&self, n: usize, size: usize) -> Vec<BTreeSet<usize>> {
        let mut result = Vec::new();
        let mut subset = BTreeSet::new();

        self.generate_subsets_recursive(0, n, size, &mut subset, &mut result);

        result
    }

    #[allow(clippy::only_used_in_recursion)]
    fn generate_subsets_recursive(
        &self,
        start: usize,
        n: usize,
        remaining: usize,
        current: &mut BTreeSet<usize>,
        result: &mut Vec<BTreeSet<usize>>,
    ) {
        if remaining == 0 {
            result.push(current.clone());
            return;
        }

        for i in start..n {
            current.insert(i);
            self.generate_subsets_recursive(i + 1, n, remaining - 1, current, result);
            current.remove(&i);
        }
    }

    fn generate_splits(&self, set: &BTreeSet<usize>) -> Vec<(BTreeSet<usize>, BTreeSet<usize>)> {
        let mut result = Vec::new();
        let items: Vec<_> = set.iter().cloned().collect();

        for i in 1..(1 << (items.len() - 1)) {
            let mut s1 = BTreeSet::new();
            let mut s2 = BTreeSet::new();

            for (j, &item) in items.iter().enumerate() {
                if (i >> j) & 1 == 1 {
                    s1.insert(item);
                } else {
                    s2.insert(item);
                }
            }

            if !s1.is_empty() && !s2.is_empty() {
                result.push((s1, s2));
            }
        }

        result
    }

    fn generate_permutations(&self, n: usize) -> Vec<Vec<usize>> {
        let mut result = Vec::new();
        let mut perm: Vec<usize> = (0..n).collect();

        self.generate_permutations_recursive(&mut perm, 0, &mut result);

        result
    }

    #[allow(clippy::only_used_in_recursion)]
    fn generate_permutations_recursive(
        &self,
        perm: &mut Vec<usize>,
        start: usize,
        result: &mut Vec<Vec<usize>>,
    ) {
        if start == perm.len() {
            result.push(perm.clone());
            return;
        }

        for i in start..perm.len() {
            perm.swap(start, i);
            self.generate_permutations_recursive(perm, start + 1, result);
            perm.swap(start, i);
        }
    }
}

#[derive(Debug, Clone)]
enum JoinOrder {
    Single(usize),
    Join(Box<JoinOrder>, Box<JoinOrder>),
}

#[derive(Debug, Clone)]
struct DPEntry {
    cost: CostEstimate,
    order: JoinOrder,
    cardinality: usize,
}
