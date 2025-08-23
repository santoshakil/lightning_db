use lightning_db::features::query_optimizer::{
    QueryOptimizer, QueryPlan, QueryNode, QueryType, JoinType,
    CostModel, TableStatistics, ColumnStatistics
};
use std::collections::HashMap;

#[test]
fn test_query_optimizer_basic_select() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let query = QueryNode::Select {
        table: "users".to_string(),
        columns: vec!["id".to_string(), "name".to_string()],
        predicate: Some(Box::new(QueryNode::Predicate {
            column: "age".to_string(),
            operator: ">=".to_string(),
            value: "18".to_string(),
        })),
    };
    
    let plan = optimizer.optimize(query).unwrap();
    
    assert!(matches!(plan.root.query_type(), QueryType::IndexScan | QueryType::TableScan));
    assert!(plan.estimated_cost > 0.0);
}

#[test]
fn test_query_optimizer_join_reordering() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let mut small_table_stats = TableStatistics::default();
    small_table_stats.row_count = 100;
    small_table_stats.total_size_bytes = 10_000;
    
    let mut large_table_stats = TableStatistics::default();
    large_table_stats.row_count = 1_000_000;
    large_table_stats.total_size_bytes = 100_000_000;
    
    optimizer.update_statistics("small_table".to_string(), small_table_stats);
    optimizer.update_statistics("large_table".to_string(), large_table_stats);
    
    let query = QueryNode::Join {
        join_type: JoinType::Inner,
        left: Box::new(QueryNode::TableScan {
            table: "large_table".to_string(),
            columns: vec!["*".to_string()],
        }),
        right: Box::new(QueryNode::TableScan {
            table: "small_table".to_string(),
            columns: vec!["*".to_string()],
        }),
        condition: Box::new(QueryNode::Predicate {
            column: "id".to_string(),
            operator: "=".to_string(),
            value: "user_id".to_string(),
        }),
    };
    
    let plan = optimizer.optimize(query).unwrap();
    
    assert!(plan.estimated_cost > 0.0);
    assert!(plan.execution_strategy.contains("HashJoin") || 
            plan.execution_strategy.contains("NestedLoop"));
}

#[test]
fn test_query_optimizer_predicate_pushdown() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let query = QueryNode::Select {
        table: "orders".to_string(),
        columns: vec!["*".to_string()],
        predicate: Some(Box::new(QueryNode::Join {
            join_type: JoinType::Inner,
            left: Box::new(QueryNode::TableScan {
                table: "orders".to_string(),
                columns: vec!["*".to_string()],
            }),
            right: Box::new(QueryNode::TableScan {
                table: "customers".to_string(),
                columns: vec!["*".to_string()],
            }),
            condition: Box::new(QueryNode::Predicate {
                column: "customer_id".to_string(),
                operator: "=".to_string(),
                value: "id".to_string(),
            }),
        })),
    };
    
    let plan = optimizer.optimize(query).unwrap();
    
    assert!(plan.optimizations_applied.contains(&"PredicatePushdown".to_string()));
}

#[test]
fn test_query_optimizer_index_selection() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let mut table_stats = TableStatistics::default();
    table_stats.row_count = 10_000;
    table_stats.indexes.insert("idx_email".to_string(), vec!["email".to_string()]);
    table_stats.indexes.insert("idx_created_at".to_string(), vec!["created_at".to_string()]);
    
    optimizer.update_statistics("users".to_string(), table_stats);
    
    let query = QueryNode::Select {
        table: "users".to_string(),
        columns: vec!["*".to_string()],
        predicate: Some(Box::new(QueryNode::Predicate {
            column: "email".to_string(),
            operator: "=".to_string(),
            value: "user@example.com".to_string(),
        })),
    };
    
    let plan = optimizer.optimize(query).unwrap();
    
    assert_eq!(plan.root.query_type(), QueryType::IndexScan);
    assert!(plan.execution_strategy.contains("idx_email"));
}

#[test]
fn test_query_optimizer_cost_estimation() {
    let cost_model = CostModel {
        seq_page_cost: 1.0,
        random_page_cost: 4.0,
        cpu_tuple_cost: 0.01,
        cpu_index_tuple_cost: 0.005,
        cpu_operator_cost: 0.0025,
    };
    
    let mut optimizer = QueryOptimizer::new(cost_model);
    
    let mut table_stats = TableStatistics::default();
    table_stats.row_count = 1000;
    table_stats.avg_row_size = 100;
    table_stats.total_pages = 100;
    
    optimizer.update_statistics("products".to_string(), table_stats);
    
    let scan_query = QueryNode::TableScan {
        table: "products".to_string(),
        columns: vec!["*".to_string()],
    };
    
    let scan_plan = optimizer.optimize(scan_query).unwrap();
    
    let expected_cost = 100.0 + (1000.0 * 0.01);
    assert!((scan_plan.estimated_cost - expected_cost).abs() < 10.0);
}

#[test]
fn test_query_optimizer_aggregate_optimization() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let query = QueryNode::Aggregate {
        input: Box::new(QueryNode::TableScan {
            table: "sales".to_string(),
            columns: vec!["amount".to_string()],
        }),
        group_by: vec!["product_id".to_string()],
        aggregates: vec![
            ("SUM".to_string(), "amount".to_string()),
            ("COUNT".to_string(), "*".to_string()),
        ],
    };
    
    let plan = optimizer.optimize(query).unwrap();
    
    assert!(matches!(plan.root.query_type(), QueryType::HashAggregate | QueryType::SortAggregate));
    assert!(plan.estimated_cost > 0.0);
}

#[test]
fn test_query_optimizer_cache_hit() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let query = QueryNode::Select {
        table: "users".to_string(),
        columns: vec!["id".to_string()],
        predicate: None,
    };
    
    let plan1 = optimizer.optimize(query.clone()).unwrap();
    let plan2 = optimizer.optimize(query).unwrap();
    
    assert_eq!(plan1.estimated_cost, plan2.estimated_cost);
    
    let cache_stats = optimizer.get_cache_stats();
    assert_eq!(cache_stats.hits, 1);
    assert_eq!(cache_stats.misses, 1);
}

#[test]
fn test_query_optimizer_adaptive_learning() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    optimizer.enable_adaptive_optimization(true);
    
    let query = QueryNode::Select {
        table: "orders".to_string(),
        columns: vec!["*".to_string()],
        predicate: Some(Box::new(QueryNode::Predicate {
            column: "status".to_string(),
            operator: "=".to_string(),
            value: "pending".to_string(),
        })),
    };
    
    let plan = optimizer.optimize(query.clone()).unwrap();
    let initial_cost = plan.estimated_cost;
    
    optimizer.update_execution_feedback(plan.clone(), 0.5, 100);
    
    let improved_plan = optimizer.optimize(query).unwrap();
    
    assert!(improved_plan.estimated_cost < initial_cost * 0.8);
}

#[test]
fn test_query_optimizer_parallel_execution() {
    let mut optimizer = QueryOptimizer::new(CostModel::default());
    
    let mut table_stats = TableStatistics::default();
    table_stats.row_count = 10_000_000;
    table_stats.total_pages = 100_000;
    
    optimizer.update_statistics("large_table".to_string(), table_stats);
    
    let query = QueryNode::TableScan {
        table: "large_table".to_string(),
        columns: vec!["*".to_string()],
    };
    
    let plan = optimizer.optimize(query).unwrap();
    
    assert!(plan.parallel_degree > 1);
    assert!(plan.execution_strategy.contains("Parallel"));
}