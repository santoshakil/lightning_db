//! Health Check HTTP Server Example
//!
//! This example demonstrates how to set up health check endpoints
//! for Lightning DB in a production environment.

use lightning_db::{Database, LightningDbConfig, health_check::{HealthCheckSystem, HealthCheckConfig}};
use std::sync::Arc;
use warp::{Filter, Reply};
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Create database
    let config = LightningDbConfig {
        cache_size: 100 * 1024 * 1024, // 100MB
        enable_statistics: true,
        ..Default::default()
    };
    
    let db = Arc::new(Database::create_or_open("./health_check_demo.db", config)?);
    
    // Create health check system
    let health_config = HealthCheckConfig {
        detailed: true,
        interval: Duration::from_secs(30),
        timeout: Duration::from_secs(5),
        ..Default::default()
    };
    
    let health_system = Arc::new(HealthCheckSystem::new(db.clone(), health_config));
    
    // Start health check background task
    health_system.start().await?;
    
    // Set up HTTP routes
    let health_system_filter = warp::any().map(move || health_system.clone());
    
    // Liveness probe - simple check if service is alive
    let liveness = warp::path!("healthz")
        .and(warp::get())
        .and(health_system_filter.clone())
        .and_then(liveness_handler);
    
    // Readiness probe - check if service is ready to handle requests
    let readiness = warp::path!("ready")
        .and(warp::get())
        .and(health_system_filter.clone())
        .and_then(readiness_handler);
    
    // Detailed health check - full health status with metrics
    let health = warp::path!("health")
        .and(warp::get())
        .and(warp::query::<HealthQuery>())
        .and(health_system_filter.clone())
        .and_then(health_handler);
    
    // Metrics endpoint for Prometheus
    let metrics = warp::path!("metrics")
        .and(warp::get())
        .and(health_system_filter.clone())
        .and_then(metrics_handler);
    
    // Database operations for testing
    let db_filter = warp::any().map(move || db.clone());
    
    let put = warp::path!("api" / "put" / String / String)
        .and(warp::post())
        .and(db_filter.clone())
        .and_then(put_handler);
    
    let get = warp::path!("api" / "get" / String)
        .and(warp::get())
        .and(db_filter.clone())
        .and_then(get_handler);
    
    // Combine all routes
    let routes = liveness
        .or(readiness)
        .or(health)
        .or(metrics)
        .or(put)
        .or(get)
        .with(warp::log("health_check_server"));
    
    println!("Lightning DB Health Check Server starting on http://0.0.0.0:8080");
    println!("Endpoints:");
    println!("  - Liveness:  GET http://localhost:8080/healthz");
    println!("  - Readiness: GET http://localhost:8080/ready");
    println!("  - Health:    GET http://localhost:8080/health?detailed=true");
    println!("  - Metrics:   GET http://localhost:8080/metrics");
    println!("  - Put:       POST http://localhost:8080/api/put/{key}/{value}");
    println!("  - Get:       GET http://localhost:8080/api/get/{key}");
    
    // Start server
    warp::serve(routes)
        .run(([0, 0, 0, 0], 8080))
        .await;
    
    Ok(())
}

#[derive(serde::Deserialize)]
struct HealthQuery {
    #[serde(default)]
    detailed: bool,
}

// Handler functions
async fn liveness_handler(
    health_system: Arc<HealthCheckSystem>,
) -> Result<impl Reply, warp::Rejection> {
    let (status, body) = lightning_db::health_check::http::liveness_handler(&health_system).await;
    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::from_str::<serde_json::Value>(&body).unwrap()),
        warp::http::StatusCode::from_u16(status).unwrap(),
    ))
}

async fn readiness_handler(
    health_system: Arc<HealthCheckSystem>,
) -> Result<impl Reply, warp::Rejection> {
    let (status, body) = lightning_db::health_check::http::readiness_handler(&health_system).await;
    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::from_str::<serde_json::Value>(&body).unwrap()),
        warp::http::StatusCode::from_u16(status).unwrap(),
    ))
}

async fn health_handler(
    query: HealthQuery,
    health_system: Arc<HealthCheckSystem>,
) -> Result<impl Reply, warp::Rejection> {
    let (status, body) = lightning_db::health_check::http::health_handler(
        &health_system,
        query.detailed,
    ).await;
    Ok(warp::reply::with_status(
        warp::reply::json(&serde_json::from_str::<serde_json::Value>(&body).unwrap()),
        warp::http::StatusCode::from_u16(status).unwrap(),
    ))
}

async fn metrics_handler(
    health_system: Arc<HealthCheckSystem>,
) -> Result<impl Reply, warp::Rejection> {
    // Get latest health check result
    match health_system.get_latest_result().await {
        Some(result) => {
            // Format as Prometheus metrics
            let mut output = String::new();
            
            // Health status
            output.push_str(&format!(
                "# HELP lightning_db_health_status Overall health status (0=healthy, 1=degraded, 2=unhealthy, 3=critical)\n"
            ));
            output.push_str(&format!(
                "# TYPE lightning_db_health_status gauge\n"
            ));
            output.push_str(&format!(
                "lightning_db_health_status {}\n",
                match result.status {
                    lightning_db::health_check::HealthStatus::Healthy => 0,
                    lightning_db::health_check::HealthStatus::Degraded => 1,
                    lightning_db::health_check::HealthStatus::Unhealthy => 2,
                    lightning_db::health_check::HealthStatus::Critical => 3,
                }
            ));
            
            // Metrics
            output.push_str(&format!(
                "# HELP lightning_db_ops_per_second Operations per second\n"
            ));
            output.push_str(&format!(
                "# TYPE lightning_db_ops_per_second gauge\n"
            ));
            output.push_str(&format!(
                "lightning_db_ops_per_second {}\n",
                result.metrics.ops_per_second
            ));
            
            output.push_str(&format!(
                "# HELP lightning_db_p99_latency_microseconds P99 latency in microseconds\n"
            ));
            output.push_str(&format!(
                "# TYPE lightning_db_p99_latency_microseconds gauge\n"
            ));
            output.push_str(&format!(
                "lightning_db_p99_latency_microseconds {}\n",
                result.metrics.p99_latency_us
            ));
            
            output.push_str(&format!(
                "# HELP lightning_db_cache_hit_rate Cache hit rate (0-1)\n"
            ));
            output.push_str(&format!(
                "# TYPE lightning_db_cache_hit_rate gauge\n"
            ));
            output.push_str(&format!(
                "lightning_db_cache_hit_rate {}\n",
                result.metrics.cache_hit_rate
            ));
            
            output.push_str(&format!(
                "# HELP lightning_db_memory_usage_bytes Memory usage in bytes\n"
            ));
            output.push_str(&format!(
                "# TYPE lightning_db_memory_usage_bytes gauge\n"
            ));
            output.push_str(&format!(
                "lightning_db_memory_usage_bytes {}\n",
                result.metrics.memory_usage_bytes
            ));
            
            Ok(warp::reply::with_header(
                output,
                "Content-Type",
                "text/plain; version=0.0.4",
            ))
        }
        None => {
            Ok(warp::reply::with_header(
                "# No metrics available yet\n",
                "Content-Type",
                "text/plain; version=0.0.4",
            ))
        }
    }
}

async fn put_handler(
    key: String,
    value: String,
    db: Arc<Database>,
) -> Result<impl Reply, warp::Rejection> {
    match db.put(key.as_bytes(), value.as_bytes()) {
        Ok(_) => Ok(warp::reply::json(&serde_json::json!({
            "status": "success",
            "key": key,
        }))),
        Err(e) => Ok(warp::reply::json(&serde_json::json!({
            "status": "error",
            "message": e.to_string(),
        }))),
    }
}

async fn get_handler(
    key: String,
    db: Arc<Database>,
) -> Result<impl Reply, warp::Rejection> {
    match db.get(key.as_bytes()) {
        Ok(Some(value)) => Ok(warp::reply::json(&serde_json::json!({
            "status": "success",
            "key": key,
            "value": String::from_utf8_lossy(&value),
        }))),
        Ok(None) => Ok(warp::reply::json(&serde_json::json!({
            "status": "not_found",
            "key": key,
        }))),
        Err(e) => Ok(warp::reply::json(&serde_json::json!({
            "status": "error",
            "message": e.to_string(),
        }))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_health_check_server() {
        // This would test the health check endpoints
        // For brevity, not implementing full tests here
    }
}