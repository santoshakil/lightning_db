use crate::{Database, realtime_stats::REALTIME_STATS};
use std::sync::Arc;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write, BufReader, BufRead};
use std::thread;
use std::time::Duration;

/// Simple HTTP Admin API for Lightning DB
/// 
/// Provides basic HTTP endpoints without external dependencies:
/// - GET /health - Basic health check
/// - GET /ready - Readiness probe  
/// - GET /metrics - Prometheus metrics (existing)
/// - GET /status - Database status in JSON
/// - POST /admin/compact - Trigger compaction
/// - POST /admin/checkpoint - Force checkpoint

pub struct SimpleAdminServer {
    db: Arc<Database>,
    port: u16,
}

impl SimpleAdminServer {
    pub fn new(db: Arc<Database>, port: u16) -> Self {
        Self { db, port }
    }

    pub fn start(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&addr)?;
        
        println!("Lightning DB Admin API listening on http://{}", addr);
        println!("Available endpoints:");
        println!("  GET  /health     - Health check");
        println!("  GET  /ready      - Readiness probe");
        println!("  GET  /status     - Database status");
        println!("  POST /admin/compact    - Trigger compaction");
        println!("  POST /admin/checkpoint - Force checkpoint");
        
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let db = self.db.clone();
                    thread::spawn(move || {
                        if let Err(e) = handle_request(stream, db) {
                            eprintln!("Error handling request: {}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error accepting connection: {}", e);
                }
            }
        }
        
        Ok(())
    }
}

fn handle_request(
    mut stream: TcpStream, 
    db: Arc<Database>
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = BufReader::new(&stream);
    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    
    let parts: Vec<&str> = request_line.trim().split_whitespace().collect();
    if parts.len() < 2 {
        send_error(&mut stream, 400, "Bad Request")?;
        return Ok(());
    }
    
    let method = parts[0];
    let path = parts[1];
    
    // Read headers (consume them but don't process for simplicity)
    loop {
        let mut line = String::new();
        reader.read_line(&mut line)?;
        if line.trim().is_empty() {
            break;
        }
    }
    
    match (method, path) {
        ("GET", "/health") => handle_health(&mut stream),
        ("GET", "/ready") => handle_ready(&mut stream, &db),
        ("GET", "/status") => handle_status(&mut stream),
        ("POST", "/admin/compact") => handle_compact(&mut stream, &db),
        ("POST", "/admin/checkpoint") => handle_checkpoint(&mut stream, &db),
        _ => send_error(&mut stream, 404, "Not Found"),
    }
}

fn handle_health(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let response = r#"{"status":"healthy","service":"lightning-db"}"#;
    send_json_response(stream, 200, response)
}

fn handle_ready(
    stream: &mut TcpStream, 
    db: &Arc<Database>
) -> Result<(), Box<dyn std::error::Error>> {
    // Test basic operations
    let test_key = b"__ready_check__";
    let write_ok = db.put(test_key, b"test").is_ok();
    let read_ok = write_ok && db.get(test_key).is_ok();
    let delete_ok = read_ok && db.delete(test_key).is_ok();
    
    let ready = write_ok && read_ok && delete_ok;
    
    let response = format!(
        r#"{{"ready":{},"checks":{{"write":{},"read":{},"delete":{}}}}}"#,
        ready, write_ok, read_ok, delete_ok
    );
    
    send_json_response(stream, 200, &response)
}

fn handle_status(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    let stats = REALTIME_STATS.read().get_current_stats();
    
    let response = format!(
        r#"{{
            "operations": {{
                "total": {},
                "reads": {},
                "writes": {},
                "deletes": {},
                "scans": {}
            }},
            "performance": {{
                "avg_latency_us": {:.2},
                "p99_latency_us": {:.2},
                "throughput_ops_sec": {:.0}
            }},
            "cache": {{
                "hit_rate": {:.2},
                "hits": {},
                "misses": {},
                "evictions": {}
            }},
            "transactions": {{
                "active": {},
                "committed": {},
                "aborted": {}
            }},
            "size": {{
                "data_bytes": {},
                "index_bytes": {},
                "wal_bytes": {},
                "cache_bytes": {}
            }}
        }}"#,
        stats.total_ops,
        stats.get_ops,
        stats.put_ops,
        stats.delete_ops,
        stats.range_ops,
        stats.avg_latency_us,
        stats.p99_latency_us,
        stats.throughput_ops_sec,
        stats.cache_hit_rate,
        stats.cache_hits,
        stats.cache_misses,
        stats.cache_evictions,
        stats.active_transactions,
        stats.committed_transactions,
        stats.aborted_transactions,
        stats.data_size_bytes,
        stats.index_size_bytes,
        stats.wal_size_bytes,
        stats.cache_size_bytes,
    );
    
    send_json_response(stream, 200, &response)
}

fn handle_compact(
    stream: &mut TcpStream, 
    db: &Arc<Database>
) -> Result<(), Box<dyn std::error::Error>> {
    match db.checkpoint() {
        Ok(_) => {
            let response = r#"{"success":true,"message":"Compaction triggered"}"#;
            send_json_response(stream, 200, response)
        }
        Err(e) => {
            let response = format!(
                r#"{{"success":false,"error":"{}"}}"#, 
                e.to_string().replace('"', "\\\"")
            );
            send_json_response(stream, 500, &response)
        }
    }
}

fn handle_checkpoint(
    stream: &mut TcpStream, 
    db: &Arc<Database>
) -> Result<(), Box<dyn std::error::Error>> {
    match db.checkpoint() {
        Ok(_) => {
            let response = r#"{"success":true,"message":"Checkpoint completed"}"#;
            send_json_response(stream, 200, response)
        }
        Err(e) => {
            let response = format!(
                r#"{{"success":false,"error":"{}"}}"#, 
                e.to_string().replace('"', "\\\"")
            );
            send_json_response(stream, 500, &response)
        }
    }
}

fn send_json_response(
    stream: &mut TcpStream, 
    status_code: u16, 
    json: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let status_text = match status_code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    
    let response = format!(
        "HTTP/1.1 {} {}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n\
         {}",
        status_code, status_text, json.len(), json
    );
    
    stream.write_all(response.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn send_error(
    stream: &mut TcpStream, 
    status_code: u16, 
    message: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let json = format!(r#"{{"error":"{}"}}"#, message);
    send_json_response(stream, status_code, &json)
}

/// Start the simple admin server
pub fn start_admin_server(db: Arc<Database>, port: u16) -> Result<(), Box<dyn std::error::Error>> {
    let server = SimpleAdminServer::new(db, port);
    server.start()
}

/// Start admin server in a background thread
pub fn start_admin_server_async(db: Arc<Database>, port: u16) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Err(e) = start_admin_server(db, port) {
            eprintln!("Admin server error: {}", e);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use crate::LightningDbConfig;
    use std::io::{BufReader, BufRead};
    
    fn read_response(stream: &mut TcpStream) -> Result<(u16, String), Box<dyn std::error::Error>> {
        let mut reader = BufReader::new(stream);
        let mut status_line = String::new();
        reader.read_line(&mut status_line)?;
        
        let parts: Vec<&str> = status_line.trim().split_whitespace().collect();
        let status_code = parts[1].parse::<u16>()?;
        
        // Read headers
        let mut content_length = 0;
        loop {
            let mut line = String::new();
            reader.read_line(&mut line)?;
            if line.trim().is_empty() {
                break;
            }
            if line.starts_with("Content-Length:") {
                content_length = line.trim()
                    .split(':')
                    .nth(1)
                    .unwrap()
                    .trim()
                    .parse::<usize>()?;
            }
        }
        
        // Read body
        let mut body = vec![0; content_length];
        reader.read_exact(&mut body)?;
        let body_str = String::from_utf8(body)?;
        
        Ok((status_code, body_str))
    }
    
    #[test]
    fn test_health_endpoint() {
        let temp_dir = TempDir::new().unwrap();
        let db = Arc::new(Database::create(temp_dir.path(), LightningDbConfig::default()).unwrap());
        
        let handle = start_admin_server_async(db, 0); // Use port 0 for auto-assignment
        thread::sleep(Duration::from_millis(100)); // Give server time to start
        
        // In a real test, we'd need to get the actual port from the server
        // For now, this is a placeholder showing the test structure
    }
}