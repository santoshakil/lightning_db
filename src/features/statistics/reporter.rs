use super::{MetricsCollector, MetricsSnapshot, WindowMetrics};
use std::fmt;
use std::io::Write;
use std::sync::Arc;

/// Formats and reports metrics in various formats
pub struct MetricsReporter {
    collector: Arc<MetricsCollector>,
}

impl MetricsReporter {
    pub fn new(collector: Arc<MetricsCollector>) -> Self {
        Self { collector }
    }

    /// Generate a text report of current metrics
    pub fn text_report(&self) -> String {
        let snapshot = self.collector.get_current_snapshot();
        let mut report = String::new();

        report.push_str(&format!("{}", TextReport(&snapshot)));

        // Add window metrics if available
        if let Some(window_1m) = self
            .collector
            .get_window_metrics(std::time::Duration::from_secs(60))
        {
            report.push_str("\n\n## 1-Minute Window Metrics\n");
            report.push_str(&format!("{}", WindowTextReport(&window_1m)));
        }

        if let Some(window_5m) = self
            .collector
            .get_window_metrics(std::time::Duration::from_secs(300))
        {
            report.push_str("\n\n## 5-Minute Window Metrics\n");
            report.push_str(&format!("{}", WindowTextReport(&window_5m)));
        }

        report
    }

    /// Generate a JSON report of current metrics
    pub fn json_report(&self) -> serde_json::Value {
        let snapshot = self.collector.get_current_snapshot();

        serde_json::json!({
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            "operations": {
                "reads": snapshot.reads,
                "writes": snapshot.writes,
                "deletes": snapshot.deletes,
                "transactions": snapshot.transactions
            },
            "latencies_us": {
                "avg_read": snapshot.avg_read_latency_us,
                "avg_write": snapshot.avg_write_latency_us,
                "avg_delete": snapshot.avg_delete_latency_us,
                "avg_transaction": snapshot.avg_transaction_latency_us
            },
            "cache": {
                "hits": snapshot.cache_hits,
                "misses": snapshot.cache_misses,
                "hit_rate": snapshot.cache_hit_rate,
                "evictions": snapshot.cache_evictions
            },
            "pages": {
                "read": snapshot.pages_read,
                "written": snapshot.pages_written,
                "faults": snapshot.page_faults
            },
            "compaction": {
                "count": snapshot.compactions,
                "bytes_read": snapshot.compaction_bytes_read,
                "bytes_written": snapshot.compaction_bytes_written
            },
            "errors": {
                "read": snapshot.read_errors,
                "write": snapshot.write_errors,
                "transaction_aborts": snapshot.transaction_aborts
            },
            "size": {
                "database_bytes": snapshot.database_size_bytes,
                "wal_bytes": snapshot.wal_size_bytes
            },
            "active_transactions": snapshot.active_transactions
        })
    }

    /// Write metrics to a writer in CSV format
    pub fn csv_report<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let snapshot = self.collector.get_current_snapshot();

        // Write header if needed
        writeln!(writer, "timestamp,reads,writes,deletes,transactions,avg_read_latency_us,avg_write_latency_us,cache_hit_rate,pages_read,pages_written,compactions,errors,database_size_bytes")?;

        // Write data row
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{:.2},{},{},{},{},{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            snapshot.reads,
            snapshot.writes,
            snapshot.deletes,
            snapshot.transactions,
            snapshot.avg_read_latency_us,
            snapshot.avg_write_latency_us,
            snapshot.cache_hit_rate,
            snapshot.pages_read,
            snapshot.pages_written,
            snapshot.compactions,
            snapshot.read_errors + snapshot.write_errors,
            snapshot.database_size_bytes
        )?;

        Ok(())
    }

    /// Generate a summary report with key metrics
    pub fn summary_report(&self) -> MetricsSummary {
        let snapshot = self.collector.get_current_snapshot();
        let window_1m = self
            .collector
            .get_window_metrics(std::time::Duration::from_secs(60));

        MetricsSummary {
            total_operations: snapshot.reads + snapshot.writes + snapshot.deletes,
            operations_per_sec: window_1m
                .as_ref()
                .map(|w| w.reads_per_sec + w.writes_per_sec)
                .unwrap_or(0.0),
            cache_hit_rate: snapshot.cache_hit_rate,
            avg_latency_us: (snapshot.avg_read_latency_us + snapshot.avg_write_latency_us) / 2,
            error_rate: window_1m.as_ref().map(|w| w.error_rate).unwrap_or(0.0),
            database_size_mb: (snapshot.database_size_bytes as f64) / (1024.0 * 1024.0),
            active_transactions: snapshot.active_transactions,
        }
    }
}

/// Summary of key metrics
#[derive(Debug, Clone)]
pub struct MetricsSummary {
    pub total_operations: u64,
    pub operations_per_sec: f64,
    pub cache_hit_rate: f64,
    pub avg_latency_us: u64,
    pub error_rate: f64,
    pub database_size_mb: f64,
    pub active_transactions: usize,
}

/// Text formatter for metrics snapshot
struct TextReport<'a>(&'a MetricsSnapshot);

impl fmt::Display for TextReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.0;
        writeln!(f, "=== Database Metrics Report ===")?;
        writeln!(f)?;
        writeln!(f, "## Operations")?;
        writeln!(f, "  Reads:        {:>12}", s.reads)?;
        writeln!(f, "  Writes:       {:>12}", s.writes)?;
        writeln!(f, "  Deletes:      {:>12}", s.deletes)?;
        writeln!(f, "  Transactions: {:>12}", s.transactions)?;
        writeln!(f)?;
        writeln!(f, "## Latencies (μs)")?;
        writeln!(f, "  Avg Read:        {:>10}", s.avg_read_latency_us)?;
        writeln!(f, "  Avg Write:       {:>10}", s.avg_write_latency_us)?;
        writeln!(f, "  Avg Delete:      {:>10}", s.avg_delete_latency_us)?;
        writeln!(f, "  Avg Transaction: {:>10}", s.avg_transaction_latency_us)?;
        writeln!(f)?;
        writeln!(f, "## Cache Performance")?;
        writeln!(f, "  Hits:      {:>12}", s.cache_hits)?;
        writeln!(f, "  Misses:    {:>12}", s.cache_misses)?;
        writeln!(f, "  Hit Rate:  {:>11.1}%", s.cache_hit_rate * 100.0)?;
        writeln!(f, "  Evictions: {:>12}", s.cache_evictions)?;
        writeln!(f)?;
        writeln!(f, "## Page I/O")?;
        writeln!(f, "  Pages Read:    {:>12}", s.pages_read)?;
        writeln!(f, "  Pages Written: {:>12}", s.pages_written)?;
        writeln!(f, "  Page Faults:   {:>12}", s.page_faults)?;
        writeln!(f)?;
        writeln!(f, "## Compaction")?;
        writeln!(f, "  Compactions:    {:>12}", s.compactions)?;
        writeln!(f, "  Bytes Read:     {:>12}", s.compaction_bytes_read)?;
        writeln!(f, "  Bytes Written:  {:>12}", s.compaction_bytes_written)?;
        writeln!(f)?;
        writeln!(f, "## Errors")?;
        writeln!(f, "  Read Errors:        {:>12}", s.read_errors)?;
        writeln!(f, "  Write Errors:       {:>12}", s.write_errors)?;
        writeln!(f, "  Transaction Aborts: {:>12}", s.transaction_aborts)?;
        writeln!(f)?;
        writeln!(f, "## Database State")?;
        writeln!(
            f,
            "  Database Size: {:>10.2} MB",
            (s.database_size_bytes as f64) / (1024.0 * 1024.0)
        )?;
        writeln!(
            f,
            "  WAL Size:      {:>10.2} MB",
            (s.wal_size_bytes as f64) / (1024.0 * 1024.0)
        )?;
        writeln!(f, "  Active Txns:   {:>12}", s.active_transactions)?;
        Ok(())
    }
}

/// Text formatter for window metrics
struct WindowTextReport<'a>(&'a WindowMetrics);

impl fmt::Display for WindowTextReport<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let w = self.0;
        writeln!(f, "  Window Duration:    {:?}", w.window)?;
        writeln!(f, "  Reads/sec:          {:.2}", w.reads_per_sec)?;
        writeln!(f, "  Writes/sec:         {:.2}", w.writes_per_sec)?;
        writeln!(f, "  Avg Read Latency:   {} μs", w.avg_read_latency_us)?;
        writeln!(f, "  Avg Write Latency:  {} μs", w.avg_write_latency_us)?;
        writeln!(f, "  Cache Hit Rate:     {:.1}%", w.cache_hit_rate * 100.0)?;
        writeln!(f, "  Compactions:        {}", w.total_compactions)?;
        writeln!(f, "  Error Rate:         {:.4}%", w.error_rate * 100.0)?;
        Ok(())
    }
}
