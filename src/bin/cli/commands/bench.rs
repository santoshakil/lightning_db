//! Performance benchmark command

use clap::{Arg, ArgMatches, Command};
use lightning_db::{Database, LightningDbConfig};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

use crate::cli::utils::{
    complete_progress, print_progress, validate_bench_params, validate_db_exists, CliResult,
};
use crate::cli::utils::display::JsonOutput;
use crate::cli::GlobalOptions;

/// Build the 'bench' subcommand
pub fn bench_command() -> Command {
    Command::new("bench")
        .about("Run performance benchmark")
        .arg(
            Arg::new("path")
                .help("Database path")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("operations")
                .help("Number of operations (1-10000000)")
                .long("ops")
                .default_value("10000"),
        )
        .arg(
            Arg::new("threads")
                .help("Number of threads (1-256)")
                .long("threads")
                .default_value("1"),
        )
        .arg(
            Arg::new("value-size")
                .help("Value size in bytes (1-10485760)")
                .long("value-size")
                .default_value("100"),
        )
}

/// Execute the 'bench' command
pub fn run_bench(matches: &ArgMatches) -> CliResult<()> {
    let global = GlobalOptions::from_matches(matches);
    let path = matches
        .get_one::<String>("path")
        .ok_or("path argument is required")?;
    let ops: usize = matches
        .get_one::<String>("operations")
        .ok_or("operations argument is required")?
        .parse()
        .map_err(|_| "operations must be a positive number")?;
    let threads: usize = matches
        .get_one::<String>("threads")
        .ok_or("threads argument is required")?
        .parse()
        .map_err(|_| "threads must be a positive number")?;
    let value_size: usize = matches
        .get_one::<String>("value-size")
        .ok_or("value-size argument is required")?
        .parse()
        .map_err(|_| "value-size must be a positive number")?;

    // Validate all parameters
    validate_bench_params(ops, threads, value_size)?;
    validate_db_exists(path)?;

    if !global.is_json() && !global.quiet {
        println!("Running benchmark...");
        println!("  Operations: {}", ops);
        println!("  Threads:    {}", threads);
        println!("  Value size: {} bytes", value_size);
        println!();
    }

    let db = Arc::new(Database::open(path, LightningDbConfig::default())?);
    let base_ops_per_thread = ops / threads;
    let extra_ops = ops % threads; // Distribute remainder to first threads
    let value = vec![b'x'; value_size];

    // Track actual total ops for accurate reporting
    let actual_total_ops: usize = (0..threads)
        .map(|t| base_ops_per_thread + if t < extra_ops { 1 } else { 0 })
        .sum();

    // Write benchmark
    if !global.is_json() && !global.quiet {
        print_progress("Write test");
        io::stdout().flush().ok();
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(threads);

    for thread_id in 0..threads {
        let db_clone = db.clone();
        let value_clone = value.clone();
        // First 'extra_ops' threads get one extra operation
        let thread_ops = base_ops_per_thread + if thread_id < extra_ops { 1 } else { 0 };

        let handle = std::thread::spawn(move || {
            for i in 0..thread_ops {
                let key = format!("bench_{}_{}", thread_id, i);
                if let Err(e) = db_clone.put(key.as_bytes(), &value_clone) {
                    eprintln!("Error in write operation: {}", e);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.join() {
            eprintln!("Thread join failed: {:?}", e);
        }
    }

    let write_duration = start.elapsed();
    let write_secs = write_duration.as_secs_f64().max(0.000001); // Avoid division by zero
    let write_ops_per_sec = actual_total_ops as f64 / write_secs;

    if !global.is_json() && !global.quiet {
        println!("[OK] {:.0} ops/sec", write_ops_per_sec);
    }

    // Read benchmark
    if !global.is_json() && !global.quiet {
        print_progress("Read test");
        io::stdout().flush().ok();
    }

    let start = Instant::now();
    let mut handles = Vec::with_capacity(threads);

    for thread_id in 0..threads {
        let db_clone = db.clone();
        let thread_ops = base_ops_per_thread + if thread_id < extra_ops { 1 } else { 0 };

        let handle = std::thread::spawn(move || {
            for i in 0..thread_ops {
                let key = format!("bench_{}_{}", thread_id, i);
                if let Err(e) = db_clone.get(key.as_bytes()) {
                    eprintln!("Error in read operation: {}", e);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        if let Err(e) = handle.join() {
            eprintln!("Thread join failed: {:?}", e);
        }
    }

    let read_duration = start.elapsed();
    let read_secs = read_duration.as_secs_f64().max(0.000001); // Avoid division by zero
    let read_ops_per_sec = actual_total_ops as f64 / read_secs;

    if !global.is_json() && !global.quiet {
        println!("[OK] {:.0} ops/sec", read_ops_per_sec);
    }

    // Cleanup
    if !global.is_json() && !global.quiet {
        print_progress("Cleanup");
        io::stdout().flush().ok();
    }

    for thread_id in 0..threads {
        let thread_ops = base_ops_per_thread + if thread_id < extra_ops { 1 } else { 0 };
        for i in 0..thread_ops {
            let key = format!("bench_{}_{}", thread_id, i);
            if let Err(e) = db.delete(key.as_bytes()) {
                eprintln!("Error in cleanup: {}", e);
            }
        }
    }

    if !global.is_json() && !global.quiet {
        complete_progress();
    }

    // Calculate latency metrics
    let write_us_per_op = if actual_total_ops > 0 {
        write_duration.as_micros() as f64 / actual_total_ops as f64
    } else {
        0.0
    };
    let read_us_per_op = if actual_total_ops > 0 {
        read_duration.as_micros() as f64 / actual_total_ops as f64
    } else {
        0.0
    };

    if global.is_json() {
        let mut output = JsonOutput::new();
        output.status(true);
        output.add_str("database", path);
        output.add_uint("operations", actual_total_ops as u64);
        output.add_uint("threads", threads as u64);
        output.add_uint("value_size_bytes", value_size as u64);

        let mut write_stats = JsonOutput::new();
        write_stats.add_float("ops_per_sec", write_ops_per_sec);
        write_stats.add_float("us_per_op", write_us_per_op);
        write_stats.add_float("duration_secs", write_secs);
        output.add_object("write", write_stats);

        let mut read_stats = JsonOutput::new();
        read_stats.add_float("ops_per_sec", read_ops_per_sec);
        read_stats.add_float("us_per_op", read_us_per_op);
        read_stats.add_float("duration_secs", read_secs);
        output.add_object("read", read_stats);

        output.print();
    } else if !global.quiet {
        println!("\nBenchmark Summary:");
        println!("  Total operations: {}", actual_total_ops);
        println!(
            "  Write: {:.0} ops/sec ({:.2} μs/op)",
            write_ops_per_sec, write_us_per_op
        );
        println!(
            "  Read:  {:.0} ops/sec ({:.2} μs/op)",
            read_ops_per_sec, read_us_per_op
        );
    }

    Ok(())
}
