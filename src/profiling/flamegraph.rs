//! Flamegraph Generation Module
//!
//! Generates interactive flamegraph visualizations from CPU profiling data
//! to help identify performance hotspots and call stack patterns.

use std::collections::HashMap;
use std::path::Path;
use std::fs::File;
use std::io::{Write, BufWriter};
use serde::Deserialize;

/// Generate a flamegraph from CPU profile data
pub fn generate_flamegraph(profile_path: &Path, output_path: &Path) -> Result<(), super::ProfilingError> {
    // Read and parse the profile data
    let profile_data = read_profile_data(profile_path)?;
    
    // Build call stack tree
    let stack_tree = build_stack_tree(&profile_data)?;
    
    // Generate SVG flamegraph
    generate_svg_flamegraph(&stack_tree, output_path)?;
    
    Ok(())
}

/// Profile data structure for flamegraph generation
#[derive(Debug, Deserialize)]
struct ProfileData {
    samples: Vec<CpuSample>,
    total_samples: u64,
}

#[derive(Debug, Deserialize)]
struct CpuSample {
    stack_trace: Vec<StackFrame>,
    cpu_usage: f64,
}

#[derive(Debug, Deserialize)]
struct StackFrame {
    function_name: String,
    module_name: String,
    file_name: Option<String>,
    line_number: Option<u32>,
}

/// Node in the call stack tree
#[derive(Debug, Clone)]
struct StackNode {
    name: String,
    full_name: String,
    sample_count: u64,
    children: HashMap<String, StackNode>,
    percentage: f64,
}

impl StackNode {
    fn new(name: String, full_name: String) -> Self {
        Self {
            name,
            full_name,
            sample_count: 0,
            children: HashMap::new(),
            percentage: 0.0,
        }
    }

    fn add_sample(&mut self) {
        self.sample_count += 1;
    }

    fn calculate_percentages(&mut self, total_samples: u64) {
        self.percentage = (self.sample_count as f64 / total_samples as f64) * 100.0;
        for child in self.children.values_mut() {
            child.calculate_percentages(total_samples);
        }
    }
}

/// Read and parse profile data
fn read_profile_data(profile_path: &Path) -> Result<ProfileData, super::ProfilingError> {
    let content = std::fs::read_to_string(profile_path)
        .map_err(|e| super::ProfilingError::IoError(format!("Failed to read profile: {}", e)))?;
    
    let parsed_data: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| super::ProfilingError::SerializationError(e.to_string()))?;
    
    // Extract samples from the profile data
    let samples: Vec<CpuSample> = parsed_data["samples"]
        .as_array()
        .ok_or_else(|| super::ProfilingError::SerializationError("No samples found".to_string()))?
        .iter()
        .map(|sample| {
            let stack_trace = sample["stack_trace"]
                .as_array()
                .unwrap_or(&vec![])
                .iter()
                .map(|frame| StackFrame {
                    function_name: frame["function_name"].as_str().unwrap_or("unknown").to_string(),
                    module_name: frame["module_name"].as_str().unwrap_or("unknown").to_string(),
                    file_name: frame["file_name"].as_str().map(|s| s.to_string()),
                    line_number: frame["line_number"].as_u64().map(|n| n as u32),
                })
                .collect();
            
            CpuSample {
                stack_trace,
                cpu_usage: sample["cpu_usage"].as_f64().unwrap_or(0.0),
            }
        })
        .collect();

    let total_samples = parsed_data["total_samples"].as_u64().unwrap_or(samples.len() as u64);

    Ok(ProfileData {
        samples,
        total_samples,
    })
}

/// Build call stack tree from profile samples
fn build_stack_tree(profile_data: &ProfileData) -> Result<StackNode, super::ProfilingError> {
    let mut root = StackNode::new("root".to_string(), "root".to_string());

    for sample in &profile_data.samples {
        let mut current_node = &mut root;
        
        // Walk through the stack trace from root to leaf
        for frame in sample.stack_trace.iter().rev() {
            let frame_name = if frame.module_name == "unknown" {
                frame.function_name.clone()
            } else if frame.function_name.contains("::") {
                frame.function_name.clone()
            } else {
                format!("{}::{}", frame.module_name, frame.function_name)
            };

            let full_name = if let (Some(file), Some(line)) = (&frame.file_name, frame.line_number) {
                format!("{} ({}:{})", frame_name, file, line)
            } else {
                frame_name.clone()
            };

            // Get or create child node
            let child = current_node.children
                .entry(frame_name.clone())
                .or_insert_with(|| StackNode::new(frame_name.clone(), full_name));
            
            current_node = child;
        }
        
        // Add sample to the leaf node
        current_node.add_sample();
    }

    // Calculate percentages
    root.calculate_percentages(profile_data.total_samples);

    Ok(root)
}

/// Generate SVG flamegraph
fn generate_svg_flamegraph(root: &StackNode, output_path: &Path) -> Result<(), super::ProfilingError> {
    let file = File::create(output_path)
        .map_err(|e| super::ProfilingError::IoError(format!("Failed to create flamegraph file: {}", e)))?;
    let mut writer = BufWriter::new(file);

    // SVG dimensions
    let width = 1200;
    let height = 800;
    let font_size = 12;
    let min_width = 0.1; // Minimum width percentage to show a frame

    // Write SVG header
    writeln!(writer, r#"<?xml version="1.0" encoding="UTF-8"?>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" style="background-color: white;">"#, width, height)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

    // Add CSS styles
    writeln!(writer, r#"<defs>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"<style type="text/css">"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"
        .frame {{ stroke: white; stroke-width: 1; cursor: pointer; }}
        .frame:hover {{ stroke: black; stroke-width: 2; }}
        .frame-text {{ font-family: Verdana, sans-serif; font-size: {}px; fill: black; pointer-events: none; }}
        .title {{ font-family: Verdana, sans-serif; font-size: 16px; font-weight: bold; fill: black; }}
        .details {{ font-family: Verdana, sans-serif; font-size: 12px; fill: #666; }}
    "#, font_size)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"</style>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"</defs>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

    // Add title
    writeln!(writer, r#"<text x="10" y="25" class="title">Lightning DB Performance Flamegraph</text>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"<text x="10" y="45" class="details">Total samples: {} | Hover over frames for details | Click to zoom</text>"#, root.sample_count)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

    // Generate flamegraph frames starting from y=60
    let frame_height = 20;
    let start_y = 60;
    let max_depth = calculate_max_depth(root);
    let available_height = height - start_y - 20;
    let actual_frame_height = if max_depth > 0 {
        (available_height / max_depth).min(frame_height)
    } else {
        frame_height
    };

    render_flame_frames(
        &mut writer,
        root,
        0.0,
        100.0,
        start_y,
        actual_frame_height,
        width as f64,
        min_width,
    )?;

    // Close SVG
    writeln!(writer, r#"</svg>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

    writer.flush()
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

    Ok(())
}

/// Calculate maximum depth of the call stack tree
fn calculate_max_depth(node: &StackNode) -> usize {
    if node.children.is_empty() {
        1
    } else {
        1 + node.children.values().map(calculate_max_depth).max().unwrap_or(0)
    }
}

/// Render flame frames recursively
fn render_flame_frames(
    writer: &mut BufWriter<File>,
    node: &StackNode,
    start_percent: f64,
    width_percent: f64,
    y: usize,
    frame_height: usize,
    svg_width: f64,
    min_width: f64,
) -> Result<(), super::ProfilingError> {
    // Skip frames that are too narrow to be visible
    if width_percent < min_width {
        return Ok(());
    }

    // Calculate frame dimensions
    let x = (start_percent / 100.0) * svg_width;
    let width = (width_percent / 100.0) * svg_width;

    // Choose color based on function type
    let color = choose_frame_color(&node.name);

    // Render frame rectangle
    writeln!(
        writer,
        r#"<rect class="frame" x="{:.1}" y="{}" width="{:.1}" height="{}" fill="{}" title="{}: {:.2}% ({} samples)">"#,
        x, y, width, frame_height, color, node.full_name, node.percentage, node.sample_count
    ).map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    writeln!(writer, r#"</rect>"#)
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;

    // Add text if frame is wide enough
    if width > 50.0 {
        let text_x = x + 5.0;
        let text_y = y + frame_height / 2 + 4;
        let display_name = truncate_function_name(&node.name, width as usize);
        
        writeln!(
            writer,
            r#"<text class="frame-text" x="{:.1}" y="{}"{}>{}% {}</text>"#,
            text_x, text_y,
            if width < 100.0 { r#" font-size="10""# } else { "" },
            format!("{:.1}", node.percentage),
            display_name
        ).map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    }

    // Render children
    if !node.children.is_empty() {
        let mut child_start_percent = start_percent;
        let total_child_samples: u64 = node.children.values().map(|c| c.sample_count).sum();
        
        if total_child_samples > 0 {
            for child in node.children.values() {
                let child_width_percent = (child.sample_count as f64 / total_child_samples as f64) * width_percent;
                
                render_flame_frames(
                    writer,
                    child,
                    child_start_percent,
                    child_width_percent,
                    y + frame_height + 1,
                    frame_height,
                    svg_width,
                    min_width,
                )?;
                
                child_start_percent += child_width_percent;
            }
        }
    }

    Ok(())
}

/// Choose appropriate color for a frame based on function type
fn choose_frame_color(function_name: &str) -> &'static str {
    if function_name.contains("lightning_db") {
        if function_name.contains("btree") {
            "#ff6b6b" // Red for B-tree operations
        } else if function_name.contains("storage") {
            "#4ecdc4" // Teal for storage operations
        } else if function_name.contains("cache") {
            "#45b7d1" // Blue for cache operations
        } else if function_name.contains("wal") {
            "#f9ca24" // Yellow for WAL operations
        } else if function_name.contains("transaction") {
            "#6c5ce7" // Purple for transaction operations
        } else {
            "#a8e6cf" // Light green for other lightning_db functions
        }
    } else if function_name.contains("std::") {
        "#ddd" // Gray for standard library
    } else if function_name.contains("alloc::") {
        "#ffbe76" // Orange for allocator
    } else if function_name.contains("core::") {
        "#fd79a8" // Pink for core
    } else {
        "#74b9ff" // Light blue for other functions
    }
}

/// Truncate function name to fit within the given width
fn truncate_function_name(name: &str, width: usize) -> String {
    // Estimate character width (rough approximation)
    let char_width = 7; // pixels per character
    let max_chars = (width / char_width).saturating_sub(3); // Leave room for percentage
    
    if name.len() <= max_chars {
        name.to_string()
    } else if max_chars > 10 {
        let mut truncated = name.chars().take(max_chars - 3).collect::<String>();
        truncated.push_str("...");
        truncated
    } else {
        // Very narrow, just show first few characters
        name.chars().take(max_chars).collect()
    }
}

/// Generate a simple text-based flamegraph for console output
pub fn generate_text_flamegraph(profile_path: &Path) -> Result<String, super::ProfilingError> {
    let profile_data = read_profile_data(profile_path)?;
    let stack_tree = build_stack_tree(&profile_data)?;
    
    let mut output = String::new();
    output.push_str("Lightning DB Performance Profile (Text Flamegraph)\n");
    output.push_str("=" .repeat(60).as_str());
    output.push('\n');
    output.push_str(&format!("Total samples: {}\n\n", stack_tree.sample_count));
    
    render_text_node(&stack_tree, &mut output, 0, "")?;
    
    Ok(output)
}

/// Render a node in text format
fn render_text_node(
    node: &StackNode,
    output: &mut String,
    depth: usize,
    prefix: &str,
) -> Result<(), super::ProfilingError> {
    if depth > 0 { // Skip root node in output
        let percentage_bar = create_percentage_bar(node.percentage);
        output.push_str(&format!(
            "{}{} {:.2}% ({} samples) {}\n",
            prefix,
            percentage_bar,
            node.percentage,
            node.sample_count,
            node.name
        ));
    }

    // Sort children by sample count (descending)
    let mut children: Vec<_> = node.children.values().collect();
    children.sort_by(|a, b| b.sample_count.cmp(&a.sample_count));

    // Render children
    for (i, child) in children.iter().enumerate() {
        let is_last = i == children.len() - 1;
        let child_prefix = if depth == 0 {
            String::new()
        } else {
            format!("{}{}  ", prefix, if is_last { "└─" } else { "├─" })
        };
        
        render_text_node(child, output, depth + 1, &child_prefix)?;
    }

    Ok(())
}

/// Create a visual percentage bar
fn create_percentage_bar(percentage: f64) -> String {
    let bar_length = 20;
    let filled = ((percentage / 100.0) * bar_length as f64) as usize;
    let empty = bar_length - filled;
    
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

/// Generate flamegraph data in a format suitable for external tools
pub fn export_flamegraph_data(profile_path: &Path, output_path: &Path) -> Result<(), super::ProfilingError> {
    let profile_data = read_profile_data(profile_path)?;
    let stack_tree = build_stack_tree(&profile_data)?;
    
    let file = File::create(output_path)
        .map_err(|e| super::ProfilingError::IoError(format!("Failed to create output file: {}", e)))?;
    let mut writer = BufWriter::new(file);
    
    // Export in collapsed stack format (compatible with flamegraph.pl)
    export_collapsed_stacks(&stack_tree, &mut writer, Vec::new())?;
    
    writer.flush()
        .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    
    Ok(())
}

/// Export stacks in collapsed format
fn export_collapsed_stacks(
    node: &StackNode,
    writer: &mut BufWriter<File>,
    mut stack: Vec<String>,
) -> Result<(), super::ProfilingError> {
    if !node.name.is_empty() && node.name != "root" {
        stack.push(node.name.clone());
    }
    
    if node.children.is_empty() && !stack.is_empty() {
        // Leaf node - write the complete stack
        let stack_str = stack.join(";");
        writeln!(writer, "{} {}", stack_str, node.sample_count)
            .map_err(|e| super::ProfilingError::IoError(e.to_string()))?;
    } else {
        // Continue with children
        for child in node.children.values() {
            export_collapsed_stacks(child, writer, stack.clone())?;
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_frame_color_selection() {
        assert_eq!(choose_frame_color("lightning_db::btree::insert"), "#ff6b6b");
        assert_eq!(choose_frame_color("lightning_db::storage::read"), "#4ecdc4");
        assert_eq!(choose_frame_color("std::collections::HashMap"), "#ddd");
        assert_eq!(choose_frame_color("unknown_function"), "#74b9ff");
    }

    #[test]
    fn test_function_name_truncation() {
        assert_eq!(truncate_function_name("short", 100), "short");
        assert_eq!(truncate_function_name("very_long_function_name_that_should_be_truncated", 50), "very_l...");
        assert_eq!(truncate_function_name("test", 10), "test");
    }

    #[test]
    fn test_percentage_bar_creation() {
        assert_eq!(create_percentage_bar(0.0), "[░░░░░░░░░░░░░░░░░░░░]");
        assert_eq!(create_percentage_bar(50.0), "[██████████░░░░░░░░░░]");
        assert_eq!(create_percentage_bar(100.0), "[████████████████████]");
    }

    #[test]
    fn test_stack_node_creation() {
        let mut node = StackNode::new("test_function".to_string(), "test::test_function".to_string());
        assert_eq!(node.sample_count, 0);
        assert_eq!(node.percentage, 0.0);
        
        node.add_sample();
        assert_eq!(node.sample_count, 1);
        
        node.calculate_percentages(10);
        assert_eq!(node.percentage, 10.0);
    }

    #[test]
    fn test_max_depth_calculation() {
        let mut root = StackNode::new("root".to_string(), "root".to_string());
        assert_eq!(calculate_max_depth(&root), 1);
        
        let mut child1 = StackNode::new("child1".to_string(), "child1".to_string());
        let child2 = StackNode::new("child2".to_string(), "child2".to_string());
        child1.children.insert("child2".to_string(), child2);
        root.children.insert("child1".to_string(), child1);
        
        assert_eq!(calculate_max_depth(&root), 3);
    }

    #[test]
    fn test_text_flamegraph_generation() {
        let temp_dir = TempDir::new().unwrap();
        let profile_path = temp_dir.path().join("test_profile.json");
        
        // Create a simple test profile
        let test_profile = r#"{
            "samples": [
                {
                    "stack_trace": [
                        {
                            "function_name": "main",
                            "module_name": "test",
                            "file_name": "main.rs",
                            "line_number": 10
                        }
                    ],
                    "cpu_usage": 50.0
                }
            ],
            "total_samples": 1
        }"#;
        
        fs::write(&profile_path, test_profile).unwrap();
        
        let result = generate_text_flamegraph(&profile_path);
        assert!(result.is_ok());
        
        let output = result.unwrap();
        assert!(output.contains("Lightning DB Performance Profile"));
        assert!(output.contains("Total samples: 1"));
    }
}