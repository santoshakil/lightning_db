//! Hardware Capability Detection
//!
//! Detects hardware features and capabilities for compression optimization including
//! CPU features, memory bandwidth, and specialized compression acceleration.

use serde::{Deserialize, Serialize};
use std::sync::OnceLock;

/// Hardware compression capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareCapabilities {
    /// CPU information
    pub cpu: CpuCapabilities,
    /// Memory information
    pub memory: MemoryCapabilities,
    /// Storage information
    pub storage: StorageCapabilities,
    /// Specialized compression hardware
    pub compression: CompressionHardware,
}

/// CPU capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpuCapabilities {
    /// Number of physical cores
    pub physical_cores: usize,
    /// Number of logical cores (with hyperthreading)
    pub logical_cores: usize,
    /// CPU frequency in MHz
    pub frequency_mhz: u32,
    /// Cache information
    pub cache: CacheInfo,
    /// Instruction set extensions
    pub instruction_sets: InstructionSets,
}

/// Cache information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheInfo {
    /// L1 data cache size in KB
    pub l1_data_kb: u32,
    /// L1 instruction cache size in KB
    pub l1_instruction_kb: u32,
    /// L2 cache size in KB
    pub l2_kb: u32,
    /// L3 cache size in KB
    pub l3_kb: u32,
    /// Cache line size in bytes
    pub line_size: u32,
}

/// Instruction set extensions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstructionSets {
    /// SSE support
    pub sse: bool,
    /// SSE2 support
    pub sse2: bool,
    /// SSE3 support
    pub sse3: bool,
    /// SSE4.1 support
    pub sse4_1: bool,
    /// SSE4.2 support
    pub sse4_2: bool,
    /// AVX support
    pub avx: bool,
    /// AVX2 support
    pub avx2: bool,
    /// AVX-512 support
    pub avx512: bool,
    /// AES-NI support
    pub aes_ni: bool,
    /// CRC32 support
    pub crc32: bool,
    /// POPCNT support
    pub popcnt: bool,
    /// BMI1 support
    pub bmi1: bool,
    /// BMI2 support
    pub bmi2: bool,
}

/// Memory capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryCapabilities {
    /// Total system memory in GB
    pub total_gb: u64,
    /// Available memory in GB
    pub available_gb: u64,
    /// Memory bandwidth in GB/s
    pub bandwidth_gb_s: f64,
    /// Memory type (DDR4, DDR5, etc.)
    pub memory_type: String,
    /// Memory frequency in MHz
    pub frequency_mhz: u32,
    /// Number of memory channels
    pub channels: u32,
}

/// Storage capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCapabilities {
    /// Storage type (SSD, NVMe, HDD)
    pub storage_type: String,
    /// Sequential read speed in MB/s
    pub seq_read_mb_s: u32,
    /// Sequential write speed in MB/s
    pub seq_write_mb_s: u32,
    /// Random read IOPS
    pub random_read_iops: u32,
    /// Random write IOPS
    pub random_write_iops: u32,
    /// Queue depth
    pub queue_depth: u32,
}

/// Compression hardware acceleration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionHardware {
    /// Intel QAT (QuickAssist Technology) available
    pub intel_qat: bool,
    /// NVIDIA GPU compression support
    pub nvidia_gpu: bool,
    /// AMD GPU compression support
    pub amd_gpu: bool,
    /// ARM NEON support
    pub arm_neon: bool,
    /// Custom FPGA acceleration
    pub fpga: bool,
    /// Software-based acceleration libraries
    pub software_acceleration: SoftwareAcceleration,
}

/// Software acceleration libraries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoftwareAcceleration {
    /// Intel IPP (Integrated Performance Primitives)
    pub intel_ipp: bool,
    /// Intel MKL (Math Kernel Library)
    pub intel_mkl: bool,
    /// OpenMP support
    pub openmp: bool,
    /// Threading libraries
    pub threading: ThreadingCapabilities,
}

/// Threading capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadingCapabilities {
    /// Maximum number of threads
    pub max_threads: usize,
    /// NUMA topology
    pub numa_nodes: u32,
    /// Thread affinity support
    pub affinity_support: bool,
}

impl HardwareCapabilities {
    /// Detect hardware capabilities
    pub fn detect() -> Self {
        static CAPABILITIES: OnceLock<HardwareCapabilities> = OnceLock::new();

        CAPABILITIES.get_or_init(Self::detect_internal).clone()
    }

    /// Internal detection implementation
    fn detect_internal() -> Self {
        Self {
            cpu: CpuCapabilities::detect(),
            memory: MemoryCapabilities::detect(),
            storage: StorageCapabilities::detect(),
            compression: CompressionHardware::detect(),
        }
    }

    /// Check if hardware compression acceleration is available
    pub fn has_compression_acceleration(&self) -> bool {
        self.compression.intel_qat
            || self.compression.nvidia_gpu
            || self.compression.amd_gpu
            || self.compression.fpga
    }

    /// Check if SIMD acceleration is available
    pub fn has_simd_acceleration(&self) -> bool {
        self.cpu.instruction_sets.avx2
            || self.cpu.instruction_sets.avx512
            || self.compression.arm_neon
    }

    /// Get optimal number of compression threads
    pub fn optimal_compression_threads(&self) -> usize {
        // Use 75% of logical cores, but at least 1 and at most 16
        let threads = (self.cpu.logical_cores * 3 / 4).clamp(1, 16);

        // Adjust based on memory bandwidth
        if self.memory.bandwidth_gb_s < 10.0 {
            threads / 2 // Memory-bound, reduce threads
        } else {
            threads
        }
    }

    /// Get recommended block size for compression
    pub fn recommended_block_size(&self) -> usize {
        // Base on L3 cache size
        let cache_based = (self.cpu.cache.l3_kb as usize * 1024) / 4; // 1/4 of L3 cache

        // Clamp to reasonable range
        cache_based.max(32 * 1024).min(1024 * 1024) // 32KB to 1MB
    }

    /// Check if parallel compression is beneficial
    pub fn benefits_from_parallel_compression(&self) -> bool {
        self.cpu.logical_cores > 2 && self.memory.bandwidth_gb_s > 5.0
    }

    /// Get memory pressure level (0.0 to 1.0)
    pub fn memory_pressure(&self) -> f64 {
        1.0 - (self.memory.available_gb as f64 / self.memory.total_gb as f64)
    }

    /// Check if storage is fast enough for compression overhead
    pub fn storage_supports_compression(&self) -> bool {
        // If storage is slow, compression can actually improve performance
        // by reducing I/O
        self.storage.seq_write_mb_s < 500 || self.storage.seq_read_mb_s > 1000
    }

    /// Estimate compression performance factor
    pub fn compression_performance_factor(&self) -> f64 {
        let mut factor = 1.0;

        // CPU frequency boost
        factor += (self.cpu.frequency_mhz as f64 / 3000.0) * 0.3;

        // Core count boost
        factor += (self.cpu.logical_cores as f64 / 8.0) * 0.2;

        // SIMD boost
        if self.cpu.instruction_sets.avx512 {
            factor += 0.5;
        } else if self.cpu.instruction_sets.avx2 {
            factor += 0.3;
        } else if self.cpu.instruction_sets.avx {
            factor += 0.2;
        }

        // Hardware acceleration boost
        if self.has_compression_acceleration() {
            factor += 1.0;
        }

        // Memory bandwidth factor
        factor *= (self.memory.bandwidth_gb_s / 20.0).min(2.0);

        factor
    }
}

impl CpuCapabilities {
    fn detect() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            Self::detect_x86_64()
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::detect_aarch64()
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::detect_generic()
        }
    }

    #[cfg(target_arch = "x86_64")]
    fn detect_x86_64() -> Self {
        let logical_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        // Estimate physical cores (assume hyperthreading if > 4 cores)
        let physical_cores = if logical_cores > 4 {
            logical_cores / 2
        } else {
            logical_cores
        };

        Self {
            physical_cores,
            logical_cores,
            frequency_mhz: Self::detect_cpu_frequency(),
            cache: CacheInfo::detect(),
            instruction_sets: InstructionSets::detect_x86_64(),
        }
    }

    #[cfg(target_arch = "aarch64")]
    fn detect_aarch64() -> Self {
        let logical_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        Self {
            physical_cores: logical_cores, // ARM typically doesn't have hyperthreading
            logical_cores,
            frequency_mhz: Self::detect_cpu_frequency(),
            cache: CacheInfo::detect(),
            instruction_sets: InstructionSets::detect_aarch64(),
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    fn detect_generic() -> Self {
        let logical_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        Self {
            physical_cores: logical_cores,
            logical_cores,
            frequency_mhz: 2000, // Conservative estimate
            cache: CacheInfo::default(),
            instruction_sets: InstructionSets::default(),
        }
    }

    fn detect_cpu_frequency() -> u32 {
        // Try to read from /proc/cpuinfo on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(content) = std::fs::read_to_string("/proc/cpuinfo") {
                for line in content.lines() {
                    if line.starts_with("cpu MHz") {
                        if let Some(freq_str) = line.split(':').nth(1) {
                            if let Ok(freq) = freq_str.trim().parse::<f32>() {
                                return freq as u32;
                            }
                        }
                    }
                }
            }
        }

        // Default estimate
        2500 // 2.5 GHz
    }
}

impl CacheInfo {
    fn detect() -> Self {
        // Try to detect actual cache sizes
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }
        #[cfg(target_os = "macos")]
        {
            Self::detect_macos()
        }
        #[cfg(target_os = "windows")]
        {
            Self::detect_windows()
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            Self::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn detect_linux() -> Self {
        let mut cache = Self::default();

        // Read from /sys/devices/system/cpu/cpu0/cache/
        if let Ok(entries) = std::fs::read_dir("/sys/devices/system/cpu/cpu0/cache") {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(index_name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Ok(level_str) = std::fs::read_to_string(path.join("level")) {
                        if let Ok(level) = level_str.trim().parse::<u32>() {
                            if let Ok(size_str) = std::fs::read_to_string(path.join("size")) {
                                if let Ok(size_kb) = Self::parse_cache_size(&size_str) {
                                    match level {
                                        1 => {
                                            if index_name.contains("index0") {
                                                cache.l1_data_kb = size_kb;
                                            } else if index_name.contains("index1") {
                                                cache.l1_instruction_kb = size_kb;
                                            }
                                        }
                                        2 => cache.l2_kb = size_kb,
                                        3 => cache.l3_kb = size_kb,
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        cache
    }

    #[cfg(target_os = "macos")]
    fn detect_macos() -> Self {
        // macOS detection would use sysctl
        Self::default()
    }

    #[cfg(target_os = "windows")]
    fn detect_windows() -> Self {
        // Windows detection would use GetLogicalProcessorInformation
        Self::default()
    }

    fn parse_cache_size(size_str: &str) -> Result<u32, std::num::ParseIntError> {
        let trimmed = size_str.trim();
        if trimmed.ends_with('K') {
            trimmed[..trimmed.len() - 1].parse::<u32>()
        } else if trimmed.ends_with("KB") {
            trimmed[..trimmed.len() - 2].parse::<u32>()
        } else {
            trimmed.parse::<u32>()
        }
    }
}

impl Default for CacheInfo {
    fn default() -> Self {
        Self {
            l1_data_kb: 32,
            l1_instruction_kb: 32,
            l2_kb: 256,
            l3_kb: 8192, // 8MB
            line_size: 64,
        }
    }
}

impl InstructionSets {
    #[cfg(target_arch = "x86_64")]
    fn detect_x86_64() -> Self {
        Self {
            sse: is_x86_feature_detected!("sse"),
            sse2: is_x86_feature_detected!("sse2"),
            sse3: is_x86_feature_detected!("sse3"),
            sse4_1: is_x86_feature_detected!("sse4.1"),
            sse4_2: is_x86_feature_detected!("sse4.2"),
            avx: is_x86_feature_detected!("avx"),
            avx2: is_x86_feature_detected!("avx2"),
            avx512: is_x86_feature_detected!("avx512f"),
            aes_ni: is_x86_feature_detected!("aes"),
            crc32: is_x86_feature_detected!("sse4.2"), // CRC32 is part of SSE4.2
            popcnt: is_x86_feature_detected!("popcnt"),
            bmi1: is_x86_feature_detected!("bmi1"),
            bmi2: is_x86_feature_detected!("bmi2"),
        }
    }

    #[cfg(target_arch = "aarch64")]
    fn detect_aarch64() -> Self {
        Self {
            sse: false,
            sse2: false,
            sse3: false,
            sse4_1: false,
            sse4_2: false,
            avx: false,
            avx2: false,
            avx512: false,
            aes_ni: std::arch::is_aarch64_feature_detected!("aes"),
            crc32: std::arch::is_aarch64_feature_detected!("crc"),
            popcnt: false,
            bmi1: false,
            bmi2: false,
        }
    }

    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    fn detect_generic() -> Self {
        Self::default()
    }
}

impl Default for InstructionSets {
    fn default() -> Self {
        Self {
            sse: false,
            sse2: false,
            sse3: false,
            sse4_1: false,
            sse4_2: false,
            avx: false,
            avx2: false,
            avx512: false,
            aes_ni: false,
            crc32: false,
            popcnt: false,
            bmi1: false,
            bmi2: false,
        }
    }
}

impl MemoryCapabilities {
    fn detect() -> Self {
        let total_gb = Self::detect_total_memory();
        let available_gb = Self::detect_available_memory();

        Self {
            total_gb,
            available_gb,
            bandwidth_gb_s: Self::estimate_bandwidth(),
            memory_type: Self::detect_memory_type(),
            frequency_mhz: Self::detect_frequency(),
            channels: Self::detect_channels(),
        }
    }

    fn detect_total_memory() -> u64 {
        #[cfg(target_os = "linux")]
        {
            if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
                for line in content.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                return kb / 1024 / 1024; // Convert to GB
                            }
                        }
                    }
                }
            }
        }

        // Fallback estimate
        8 // 8GB
    }

    fn detect_available_memory() -> u64 {
        #[cfg(target_os = "linux")]
        {
            if let Ok(content) = std::fs::read_to_string("/proc/meminfo") {
                for line in content.lines() {
                    if line.starts_with("MemAvailable:") {
                        if let Some(kb_str) = line.split_whitespace().nth(1) {
                            if let Ok(kb) = kb_str.parse::<u64>() {
                                return kb / 1024 / 1024; // Convert to GB
                            }
                        }
                    }
                }
            }
        }

        // Fallback: assume 75% of total is available
        Self::detect_total_memory() * 3 / 4
    }

    fn estimate_bandwidth() -> f64 {
        // Very rough estimation based on memory type and channels
        // Modern DDR4: ~25 GB/s per channel
        // Modern DDR5: ~40 GB/s per channel
        let channels = Self::detect_channels() as f64;

        if Self::detect_memory_type().contains("DDR5") {
            40.0 * channels
        } else {
            25.0 * channels
        }
    }

    fn detect_memory_type() -> String {
        // This would require platform-specific detection
        "DDR4".to_string()
    }

    fn detect_frequency() -> u32 {
        // Default DDR4 frequency
        2400
    }

    fn detect_channels() -> u32 {
        // Estimate based on total memory
        let total_gb = Self::detect_total_memory();
        if total_gb >= 32 {
            4 // Quad channel
        } else if total_gb >= 16 {
            2 // Dual channel
        } else {
            1 // Single channel
        }
    }
}

impl StorageCapabilities {
    fn detect() -> Self {
        #[cfg(target_os = "linux")]
        {
            Self::detect_linux()
        }
        #[cfg(not(target_os = "linux"))]
        {
            Self::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn detect_linux() -> Self {
        // Try to detect NVMe vs SSD vs HDD
        if std::path::Path::new("/sys/block").exists() {
            // Look for NVMe devices
            if let Ok(entries) = std::fs::read_dir("/sys/block") {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    if let Some(name_str) = name.to_str() {
                        if name_str.starts_with("nvme") {
                            return Self::nvme_defaults();
                        }
                    }
                }
            }

            // Assume SSD if no NVMe found
            Self::ssd_defaults()
        } else {
            Self::default()
        }
    }

    fn nvme_defaults() -> Self {
        Self {
            storage_type: "NVMe".to_string(),
            seq_read_mb_s: 3500,
            seq_write_mb_s: 3000,
            random_read_iops: 500000,
            random_write_iops: 400000,
            queue_depth: 32,
        }
    }

    fn ssd_defaults() -> Self {
        Self {
            storage_type: "SSD".to_string(),
            seq_read_mb_s: 550,
            seq_write_mb_s: 520,
            random_read_iops: 100000,
            random_write_iops: 90000,
            queue_depth: 32,
        }
    }
}

impl Default for StorageCapabilities {
    fn default() -> Self {
        Self {
            storage_type: "HDD".to_string(),
            seq_read_mb_s: 150,
            seq_write_mb_s: 140,
            random_read_iops: 120,
            random_write_iops: 100,
            queue_depth: 1,
        }
    }
}

impl CompressionHardware {
    fn detect() -> Self {
        Self {
            intel_qat: Self::detect_intel_qat(),
            nvidia_gpu: Self::detect_nvidia_gpu(),
            amd_gpu: Self::detect_amd_gpu(),
            arm_neon: Self::detect_arm_neon(),
            fpga: Self::detect_fpga(),
            software_acceleration: SoftwareAcceleration::detect(),
        }
    }

    fn detect_intel_qat() -> bool {
        // Check for Intel QAT devices
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new("/dev/qat_adf_ctl").exists()
                || std::path::Path::new("/sys/bus/pci/drivers/qat_dh895xcc").exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    fn detect_nvidia_gpu() -> bool {
        // Check for NVIDIA GPU
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new("/proc/driver/nvidia").exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    fn detect_amd_gpu() -> bool {
        // Check for AMD GPU
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new("/sys/class/drm").exists()
                && std::fs::read_dir("/sys/class/drm")
                    .map(|entries| {
                        entries.flatten().any(|entry| {
                            entry
                                .file_name()
                                .to_str()
                                .map_or(false, |name| name.contains("amdgpu"))
                        })
                    })
                    .unwrap_or(false)
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    fn detect_arm_neon() -> bool {
        #[cfg(target_arch = "aarch64")]
        {
            std::arch::is_aarch64_feature_detected!("neon")
        }
        #[cfg(not(target_arch = "aarch64"))]
        {
            false
        }
    }

    fn detect_fpga() -> bool {
        // Check for FPGA devices (very basic detection)
        #[cfg(target_os = "linux")]
        {
            std::path::Path::new("/dev/fpga0").exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }
}

impl SoftwareAcceleration {
    fn detect() -> Self {
        Self {
            intel_ipp: Self::detect_intel_ipp(),
            intel_mkl: Self::detect_intel_mkl(),
            openmp: Self::detect_openmp(),
            threading: ThreadingCapabilities::detect(),
        }
    }

    fn detect_intel_ipp() -> bool {
        // Check if Intel IPP is available
        false // Placeholder - would check for IPP installation
    }

    fn detect_intel_mkl() -> bool {
        // Check if Intel MKL is available
        false // Placeholder - would check for MKL installation
    }

    fn detect_openmp() -> bool {
        // Check if OpenMP is available
        false // Placeholder - would be set by build script
    }
}

impl ThreadingCapabilities {
    fn detect() -> Self {
        Self {
            max_threads: std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1),
            numa_nodes: Self::detect_numa_nodes(),
            affinity_support: Self::detect_affinity_support(),
        }
    }

    fn detect_numa_nodes() -> u32 {
        #[cfg(target_os = "linux")]
        {
            if let Ok(entries) = std::fs::read_dir("/sys/devices/system/node") {
                entries
                    .flatten()
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .map_or(false, |name| name.starts_with("node"))
                    })
                    .count() as u32
            } else {
                1
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            1
        }
    }

    fn detect_affinity_support() -> bool {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
        {
            true
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_detection() {
        let capabilities = HardwareCapabilities::detect();

        // Basic sanity checks
        assert!(capabilities.cpu.logical_cores > 0);
        assert!(capabilities.cpu.frequency_mhz > 0);
        assert!(capabilities.memory.total_gb > 0);
        assert!(capabilities.memory.bandwidth_gb_s > 0.0);
    }

    #[test]
    fn test_cpu_capabilities() {
        let cpu = CpuCapabilities::detect();

        assert!(cpu.logical_cores >= cpu.physical_cores);
        assert!(cpu.physical_cores > 0);
        assert!(cpu.frequency_mhz > 0);
    }

    #[test]
    fn test_cache_info() {
        let cache = CacheInfo::detect();

        assert!(cache.l1_data_kb > 0);
        assert!(cache.l2_kb > 0);
        assert!(cache.line_size > 0);
    }

    #[test]
    fn test_memory_capabilities() {
        let memory = MemoryCapabilities::detect();

        assert!(memory.total_gb > 0);
        assert!(memory.available_gb <= memory.total_gb);
        assert!(memory.bandwidth_gb_s > 0.0);
        assert!(memory.channels > 0);
    }

    #[test]
    fn test_optimization_recommendations() {
        let capabilities = HardwareCapabilities::detect();

        let threads = capabilities.optimal_compression_threads();
        assert!(threads > 0);
        assert!(threads <= capabilities.cpu.logical_cores);

        let block_size = capabilities.recommended_block_size();
        assert!(block_size >= 32 * 1024);
        assert!(block_size <= 1024 * 1024);

        let performance_factor = capabilities.compression_performance_factor();
        assert!(performance_factor > 0.0);
    }

    #[test]
    fn test_feature_detection() {
        let capabilities = HardwareCapabilities::detect();

        // These should not panic
        let _has_compression = capabilities.has_compression_acceleration();
        let _has_simd = capabilities.has_simd_acceleration();
        let _benefits_parallel = capabilities.benefits_from_parallel_compression();
        let _storage_supports = capabilities.storage_supports_compression();
    }
}
