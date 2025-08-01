//! Hardware Detection Module
//!
//! Detects system hardware capabilities including CPU, memory, storage,
//! and provides recommendations for optimal Lightning DB configuration.

use crate::Result;
use std::fs;
use std::path::Path;
use serde::{Serialize, Deserialize};

/// Hardware detector
pub struct HardwareDetector {
    cache_info: Option<CacheInfo>,
}

/// Hardware information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HardwareInfo {
    /// Number of CPU cores
    pub cpu_count: usize,
    /// Number of physical CPU cores (excluding hyperthreading)
    pub physical_cores: usize,
    /// CPU architecture
    pub cpu_arch: String,
    /// CPU model name
    pub cpu_model: String,
    /// CPU base frequency in MHz
    pub cpu_base_freq_mhz: Option<u32>,
    /// CPU features (AVX2, SSE4.2, etc.)
    pub cpu_features: Vec<String>,
    /// Total system memory in MB
    pub total_memory_mb: u64,
    /// Available memory in MB
    pub available_memory_mb: u64,
    /// Memory bandwidth in MB/s
    pub memory_bandwidth_mb_per_sec: Option<f64>,
    /// CPU cache sizes
    pub cache_sizes: CacheSizes,
    /// Storage devices detected
    pub storage_devices: Vec<StorageDevice>,
    /// Primary storage type (SSD/HDD)
    pub storage_type: StorageType,
    /// Storage bandwidth MB/s
    pub storage_bandwidth_mb_per_sec: Option<f64>,
    /// NUMA nodes
    pub numa_nodes: usize,
    /// Huge pages available
    pub huge_pages_available: bool,
    /// GPU devices available
    pub gpu_devices: Vec<GpuDevice>,
    /// Network interfaces
    pub network_interfaces: Vec<NetworkInterface>,
    /// System thermal state
    pub thermal_state: ThermalState,
    /// Power profile
    pub power_profile: PowerProfile,
}

/// CPU cache sizes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSizes {
    pub l1_data_kb: u32,
    pub l1_instruction_kb: u32,
    pub l2_kb: u32,
    pub l3_kb: u32,
    pub cache_line_size: u32,
}

/// Storage device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDevice {
    pub name: String,
    pub device_type: StorageType,
    pub size_gb: Option<u64>,
    pub interface: String,
    pub bandwidth_mb_per_sec: Option<f64>,
}

/// Storage type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum StorageType {
    SSD,
    NVME,
    HDD,
    Unknown,
}

/// GPU device information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GpuDevice {
    pub name: String,
    pub vendor: String,
    pub memory_mb: Option<u64>,
    pub compute_units: Option<u32>,
    pub supports_compute: bool,
}

/// Network interface information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkInterface {
    pub name: String,
    pub interface_type: NetworkType,
    pub speed_mbps: Option<u64>,
    pub is_up: bool,
}

/// Network interface type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum NetworkType {
    Ethernet,
    WiFi,
    Loopback,
    InfiniBand,
    Unknown,
}

/// System thermal state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThermalState {
    pub cpu_temp_celsius: Option<f32>,
    pub thermal_throttling: bool,
    pub cooling_available: bool,
}

/// Power profile information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerProfile {
    pub power_source: PowerSource,
    pub cpu_governor: Option<String>,
    pub max_performance_available: bool,
}

/// Power source type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum PowerSource {
    AC,
    Battery,
    Unknown,
}

/// Cache information
#[derive(Debug, Clone)]
struct CacheInfo {
    l1d_cache: u32,
    l1i_cache: u32,
    l2_cache: u32,
    l3_cache: u32,
    cache_line_size: u32,
}

impl HardwareDetector {
    /// Create a new hardware detector
    pub fn new() -> Self {
        Self {
            cache_info: None,
        }
    }

    /// Detect hardware capabilities
    pub fn detect(&self) -> Result<HardwareInfo> {
        let cpu_count = self.detect_cpu_count();
        let physical_cores = self.detect_physical_cores();
        let (total_memory_mb, available_memory_mb) = self.detect_memory();
        let cpu_features = self.detect_cpu_features();
        let cache_sizes = self.detect_cache_sizes();
        let storage_devices = self.detect_storage_devices();
        let storage_type = self.detect_primary_storage_type(&storage_devices);
        let numa_nodes = self.detect_numa_nodes();
        let huge_pages_available = self.detect_huge_pages();
        let gpu_devices = self.detect_gpu_devices();
        let network_interfaces = self.detect_network_interfaces();
        let thermal_state = self.detect_thermal_state();
        let power_profile = self.detect_power_profile();

        Ok(HardwareInfo {
            cpu_count,
            physical_cores,
            cpu_arch: self.detect_cpu_arch(),
            cpu_model: self.detect_cpu_model(),
            cpu_base_freq_mhz: self.detect_cpu_frequency(),
            cpu_features,
            total_memory_mb,
            available_memory_mb,
            memory_bandwidth_mb_per_sec: self.measure_memory_bandwidth(),
            cache_sizes,
            storage_devices,
            storage_type,
            storage_bandwidth_mb_per_sec: self.measure_storage_bandwidth(),
            numa_nodes,
            huge_pages_available,
            gpu_devices,
            network_interfaces,
            thermal_state,
            power_profile,
        })
    }

    /// Detect number of CPU cores
    fn detect_cpu_count(&self) -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1)
    }

    /// Detect CPU architecture
    fn detect_cpu_arch(&self) -> String {
        #[cfg(target_arch = "x86_64")]
        return "x86_64".to_string();
        
        #[cfg(target_arch = "aarch64")]
        return "aarch64".to_string();
        
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        return "unknown".to_string();
    }

    /// Detect CPU features
    fn detect_cpu_features(&self) -> Vec<String> {
        let mut features = Vec::new();

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                features.push("AVX2".to_string());
            }
            if is_x86_feature_detected!("sse4.2") {
                features.push("SSE4.2".to_string());
            }
            if is_x86_feature_detected!("avx512f") {
                features.push("AVX512F".to_string());
            }
            if is_x86_feature_detected!("bmi2") {
                features.push("BMI2".to_string());
            }
        }

        features
    }

    /// Detect system memory
    fn detect_memory(&self) -> (u64, u64) {
        #[cfg(target_os = "linux")]
        {
            if let Ok(meminfo) = fs::read_to_string("/proc/meminfo") {
                let mut total_kb = 0u64;
                let mut available_kb = 0u64;

                for line in meminfo.lines() {
                    if line.starts_with("MemTotal:") {
                        if let Some(kb) = line.split_whitespace().nth(1) {
                            total_kb = kb.parse().unwrap_or(0);
                        }
                    } else if line.starts_with("MemAvailable:") {
                        if let Some(kb) = line.split_whitespace().nth(1) {
                            available_kb = kb.parse().unwrap_or(0);
                        }
                    }
                }

                return (total_kb / 1024, available_kb / 1024);
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            // Get total memory
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "hw.memsize"])
                .output()
            {
                if let Ok(bytes_str) = String::from_utf8(output.stdout) {
                    if let Ok(bytes) = bytes_str.trim().parse::<u64>() {
                        let total_mb = bytes / 1024 / 1024;
                        // Estimate available as 50% of total on macOS
                        return (total_mb, total_mb / 2);
                    }
                }
            }
        }

        // Default fallback
        (8192, 4096) // 8GB total, 4GB available
    }

    /// Detect CPU cache sizes
    fn detect_cache_sizes(&self) -> CacheSizes {
        #[cfg(target_os = "linux")]
        {
            let mut sizes = CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 8192,
                cache_line_size: 64,
            };

            // Try to read from sysfs
            if let Ok(l1d) = fs::read_to_string("/sys/devices/system/cpu/cpu0/cache/index0/size") {
                if let Ok(kb) = l1d.trim().trim_end_matches('K').parse::<u32>() {
                    sizes.l1_data_kb = kb;
                }
            }

            if let Ok(l2) = fs::read_to_string("/sys/devices/system/cpu/cpu0/cache/index2/size") {
                if let Ok(kb) = l2.trim().trim_end_matches('K').parse::<u32>() {
                    sizes.l2_kb = kb;
                }
            }

            if let Ok(l3) = fs::read_to_string("/sys/devices/system/cpu/cpu0/cache/index3/size") {
                if let Ok(kb) = l3.trim().trim_end_matches('K').parse::<u32>() {
                    sizes.l3_kb = kb;
                }
            }

            return sizes;
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            let mut sizes = CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 8192,
                cache_line_size: 64,
            };

            // Get L1 cache size
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "hw.l1dcachesize"])
                .output()
            {
                if let Ok(bytes_str) = String::from_utf8(output.stdout) {
                    if let Ok(bytes) = bytes_str.trim().parse::<u32>() {
                        sizes.l1_data_kb = bytes / 1024;
                    }
                }
            }

            // Get L2 cache size
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "hw.l2cachesize"])
                .output()
            {
                if let Ok(bytes_str) = String::from_utf8(output.stdout) {
                    if let Ok(bytes) = bytes_str.trim().parse::<u32>() {
                        sizes.l2_kb = bytes / 1024;
                    }
                }
            }

            // Get L3 cache size
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "hw.l3cachesize"])
                .output()
            {
                if let Ok(bytes_str) = String::from_utf8(output.stdout) {
                    if let Ok(bytes) = bytes_str.trim().parse::<u32>() {
                        sizes.l3_kb = bytes / 1024;
                    }
                }
            }

            return sizes;
        }

        // Default values
        CacheSizes {
            l1_data_kb: 32,
            l1_instruction_kb: 32,
            l2_kb: 256,
            l3_kb: 8192,
            cache_line_size: 64,
        }
    }

    /// Detect storage type
    fn detect_storage_type(&self, path: &str) -> StorageType {
        #[cfg(target_os = "linux")]
        {
            // Try to determine if the path is on an SSD
            if let Ok(device) = self.get_device_for_path(path) {
                let rotational_path = format!("/sys/block/{}/queue/rotational", device);
                if let Ok(rotational) = fs::read_to_string(&rotational_path) {
                    if rotational.trim() == "0" {
                        // Check if it's NVMe
                        if device.starts_with("nvme") {
                            return StorageType::NVME;
                        }
                        return StorageType::SSD;
                    } else {
                        return StorageType::HDD;
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            // On macOS, check if using APFS (likely SSD)
            if let Ok(output) = Command::new("diskutil")
                .args(&["info", "/"])
                .output()
            {
                if let Ok(info) = String::from_utf8(output.stdout) {
                    if info.contains("Solid State") || info.contains("APFS") {
                        return StorageType::SSD;
                    }
                }
            }
        }

        StorageType::Unknown
    }

    /// Get device for path (Linux only)
    #[cfg(target_os = "linux")]
    fn get_device_for_path(&self, _path: &str) -> Result<String> {
        // Simplified - in production would use stat and resolve device
        if Path::new("/sys/block/nvme0n1").exists() {
            Ok("nvme0n1".to_string())
        } else if Path::new("/sys/block/sda").exists() {
            Ok("sda".to_string())
        } else {
            Ok("unknown".to_string())
        }
    }

    /// Measure storage bandwidth
    fn measure_storage_bandwidth(&self) -> Option<f64> {
        self.benchmark_storage_io().unwrap_or_else(|_| {
            // Fallback to estimates based on storage type
            match self.detect_storage_type("/") {
                StorageType::NVME => Some(3500.0),
                StorageType::SSD => Some(500.0),
                StorageType::HDD => Some(120.0),
                StorageType::Unknown => None,
            }
        })
    }

    /// Benchmark actual storage I/O performance
    fn benchmark_storage_io(&self) -> Result<Option<f64>> {
        use std::time::Instant;
        use std::io::Write;
        
        // Create a temporary test file
        let test_data = vec![0u8; 1024 * 1024]; // 1MB test data
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("lightning_db_io_test");
        
        let start = Instant::now();
        
        // Perform write test
        let mut file = std::fs::File::create(&test_file)
            .map_err(|e| crate::Error::Io(e.to_string()))?;
        
        for _ in 0..10 {
            file.write_all(&test_data)
                .map_err(|e| crate::Error::Io(e.to_string()))?;
        }
        
        file.sync_all()
            .map_err(|e| crate::Error::Io(e.to_string()))?;
        
        let write_duration = start.elapsed();
        
        // Clean up
        let _ = std::fs::remove_file(&test_file);
        
        // Calculate bandwidth (10MB written)
        let mb_written = 10.0;
        let bandwidth = mb_written / write_duration.as_secs_f64();
        
        Ok(Some(bandwidth))
    }

    /// Detect NUMA nodes
    fn detect_numa_nodes(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            if let Ok(nodes) = fs::read_dir("/sys/devices/system/node/") {
                let count = nodes
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        entry.file_name()
                            .to_str()
                            .map(|s| s.starts_with("node"))
                            .unwrap_or(false)
                    })
                    .count();
                
                if count > 0 {
                    return count;
                }
            }
        }

        1 // Default to single NUMA node
    }

    /// Detect huge pages support
    fn detect_huge_pages(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            Path::new("/sys/kernel/mm/transparent_hugepage/enabled").exists()
        }

        #[cfg(not(target_os = "linux"))]
        {
            false
        }
    }

    /// Detect physical CPU cores (excluding hyperthreading)
    fn detect_physical_cores(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
                let mut physical_cores = std::collections::HashSet::new();
                let mut current_physical_id = None;
                let mut current_core_id = None;

                for line in cpuinfo.lines() {
                    if line.starts_with("physical id") {
                        if let Some(id_str) = line.split(':').nth(1) {
                            current_physical_id = id_str.trim().parse().ok();
                        }
                    } else if line.starts_with("core id") {
                        if let Some(id_str) = line.split(':').nth(1) {
                            current_core_id = id_str.trim().parse().ok();
                        }
                    }

                    if let (Some(phys_id), Some(core_id)) = (current_physical_id, current_core_id) {
                        physical_cores.insert((phys_id, core_id));
                        current_physical_id = None;
                        current_core_id = None;
                    }
                }

                if !physical_cores.is_empty() {
                    return physical_cores.len();
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "hw.physicalcpu"])
                .output()
            {
                if let Ok(cores_str) = String::from_utf8(output.stdout) {
                    if let Ok(cores) = cores_str.trim().parse::<usize>() {
                        return cores;
                    }
                }
            }
        }

        // Fallback: assume no hyperthreading
        self.detect_cpu_count()
    }

    /// Detect CPU model name
    fn detect_cpu_model(&self) -> String {
        #[cfg(target_os = "linux")]
        {
            if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
                for line in cpuinfo.lines() {
                    if line.starts_with("model name") {
                        if let Some(model) = line.split(':').nth(1) {
                            return model.trim().to_string();
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "machdep.cpu.brand_string"])
                .output()
            {
                if let Ok(model) = String::from_utf8(output.stdout) {
                    return model.trim().to_string();
                }
            }
        }

        "Unknown CPU".to_string()
    }

    /// Detect CPU base frequency
    fn detect_cpu_frequency(&self) -> Option<u32> {
        #[cfg(target_os = "linux")]
        {
            if let Ok(freq_str) = fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/base_frequency") {
                if let Ok(freq_khz) = freq_str.trim().parse::<u32>() {
                    return Some(freq_khz / 1000); // Convert to MHz
                }
            }
            
            // Fallback: try cpuinfo
            if let Ok(cpuinfo) = fs::read_to_string("/proc/cpuinfo") {
                for line in cpuinfo.lines() {
                    if line.starts_with("cpu MHz") {
                        if let Some(freq_str) = line.split(':').nth(1) {
                            if let Ok(freq_mhz) = freq_str.trim().parse::<f32>() {
                                return Some(freq_mhz as u32);
                            }
                        }
                    }
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            if let Ok(output) = Command::new("sysctl")
                .args(&["-n", "hw.cpufrequency"])
                .output()
            {
                if let Ok(freq_str) = String::from_utf8(output.stdout) {
                    if let Ok(freq_hz) = freq_str.trim().parse::<u64>() {
                        return Some((freq_hz / 1_000_000) as u32); // Convert to MHz
                    }
                }
            }
        }

        None
    }

    /// Measure memory bandwidth
    fn measure_memory_bandwidth(&self) -> Option<f64> {
        // This would require a sophisticated memory benchmark
        // For now, return estimates based on memory type
        // DDR4: ~25-30 GB/s, DDR5: ~40-50 GB/s
        Some(25000.0) // Conservative estimate in MB/s
    }

    /// Detect storage devices
    fn detect_storage_devices(&self) -> Vec<StorageDevice> {
        let mut devices = Vec::new();

        #[cfg(target_os = "linux")]
        {
            if let Ok(block_devices) = fs::read_dir("/sys/block") {
                for entry in block_devices.filter_map(|e| e.ok()) {
                    let device_name = entry.file_name().to_string_lossy().to_string();
                    
                    // Skip loop, ram, and other virtual devices
                    if device_name.starts_with("loop") 
                        || device_name.starts_with("ram") 
                        || device_name.starts_with("dm-") {
                        continue;
                    }

                    let device_type = if device_name.starts_with("nvme") {
                        StorageType::NVME
                    } else {
                        // Check rotational
                        let rotational_path = format!("/sys/block/{}/queue/rotational", device_name);
                        if let Ok(rotational) = fs::read_to_string(&rotational_path) {
                            if rotational.trim() == "0" {
                                StorageType::SSD
                            } else {
                                StorageType::HDD
                            }
                        } else {
                            StorageType::Unknown
                        }
                    };

                    // Try to get size
                    let size_gb = {
                        let size_path = format!("/sys/block/{}/size", device_name);
                        if let Ok(size_str) = fs::read_to_string(&size_path) {
                            if let Ok(size_sectors) = size_str.trim().parse::<u64>() {
                                Some(size_sectors * 512 / 1024 / 1024 / 1024) // Convert to GB
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };

                    devices.push(StorageDevice {
                        name: device_name.clone(),
                        device_type,
                        size_gb,
                        interface: if device_name.starts_with("nvme") { "NVMe".to_string() } else { "SATA".to_string() },
                        bandwidth_mb_per_sec: None, // Would require benchmarking
                    });
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            if let Ok(output) = Command::new("diskutil")
                .args(&["list", "-plist"])
                .output()
            {
                // Parse plist output to get disk information
                // Simplified implementation
                devices.push(StorageDevice {
                    name: "disk0".to_string(),
                    device_type: StorageType::SSD, // Most Macs have SSDs
                    size_gb: None,
                    interface: "APFS".to_string(),
                    bandwidth_mb_per_sec: None,
                });
            }
        }

        devices
    }

    /// Determine primary storage type from devices
    fn detect_primary_storage_type(&self, devices: &[StorageDevice]) -> StorageType {
        // Return the fastest storage type found
        for device in devices {
            match device.device_type {
                StorageType::NVME => return StorageType::NVME,
                _ => continue,
            }
        }
        
        for device in devices {
            match device.device_type {
                StorageType::SSD => return StorageType::SSD,
                _ => continue,
            }
        }

        devices.first()
            .map(|d| d.device_type)
            .unwrap_or(StorageType::Unknown)
    }

    /// Detect GPU devices
    fn detect_gpu_devices(&self) -> Vec<GpuDevice> {
        let mut devices = Vec::new();

        #[cfg(target_os = "linux")]
        {
            // Check for NVIDIA GPUs
            if let Ok(output) = std::process::Command::new("nvidia-smi")
                .args(&["--query-gpu=name,memory.total", "--format=csv,noheader,nounits"])
                .output()
            {
                if output.status.success() {
                    if let Ok(output_str) = String::from_utf8(output.stdout) {
                        for line in output_str.lines() {
                            let parts: Vec<&str> = line.split(',').collect();
                            if parts.len() >= 2 {
                                devices.push(GpuDevice {
                                    name: parts[0].trim().to_string(),
                                    vendor: "NVIDIA".to_string(),
                                    memory_mb: parts[1].trim().parse().ok(),
                                    compute_units: None,
                                    supports_compute: true,
                                });
                            }
                        }
                    }
                }
            }

            // Check for AMD GPUs (simplified)
            if devices.is_empty() {
                if std::path::Path::new("/dev/dri").exists() {
                    devices.push(GpuDevice {
                        name: "AMD GPU".to_string(),
                        vendor: "AMD".to_string(),
                        memory_mb: None,
                        compute_units: None,
                        supports_compute: true,
                    });
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            // Check for Metal support (indicates GPU)
            if let Ok(output) = Command::new("system_profiler")
                .args(&["SPDisplaysDataType", "-xml"])
                .output()
            {
                if output.status.success() {
                    // Simplified: assume modern Macs have capable GPUs
                    devices.push(GpuDevice {
                        name: "Apple GPU".to_string(),
                        vendor: "Apple".to_string(),
                        memory_mb: None,
                        compute_units: None,
                        supports_compute: true,
                    });
                }
            }
        }

        devices
    }

    /// Detect network interfaces
    fn detect_network_interfaces(&self) -> Vec<NetworkInterface> {
        let mut interfaces = Vec::new();

        #[cfg(target_os = "linux")]
        {
            if let Ok(net_devices) = fs::read_dir("/sys/class/net") {
                for entry in net_devices.filter_map(|e| e.ok()) {
                    let if_name = entry.file_name().to_string_lossy().to_string();
                    
                    // Determine interface type
                    let interface_type = if if_name == "lo" {
                        NetworkType::Loopback
                    } else if if_name.starts_with("eth") || if_name.starts_with("en") {
                        NetworkType::Ethernet
                    } else if if_name.starts_with("wlan") || if_name.starts_with("wl") {
                        NetworkType::WiFi
                    } else if if_name.starts_with("ib") {
                        NetworkType::InfiniBand
                    } else {
                        NetworkType::Unknown
                    };

                    // Check if interface is up
                    let is_up = {
                        let operstate_path = format!("/sys/class/net/{}/operstate", if_name);
                        if let Ok(state) = fs::read_to_string(&operstate_path) {
                            state.trim() == "up"
                        } else {
                            false
                        }
                    };

                    // Try to get speed
                    let speed_mbps = {
                        let speed_path = format!("/sys/class/net/{}/speed", if_name);
                        if let Ok(speed_str) = fs::read_to_string(&speed_path) {
                            speed_str.trim().parse().ok()
                        } else {
                            None
                        }
                    };

                    interfaces.push(NetworkInterface {
                        name: if_name,
                        interface_type,
                        speed_mbps,
                        is_up,
                    });
                }
            }
        }

        #[cfg(target_os = "macos")]
        {
            use std::process::Command;
            
            if let Ok(output) = Command::new("ifconfig")
                .args(&["-l"])
                .output()
            {
                if let Ok(if_list) = String::from_utf8(output.stdout) {
                    for if_name in if_list.split_whitespace() {
                        let interface_type = if if_name == "lo0" {
                            NetworkType::Loopback
                        } else if if_name.starts_with("en") {
                            NetworkType::Ethernet
                        } else if if_name.starts_with("wl") {
                            NetworkType::WiFi
                        } else {
                            NetworkType::Unknown
                        };

                        interfaces.push(NetworkInterface {
                            name: if_name.to_string(),
                            interface_type,
                            speed_mbps: None,
                            is_up: true, // Simplified
                        });
                    }
                }
            }
        }

        interfaces
    }

    /// Detect thermal state
    fn detect_thermal_state(&self) -> ThermalState {
        #[cfg(target_os = "linux")]
        {
            let cpu_temp_celsius = {
                // Try common thermal zones
                for i in 0..10 {
                    let temp_path = format!("/sys/class/thermal/thermal_zone{}/temp", i);
                    if let Ok(temp_str) = fs::read_to_string(&temp_path) {
                        if let Ok(temp_millicelsius) = temp_str.trim().parse::<i32>() {
                            return ThermalState {
                                cpu_temp_celsius: Some(temp_millicelsius as f32 / 1000.0),
                                thermal_throttling: temp_millicelsius > 80000, // > 80°C
                                cooling_available: true,
                            };
                        }
                    }
                }
                None
            };

            ThermalState {
                cpu_temp_celsius,
                thermal_throttling: false,
                cooling_available: true,
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            ThermalState {
                cpu_temp_celsius: None,
                thermal_throttling: false,
                cooling_available: true,
            }
        }
    }

    /// Detect power profile
    fn detect_power_profile(&self) -> PowerProfile {
        #[cfg(target_os = "linux")]
        {
            let cpu_governor = {
                if let Ok(governor) = fs::read_to_string("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor") {
                    Some(governor.trim().to_string())
                } else {
                    None
                }
            };

            let power_source = {
                // Check for battery
                if std::path::Path::new("/sys/class/power_supply/BAT0").exists() {
                    PowerSource::Battery
                } else {
                    PowerSource::AC
                }
            };

            PowerProfile {
                power_source,
                cpu_governor,
                max_performance_available: cpu_governor.as_deref() == Some("performance"),
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            PowerProfile {
                power_source: PowerSource::Unknown,
                cpu_governor: None,
                max_performance_available: true,
            }
        }
    }

    /// Get recommended configuration based on hardware
    pub fn recommend_config(&self, hw: &HardwareInfo) -> RecommendedConfig {
        let mut recommendations = Vec::new();
        
        // Cache size recommendation (use 25% of available memory)
        let recommended_cache_mb = hw.available_memory_mb / 4;
        recommendations.push(format!(
            "Set cache_size to {} MB (25% of available memory)",
            recommended_cache_mb
        ));

        // CPU optimization
        if hw.cpu_count >= 8 {
            recommendations.push("Enable prefetch_enabled with 4+ workers".to_string());
            recommendations.push("Use optimized_transactions and optimized_page_manager".to_string());
        }

        // Storage optimization
        match hw.storage_type {
            StorageType::NVME => {
                recommendations.push("Enable mmap with large size for NVMe".to_string());
                recommendations.push("Use async WAL mode for maximum throughput".to_string());
            }
            StorageType::SSD => {
                recommendations.push("Enable mmap for SSD storage".to_string());
                recommendations.push("Use async WAL mode".to_string());
            }
            StorageType::HDD => {
                recommendations.push("Disable compression for HDD".to_string());
                recommendations.push("Use larger write batches (10000+)".to_string());
            }
            _ => {}
        }

        // NUMA optimization
        if hw.numa_nodes > 1 {
            recommendations.push("Enable NUMA-aware memory allocation".to_string());
            recommendations.push("Pin threads to NUMA nodes".to_string());
        }

        // CPU features
        if hw.cpu_features.contains(&"AVX2".to_string()) {
            recommendations.push("SIMD optimizations available (AVX2)".to_string());
        }

        RecommendedConfig {
            cache_size_mb: recommended_cache_mb,
            prefetch_workers: (hw.cpu_count / 4).max(1),
            enable_huge_pages: hw.huge_pages_available,
            enable_numa_optimization: hw.numa_nodes > 1,
            recommendations,
        }
    }
}

/// Recommended configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecommendedConfig {
    pub cache_size_mb: u64,
    pub prefetch_workers: usize,
    pub enable_huge_pages: bool,
    pub enable_numa_optimization: bool,
    pub recommendations: Vec<String>,
}

impl std::fmt::Display for StorageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageType::SSD => write!(f, "SSD"),
            StorageType::NVME => write!(f, "NVMe"),
            StorageType::HDD => write!(f, "HDD"),
            StorageType::Unknown => write!(f, "Unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hardware_detection() {
        let detector = HardwareDetector::new();
        let hw_info = detector.detect().unwrap();
        
        assert!(hw_info.cpu_count > 0);
        assert!(hw_info.total_memory_mb > 0);
    }

    #[test]
    fn test_recommendations() {
        let detector = HardwareDetector::new();
        let hw_info = HardwareInfo {
            cpu_count: 16,
            physical_cores: 8,
            cpu_arch: "x86_64".to_string(),
            cpu_model: "Intel Core i7".to_string(),
            cpu_base_freq_mhz: Some(3200),
            cpu_features: vec!["AVX2".to_string()],
            total_memory_mb: 32768,
            available_memory_mb: 16384,
            memory_bandwidth_mb_per_sec: Some(25000.0),
            cache_sizes: CacheSizes {
                l1_data_kb: 32,
                l1_instruction_kb: 32,
                l2_kb: 256,
                l3_kb: 16384,
                cache_line_size: 64,
            },
            storage_devices: vec![StorageDevice {
                name: "nvme0n1".to_string(),
                device_type: StorageType::NVME,
                size_gb: Some(1000),
                interface: "NVMe".to_string(),
                bandwidth_mb_per_sec: Some(3500.0),
            }],
            storage_type: StorageType::NVME,
            storage_bandwidth_mb_per_sec: Some(3500.0),
            numa_nodes: 2,
            huge_pages_available: true,
            gpu_devices: Vec::new(),
            network_interfaces: Vec::new(),
            thermal_state: ThermalState {
                cpu_temp_celsius: Some(45.0),
                thermal_throttling: false,
                cooling_available: true,
            },
            power_profile: PowerProfile {
                power_source: PowerSource::AC,
                cpu_governor: Some("performance".to_string()),
                max_performance_available: true,
            },
        };

        let recommendations = detector.recommend_config(&hw_info);
        assert_eq!(recommendations.cache_size_mb, 4096); // 25% of 16GB
        assert!(recommendations.enable_numa_optimization);
    }
}