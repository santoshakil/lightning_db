import init, { 
    LightningDB, 
    Config, 
    string_to_bytes, 
    bytes_to_string,
    benchmark_writes,
    benchmark_reads
} from 'lightning_db_wasm';

let db;
let currentTxId = null;

// Initialize the WASM module and database
async function initApp() {
    try {
        await init();
        
        const config = new Config();
        db = new LightningDB(config);
        
        updateStats();
        log('basicOutput', '✅ Lightning DB initialized successfully!', 'success');
        
        // Setup file import handler
        document.getElementById('importFile').addEventListener('change', handleFileImport);
        
    } catch (error) {
        log('basicOutput', `❌ Initialization failed: ${error}`, 'error');
    }
}

// Utility functions
function log(elementId, message, type = 'info') {
    const element = document.getElementById(elementId);
    const timestamp = new Date().toLocaleTimeString();
    const prefix = type === 'error' ? '❌' : type === 'success' ? '✅' : 'ℹ️';
    element.innerHTML += `[${timestamp}] ${prefix} ${message}\n`;
    element.scrollTop = element.scrollHeight;
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatNumber(num) {
    return new Intl.NumberFormat().format(Math.round(num));
}

// Stats updating
async function updateStats() {
    try {
        const stats = db.get_stats();
        document.getElementById('totalKeys').textContent = formatNumber(stats.total_keys);
        document.getElementById('totalSize').textContent = formatBytes(stats.total_size);
        document.getElementById('cacheHitRate').textContent = Math.round(stats.cache_hit_rate * 100) + '%';
        document.getElementById('compressionRatio').textContent = Math.round(stats.compression_ratio * 100) + '%';
    } catch (error) {
        console.error('Failed to update stats:', error);
    }
}

// Basic operations
window.putData = async function() {
    try {
        const key = document.getElementById('putKey').value;
        const value = document.getElementById('putValue').value;
        
        if (!key) {
            log('basicOutput', 'Please enter a key', 'error');
            return;
        }
        
        await db.put(key, string_to_bytes(value));
        log('basicOutput', `✅ Put: "${key}" = "${value}"`);
        updateStats();
    } catch (error) {
        log('basicOutput', `❌ Put failed: ${error}`, 'error');
    }
};

window.getData = async function() {
    try {
        const key = document.getElementById('putKey').value;
        
        if (!key) {
            log('basicOutput', 'Please enter a key', 'error');
            return;
        }
        
        const result = await db.get(key);
        if (result) {
            const value = bytes_to_string(result);
            log('basicOutput', `✅ Get: "${key}" = "${value}"`);
        } else {
            log('basicOutput', `🔍 Key "${key}" not found`);
        }
    } catch (error) {
        log('basicOutput', `❌ Get failed: ${error}`, 'error');
    }
};

window.deleteData = async function() {
    try {
        const key = document.getElementById('putKey').value;
        
        if (!key) {
            log('basicOutput', 'Please enter a key', 'error');
            return;
        }
        
        await db.delete(key);
        log('basicOutput', `🗑️ Deleted: "${key}"`);
        updateStats();
    } catch (error) {
        log('basicOutput', `❌ Delete failed: ${error}`, 'error');
    }
};

window.clearAll = async function() {
    try {
        await db.clear();
        log('basicOutput', '🧹 All data cleared');
        updateStats();
    } catch (error) {
        log('basicOutput', `❌ Clear failed: ${error}`, 'error');
    }
};

// Transaction operations
window.startTransaction = async function() {
    try {
        currentTxId = await db.begin_transaction();
        log('transactionOutput', `🚀 Transaction ${currentTxId} started`);
        
        document.getElementById('txPutBtn').disabled = false;
        document.getElementById('commitBtn').disabled = false;
        document.getElementById('rollbackBtn').disabled = false;
    } catch (error) {
        log('transactionOutput', `❌ Failed to start transaction: ${error}`, 'error');
    }
};

window.txPut = async function() {
    if (!currentTxId) {
        log('transactionOutput', 'No active transaction', 'error');
        return;
    }
    
    try {
        const key = document.getElementById('txKey').value;
        const value = document.getElementById('txValue').value;
        
        await db.tx_put(currentTxId, key, string_to_bytes(value));
        log('transactionOutput', `📝 TX Put: "${key}" = "${value}"`);
    } catch (error) {
        log('transactionOutput', `❌ TX Put failed: ${error}`, 'error');
    }
};

window.commitTransaction = async function() {
    if (!currentTxId) {
        log('transactionOutput', 'No active transaction', 'error');
        return;
    }
    
    try {
        await db.commit_transaction(currentTxId);
        log('transactionOutput', `✅ Transaction ${currentTxId} committed`);
        currentTxId = null;
        
        document.getElementById('txPutBtn').disabled = true;
        document.getElementById('commitBtn').disabled = true;
        document.getElementById('rollbackBtn').disabled = true;
        updateStats();
    } catch (error) {
        log('transactionOutput', `❌ Commit failed: ${error}`, 'error');
    }
};

window.rollbackTransaction = async function() {
    if (!currentTxId) {
        log('transactionOutput', 'No active transaction', 'error');
        return;
    }
    
    try {
        await db.rollback_transaction(currentTxId);
        log('transactionOutput', `↩️ Transaction ${currentTxId} rolled back`);
        currentTxId = null;
        
        document.getElementById('txPutBtn').disabled = true;
        document.getElementById('commitBtn').disabled = true;
        document.getElementById('rollbackBtn').disabled = true;
    } catch (error) {
        log('transactionOutput', `❌ Rollback failed: ${error}`, 'error');
    }
};

// Benchmark operations
window.benchmarkWrites = async function() {
    const count = parseInt(document.getElementById('benchmarkCount').value);
    const resultsDiv = document.getElementById('benchmarkResults');
    
    try {
        const start = performance.now();
        const result = benchmark_writes(db, count);
        const end = performance.now();
        
        resultsDiv.innerHTML += `
            <div class="stat-card">
                <div class="stat-value">${formatNumber(result.ops_per_sec)}</div>
                <div class="stat-label">Writes/sec</div>
                <div style="margin-top: 8px; font-size: 12px; color: #666;">
                    ${formatNumber(count)} operations in ${result.duration_ms.toFixed(1)}ms<br>
                    Avg latency: ${result.avg_latency_us.toFixed(2)}μs
                </div>
            </div>
        `;
        updateStats();
    } catch (error) {
        resultsDiv.innerHTML += `<div class="error">Write benchmark failed: ${error}</div>`;
    }
};

window.benchmarkReads = async function() {
    const count = parseInt(document.getElementById('benchmarkCount').value);
    const resultsDiv = document.getElementById('benchmarkResults');
    
    try {
        // First populate with data
        for (let i = 0; i < count; i++) {
            await db.put(`bench_key_${i}`, string_to_bytes(`bench_value_${i}`));
        }
        
        const result = benchmark_reads(db, count);
        
        resultsDiv.innerHTML += `
            <div class="stat-card">
                <div class="stat-value">${formatNumber(result.ops_per_sec)}</div>
                <div class="stat-label">Reads/sec</div>
                <div style="margin-top: 8px; font-size: 12px; color: #666;">
                    ${formatNumber(count)} operations in ${result.duration_ms.toFixed(1)}ms<br>
                    Avg latency: ${result.avg_latency_us.toFixed(2)}μs
                </div>
            </div>
        `;
    } catch (error) {
        resultsDiv.innerHTML += `<div class="error">Read benchmark failed: ${error}</div>`;
    }
};

window.benchmarkMixed = async function() {
    const count = parseInt(document.getElementById('benchmarkCount').value);
    const resultsDiv = document.getElementById('benchmarkResults');
    
    try {
        const start = performance.now();
        
        // Mixed workload: 70% reads, 30% writes
        for (let i = 0; i < count; i++) {
            if (Math.random() < 0.7) {
                // Read operation
                const key = `key_${Math.floor(Math.random() * count)}`;
                await db.get(key);
            } else {
                // Write operation
                const key = `key_${i}`;
                const value = `value_${i}`;
                await db.put(key, string_to_bytes(value));
            }
        }
        
        const end = performance.now();
        const duration = end - start;
        const opsPerSec = (count / duration) * 1000;
        const avgLatency = (duration * 1000) / count;
        
        resultsDiv.innerHTML += `
            <div class="stat-card">
                <div class="stat-value">${formatNumber(opsPerSec)}</div>
                <div class="stat-label">Mixed Ops/sec</div>
                <div style="margin-top: 8px; font-size: 12px; color: #666;">
                    ${formatNumber(count)} operations in ${duration.toFixed(1)}ms<br>
                    Avg latency: ${avgLatency.toFixed(2)}μs<br>
                    70% reads, 30% writes
                </div>
            </div>
        `;
        updateStats();
    } catch (error) {
        resultsDiv.innerHTML += `<div class="error">Mixed benchmark failed: ${error}</div>`;
    }
};

// Data explorer
window.listKeys = async function() {
    try {
        const prefix = document.getElementById('prefixFilter').value;
        const keys = await db.keys_with_prefix(prefix);
        
        const output = document.getElementById('explorerOutput');
        output.innerHTML = `Found ${keys.length} keys:\n`;
        
        for (const key of keys.slice(0, 100)) { // Limit to 100 for display
            try {
                const value = await db.get(key);
                const valueStr = value ? bytes_to_string(value) : 'null';
                const truncated = valueStr.length > 50 ? valueStr.substring(0, 50) + '...' : valueStr;
                output.innerHTML += `📄 ${key}: ${truncated}\n`;
            } catch (e) {
                output.innerHTML += `📄 ${key}: <error reading value>\n`;
            }
        }
        
        if (keys.length > 100) {
            output.innerHTML += `... and ${keys.length - 100} more keys\n`;
        }
    } catch (error) {
        log('explorerOutput', `❌ List keys failed: ${error}`, 'error');
    }
};

window.exportData = async function() {
    try {
        const keys = await db.keys_with_prefix('');
        const data = {};
        
        for (const key of keys) {
            const value = await db.get(key);
            data[key] = value ? bytes_to_string(value) : null;
        }
        
        const json = JSON.stringify(data, null, 2);
        const blob = new Blob([json], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        
        const a = document.createElement('a');
        a.href = url;
        a.download = 'lightning-db-export.json';
        a.click();
        
        URL.revokeObjectURL(url);
        log('explorerOutput', `✅ Exported ${keys.length} keys to JSON`);
    } catch (error) {
        log('explorerOutput', `❌ Export failed: ${error}`, 'error');
    }
};

async function handleFileImport(event) {
    const file = event.target.files[0];
    if (!file) return;
    
    try {
        const text = await file.text();
        const data = JSON.parse(text);
        
        let imported = 0;
        for (const [key, value] of Object.entries(data)) {
            if (value !== null) {
                await db.put(key, string_to_bytes(value));
                imported++;
            }
        }
        
        log('explorerOutput', `✅ Imported ${imported} keys from ${file.name}`);
        updateStats();
    } catch (error) {
        log('explorerOutput', `❌ Import failed: ${error}`, 'error');
    }
}

// Initialize the app
initApp();