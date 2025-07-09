import Foundation

/// Lightning DB Swift wrapper
public class LightningDB {
    private let dbPointer: OpaquePointer
    
    /// Configuration for Lightning DB
    public struct Config {
        public let path: String
        public let cacheSize: UInt64
        public let compressionEnabled: Bool
        public let syncMode: SyncMode
        
        public enum SyncMode: String {
            case sync = "sync"
            case async = "async"
            case periodic = "periodic"
        }
        
        public init(
            path: String,
            cacheSize: UInt64 = 100 * 1024 * 1024, // 100MB default
            compressionEnabled: Bool = true,
            syncMode: SyncMode = .async
        ) {
            self.path = path
            self.cacheSize = cacheSize
            self.compressionEnabled = compressionEnabled
            self.syncMode = syncMode
        }
    }
    
    /// Initialize Lightning DB with configuration
    public init(config: Config) throws {
        // Initialize FFI
        lightning_db_init()
        
        // Create database
        var error: UnsafeMutablePointer<CChar>?
        let db = lightning_db_create(
            config.path,
            config.cacheSize,
            config.compressionEnabled ? 1 : 0,
            config.syncMode.rawValue,
            &error
        )
        
        if let error = error {
            let errorString = String(cString: error)
            lightning_db_free_string(error)
            throw LightningDBError.initializationFailed(errorString)
        }
        
        guard let db = db else {
            throw LightningDBError.initializationFailed("Failed to create database")
        }
        
        self.dbPointer = db
    }
    
    deinit {
        lightning_db_close(dbPointer)
    }
    
    /// Put a key-value pair
    public func put(key: String, value: Data) throws {
        var error: UnsafeMutablePointer<CChar>?
        
        let success = value.withUnsafeBytes { valueBytes in
            lightning_db_put(
                dbPointer,
                key,
                valueBytes.bindMemory(to: UInt8.self).baseAddress,
                UInt(value.count),
                &error
            )
        }
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.operationFailed(errorString)
            }
            throw LightningDBError.operationFailed("Put operation failed")
        }
    }
    
    /// Get a value by key
    public func get(key: String) throws -> Data? {
        var error: UnsafeMutablePointer<CChar>?
        var dataPtr: UnsafeMutablePointer<UInt8>?
        var dataLen: UInt = 0
        
        let found = lightning_db_get(
            dbPointer,
            key,
            &dataPtr,
            &dataLen,
            &error
        )
        
        if let error = error {
            let errorString = String(cString: error)
            lightning_db_free_string(error)
            throw LightningDBError.operationFailed(errorString)
        }
        
        guard found, let dataPtr = dataPtr else {
            return nil
        }
        
        let data = Data(bytes: dataPtr, count: Int(dataLen))
        lightning_db_free_data(dataPtr)
        
        return data
    }
    
    /// Delete a key
    public func delete(key: String) throws {
        var error: UnsafeMutablePointer<CChar>?
        
        let success = lightning_db_delete(dbPointer, key, &error)
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.operationFailed(errorString)
            }
            throw LightningDBError.operationFailed("Delete operation failed")
        }
    }
    
    /// Begin a transaction
    public func beginTransaction() throws -> Transaction {
        var error: UnsafeMutablePointer<CChar>?
        let txId = lightning_db_begin_transaction(dbPointer, &error)
        
        if let error = error {
            let errorString = String(cString: error)
            lightning_db_free_string(error)
            throw LightningDBError.transactionFailed(errorString)
        }
        
        return Transaction(db: self, id: txId)
    }
    
    /// Perform a range query
    public func range(start: String, end: String, limit: Int = 100) throws -> [(key: String, value: Data)] {
        var error: UnsafeMutablePointer<CChar>?
        var keys: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?
        var values: UnsafeMutablePointer<UnsafeMutablePointer<UInt8>?>?
        var valueLengths: UnsafeMutablePointer<UInt>?
        var count: UInt = 0
        
        let success = lightning_db_range(
            dbPointer,
            start,
            end,
            UInt(limit),
            &keys,
            &values,
            &valueLengths,
            &count,
            &error
        )
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.operationFailed(errorString)
            }
            throw LightningDBError.operationFailed("Range query failed")
        }
        
        var results: [(String, Data)] = []
        
        if let keys = keys, let values = values, let valueLengths = valueLengths {
            for i in 0..<Int(count) {
                if let keyPtr = keys[i], let valuePtr = values[i] {
                    let key = String(cString: keyPtr)
                    let valueLen = Int(valueLengths[i])
                    let value = Data(bytes: valuePtr, count: valueLen)
                    results.append((key, value))
                    
                    lightning_db_free_string(keyPtr)
                    lightning_db_free_data(valuePtr)
                }
            }
            
            // Free arrays
            free(keys)
            free(values)
            free(valueLengths)
        }
        
        return results
    }
    
    /// Get database statistics
    public func getStats() throws -> Stats {
        var error: UnsafeMutablePointer<CChar>?
        var stats = lightning_db_stats()
        
        let success = lightning_db_get_stats(dbPointer, &stats, &error)
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.operationFailed(errorString)
            }
            throw LightningDBError.operationFailed("Failed to get stats")
        }
        
        return Stats(
            totalKeys: Int(stats.total_keys),
            totalSize: Int(stats.total_size),
            cacheHitRate: Double(stats.cache_hit_rate),
            compressionRatio: Double(stats.compression_ratio)
        )
    }
}

/// Transaction handle
public class Transaction {
    private weak var db: LightningDB?
    private let id: UInt64
    private var committed = false
    
    init(db: LightningDB, id: UInt64) {
        self.db = db
        self.id = id
    }
    
    deinit {
        if !committed {
            _ = try? rollback()
        }
    }
    
    public func put(key: String, value: Data) throws {
        guard let db = db, !committed else {
            throw LightningDBError.transactionClosed
        }
        
        var error: UnsafeMutablePointer<CChar>?
        
        let success = value.withUnsafeBytes { valueBytes in
            lightning_db_tx_put(
                db.dbPointer,
                id,
                key,
                valueBytes.bindMemory(to: UInt8.self).baseAddress,
                UInt(value.count),
                &error
            )
        }
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.transactionFailed(errorString)
            }
            throw LightningDBError.transactionFailed("Transaction put failed")
        }
    }
    
    public func commit() throws {
        guard let db = db, !committed else {
            throw LightningDBError.transactionClosed
        }
        
        var error: UnsafeMutablePointer<CChar>?
        let success = lightning_db_commit_transaction(db.dbPointer, id, &error)
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.transactionFailed(errorString)
            }
            throw LightningDBError.transactionFailed("Transaction commit failed")
        }
        
        committed = true
    }
    
    public func rollback() throws {
        guard let db = db, !committed else {
            throw LightningDBError.transactionClosed
        }
        
        var error: UnsafeMutablePointer<CChar>?
        let success = lightning_db_rollback_transaction(db.dbPointer, id, &error)
        
        if !success {
            if let error = error {
                let errorString = String(cString: error)
                lightning_db_free_string(error)
                throw LightningDBError.transactionFailed(errorString)
            }
            throw LightningDBError.transactionFailed("Transaction rollback failed")
        }
        
        committed = true
    }
}

/// Database statistics
public struct Stats {
    public let totalKeys: Int
    public let totalSize: Int
    public let cacheHitRate: Double
    public let compressionRatio: Double
}

/// Lightning DB errors
public enum LightningDBError: LocalizedError {
    case initializationFailed(String)
    case operationFailed(String)
    case transactionFailed(String)
    case transactionClosed
    
    public var errorDescription: String? {
        switch self {
        case .initializationFailed(let msg):
            return "Database initialization failed: \(msg)"
        case .operationFailed(let msg):
            return "Database operation failed: \(msg)"
        case .transactionFailed(let msg):
            return "Transaction failed: \(msg)"
        case .transactionClosed:
            return "Transaction is already closed"
        }
    }
}

// MARK: - Convenience Extensions

extension LightningDB {
    /// Put a string value
    public func put(key: String, value: String) throws {
        guard let data = value.data(using: .utf8) else {
            throw LightningDBError.operationFailed("Invalid string encoding")
        }
        try put(key: key, value: data)
    }
    
    /// Get a string value
    public func getString(key: String) throws -> String? {
        guard let data = try get(key: key) else {
            return nil
        }
        return String(data: data, encoding: .utf8)
    }
    
    /// Put a codable value
    public func put<T: Encodable>(key: String, value: T) throws {
        let encoder = JSONEncoder()
        let data = try encoder.encode(value)
        try put(key: key, value: data)
    }
    
    /// Get a codable value
    public func get<T: Decodable>(key: String, type: T.Type) throws -> T? {
        guard let data = try get(key: key) else {
            return nil
        }
        let decoder = JSONDecoder()
        return try decoder.decode(type, from: data)
    }
}