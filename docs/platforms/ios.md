# iOS Integration Guide

## Overview

Lightning DB provides native iOS integration through XCFramework and Swift Package Manager. This guide covers everything you need to integrate Lightning DB into your iOS applications.

## Prerequisites

- iOS 13.0+ / macOS 10.15+
- Xcode 12.0+
- Swift 5.3+

## Installation

### Method 1: Swift Package Manager (Recommended)

Add to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/santoshakil/lightning_db.git", from: "0.1.0")
]
```

Or in Xcode:
1. File → Add Package Dependencies
2. Enter: `https://github.com/santoshakil/lightning_db.git`
3. Select version and add to target

### Method 2: XCFramework

1. Download `LightningDB.xcframework` from releases
2. Drag to your Xcode project
3. Add to "Frameworks, Libraries, and Embedded Content"

### Method 3: CocoaPods

Add to your `Podfile`:

```ruby
pod 'LightningDB', '~> 0.1'
```

Run:
```bash
pod install
```

## Quick Start

### Basic Usage

```swift
import LightningDB

class ViewController: UIViewController {
    private var db: LightningDB!
    
    override func viewDidLoad() {
        super.viewDidLoad()
        
        // Initialize database
        let config = LightningDB.Config(
            path: documentsPath + "/mydb",
            cacheSize: 100 * 1024 * 1024, // 100MB
            compressionEnabled: true,
            syncMode: .async
        )
        
        do {
            db = try LightningDB(config: config)
            
            // Basic operations
            try db.put(key: "greeting", value: "Hello, Lightning DB!".data(using: .utf8)!)
            
            if let data = try db.get(key: "greeting"),
               let message = String(data: data, encoding: .utf8) {
                print("Retrieved: \(message)")
            }
        } catch {
            print("Database error: \(error)")
        }
    }
    
    private var documentsPath: String {
        NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)[0]
    }
}
```

### SwiftUI Integration

```swift
import SwiftUI
import LightningDB

@main
struct MyApp: App {
    @StateObject private var databaseManager = DatabaseManager()
    
    var body: some Scene {
        WindowGroup {
            ContentView()
                .environmentObject(databaseManager)
        }
    }
}

class DatabaseManager: ObservableObject {
    private let db: LightningDB
    
    init() {
        let config = LightningDB.Config(
            path: FileManager.default.urls(for: .documentDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("app.db").path,
            cacheSize: 50 * 1024 * 1024
        )
        
        do {
            db = try LightningDB(config: config)
        } catch {
            fatalError("Failed to initialize database: \(error)")
        }
    }
    
    func save<T: Codable>(_ object: T, forKey key: String) throws {
        let data = try JSONEncoder().encode(object)
        try db.put(key: key, value: data)
    }
    
    func load<T: Codable>(_ type: T.Type, forKey key: String) throws -> T? {
        guard let data = try db.get(key: key) else { return nil }
        return try JSONDecoder().decode(type, from: data)
    }
}
```

## Advanced Usage

### Codable Support

```swift
struct User: Codable {
    let id: String
    let name: String
    let email: String
    let createdAt: Date
}

extension LightningDB {
    func put<T: Codable>(key: String, value: T) throws {
        let data = try JSONEncoder().encode(value)
        try put(key: key, value: data)
    }
    
    func get<T: Codable>(key: String, type: T.Type) throws -> T? {
        guard let data = try get(key: key) else { return nil }
        return try JSONDecoder().decode(type, from: data)
    }
}

// Usage
let user = User(id: "123", name: "John", email: "john@example.com", createdAt: Date())
try db.put(key: "user:123", value: user)

let retrievedUser: User? = try db.get(key: "user:123", type: User.self)
```

### Async/Await Support

```swift
extension LightningDB {
    func putAsync(key: String, value: Data) async throws {
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try self.put(key: key, value: value)
                    continuation.resume()
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }
    
    func getAsync(key: String) async throws -> Data? {
        return try await withCheckedThrowingContinuation { continuation in
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    let result = try self.get(key: key)
                    continuation.resume(returning: result)
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }
}

// Usage
Task {
    try await db.putAsync(key: "async_key", value: "async_value".data(using: .utf8)!)
    let value = try await db.getAsync(key: "async_key")
}
```

### Transaction Management

```swift
class OrderService {
    private let db: LightningDB
    
    init(db: LightningDB) {
        self.db = db
    }
    
    func processOrder(_ order: Order, items: [OrderItem]) throws {
        let transaction = try db.beginTransaction()
        
        do {
            // Save order
            try transaction.put(key: "order:\(order.id)", value: order)
            
            // Save items
            for item in items {
                try transaction.put(key: "item:\(item.id)", value: item)
            }
            
            // Update inventory
            for item in items {
                let currentStock = try getStock(productId: item.productId)
                let newStock = currentStock - item.quantity
                try transaction.put(key: "stock:\(item.productId)", value: newStock)
            }
            
            try transaction.commit()
        } catch {
            try transaction.rollback()
            throw error
        }
    }
}
```

### Core Data Migration

```swift
class CoreDataMigration {
    private let coreDataContext: NSManagedObjectContext
    private let lightningDB: LightningDB
    
    init(context: NSManagedObjectContext, db: LightningDB) {
        self.coreDataContext = context
        self.lightningDB = db
    }
    
    func migrate() throws {
        let fetchRequest: NSFetchRequest<NSManagedObject> = NSFetchRequest(entityName: "User")
        let users = try coreDataContext.fetch(fetchRequest)
        
        let transaction = try lightningDB.beginTransaction()
        
        do {
            for user in users {
                let userData = User(
                    id: user.value(forKey: "id") as! String,
                    name: user.value(forKey: "name") as! String,
                    email: user.value(forKey: "email") as! String,
                    createdAt: user.value(forKey: "createdAt") as! Date
                )
                
                try transaction.put(key: "user:\(userData.id)", value: userData)
            }
            
            try transaction.commit()
        } catch {
            try transaction.rollback()
            throw error
        }
    }
}
```

## Performance Optimization

### Configuration Tuning

```swift
let config = LightningDB.Config(
    path: dbPath,
    cacheSize: 200 * 1024 * 1024, // 200MB for large datasets
    compressionEnabled: true,
    syncMode: .async, // Better performance
    walBufferSize: 32 * 1024 * 1024, // 32MB WAL buffer
    maxConcurrentTransactions: 50
)
```

### Batch Operations

```swift
class BatchProcessor {
    private let db: LightningDB
    
    init(db: LightningDB) {
        self.db = db
    }
    
    func processBatch<T: Codable>(_ items: [T], keyGenerator: (T) -> String, batchSize: Int = 1000) throws {
        let batches = items.chunked(into: batchSize)
        
        for batch in batches {
            let transaction = try db.beginTransaction()
            
            do {
                for item in batch {
                    let key = keyGenerator(item)
                    try transaction.put(key: key, value: item)
                }
                try transaction.commit()
            } catch {
                try transaction.rollback()
                throw error
            }
        }
    }
}

extension Array {
    func chunked(into size: Int) -> [[Element]] {
        return stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
```

## Background Processing

### Background App Refresh

```swift
class DatabaseSyncManager {
    private let db: LightningDB
    
    init(db: LightningDB) {
        self.db = db
    }
    
    func scheduleBackgroundSync() {
        let identifier = "com.yourapp.database-sync"
        let request = BGAppRefreshTaskRequest(identifier: identifier)
        request.earliestBeginDate = Date(timeIntervalSinceNow: 15 * 60) // 15 minutes
        
        try? BGTaskScheduler.shared.submit(request)
    }
    
    func handleBackgroundSync(task: BGAppRefreshTask) {
        task.expirationHandler = {
            task.setTaskCompleted(success: false)
        }
        
        Task {
            do {
                // Perform maintenance operations
                try await db.checkpoint()
                try await db.compact()
                
                // Schedule next sync
                scheduleBackgroundSync()
                
                task.setTaskCompleted(success: true)
            } catch {
                task.setTaskCompleted(success: false)
            }
        }
    }
}

// In AppDelegate
func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
    BGTaskScheduler.shared.register(forTaskWithIdentifier: "com.yourapp.database-sync", using: nil) { task in
        self.databaseSyncManager.handleBackgroundSync(task: task as! BGAppRefreshTask)
    }
    return true
}
```

## Data Persistence Patterns

### Repository Pattern

```swift
protocol Repository {
    associatedtype Entity
    
    func save(_ entity: Entity) throws
    func find(id: String) throws -> Entity?
    func findAll() throws -> [Entity]
    func delete(id: String) throws
}

class UserRepository: Repository {
    private let db: LightningDB
    private let keyPrefix = "user:"
    
    init(db: LightningDB) {
        self.db = db
    }
    
    func save(_ user: User) throws {
        try db.put(key: keyPrefix + user.id, value: user)
    }
    
    func find(id: String) throws -> User? {
        return try db.get(key: keyPrefix + id, type: User.self)
    }
    
    func findAll() throws -> [User] {
        let results = try db.range(start: keyPrefix, end: keyPrefix + "~")
        return try results.compactMap { (_, data) in
            try JSONDecoder().decode(User.self, from: data)
        }
    }
    
    func delete(id: String) throws {
        try db.delete(key: keyPrefix + id)
    }
}
```

### Reactive Extensions (Combine)

```swift
import Combine

extension LightningDB {
    func putPublisher(key: String, value: Data) -> AnyPublisher<Void, Error> {
        Future { promise in
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    try self.put(key: key, value: value)
                    promise(.success(()))
                } catch {
                    promise(.failure(error))
                }
            }
        }
        .eraseToAnyPublisher()
    }
    
    func getPublisher(key: String) -> AnyPublisher<Data?, Error> {
        Future { promise in
            DispatchQueue.global(qos: .userInitiated).async {
                do {
                    let result = try self.get(key: key)
                    promise(.success(result))
                } catch {
                    promise(.failure(error))
                }
            }
        }
        .eraseToAnyPublisher()
    }
}

// Usage
class UserService: ObservableObject {
    @Published var users: [User] = []
    private let db: LightningDB
    private var cancellables = Set<AnyCancellable>()
    
    init(db: LightningDB) {
        self.db = db
    }
    
    func saveUser(_ user: User) {
        db.putPublisher(key: "user:\(user.id)", value: try! JSONEncoder().encode(user))
            .sink(
                receiveCompletion: { completion in
                    if case .failure(let error) = completion {
                        print("Save failed: \(error)")
                    }
                },
                receiveValue: { _ in
                    print("User saved successfully")
                }
            )
            .store(in: &cancellables)
    }
}
```

## Testing

### Unit Tests

```swift
import XCTest
@testable import YourApp

class DatabaseTests: XCTestCase {
    private var db: LightningDB!
    private var tempURL: URL!
    
    override func setUp() {
        super.setUp()
        
        tempURL = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        
        let config = LightningDB.Config(path: tempURL.path)
        db = try! LightningDB(config: config)
    }
    
    override func tearDown() {
        db = nil
        try? FileManager.default.removeItem(at: tempURL)
        super.tearDown()
    }
    
    func testBasicOperations() throws {
        // Test put/get
        let testData = "test_value".data(using: .utf8)!
        try db.put(key: "test_key", value: testData)
        
        let retrieved = try db.get(key: "test_key")
        XCTAssertEqual(retrieved, testData)
        
        // Test delete
        try db.delete(key: "test_key")
        let afterDelete = try db.get(key: "test_key")
        XCTAssertNil(afterDelete)
    }
    
    func testTransactions() throws {
        let transaction = try db.beginTransaction()
        
        let testData = "tx_value".data(using: .utf8)!
        try transaction.put(key: "tx_key", value: testData)
        
        // Value not visible before commit
        XCTAssertNil(try db.get(key: "tx_key"))
        
        try transaction.commit()
        
        // Value visible after commit
        let committed = try db.get(key: "tx_key")
        XCTAssertEqual(committed, testData)
    }
    
    func testCodableSupport() throws {
        let user = User(id: "123", name: "Test User", email: "test@example.com", createdAt: Date())
        
        try db.put(key: "user:123", value: user)
        let retrieved: User? = try db.get(key: "user:123", type: User.self)
        
        XCTAssertEqual(retrieved?.id, user.id)
        XCTAssertEqual(retrieved?.name, user.name)
        XCTAssertEqual(retrieved?.email, user.email)
    }
}
```

### Performance Tests

```swift
func testPerformance() throws {
    measure {
        let transaction = try! db.beginTransaction()
        
        for i in 0..<10000 {
            let key = "perf_key_\(i)"
            let value = "perf_value_\(i)".data(using: .utf8)!
            try! transaction.put(key: key, value: value)
        }
        
        try! transaction.commit()
    }
}
```

## Best Practices

### 1. Resource Management

```swift
class DatabaseManager {
    private let db: LightningDB
    
    init(path: String) throws {
        let config = LightningDB.Config(path: path)
        db = try LightningDB(config: config)
    }
    
    deinit {
        // LightningDB automatically closes when deallocated
    }
}
```

### 2. Error Handling

```swift
enum DatabaseError: Error, LocalizedError {
    case initializationFailed
    case operationFailed(String)
    case transactionFailed(String)
    
    var errorDescription: String? {
        switch self {
        case .initializationFailed:
            return "Failed to initialize database"
        case .operationFailed(let message):
            return "Database operation failed: \(message)"
        case .transactionFailed(let message):
            return "Transaction failed: \(message)"
        }
    }
}

class SafeDatabase {
    private let db: LightningDB
    
    init(path: String) throws {
        do {
            let config = LightningDB.Config(path: path)
            db = try LightningDB(config: config)
        } catch {
            throw DatabaseError.initializationFailed
        }
    }
    
    func safeGet(key: String) -> Data? {
        do {
            return try db.get(key: key)
        } catch {
            print("Error getting key \(key): \(error)")
            return nil
        }
    }
}
```

### 3. Thread Safety

```swift
class ThreadSafeDatabase {
    private let db: LightningDB
    private let queue = DispatchQueue(label: "database.queue", qos: .userInitiated)
    
    init(db: LightningDB) {
        self.db = db
    }
    
    func put(key: String, value: Data, completion: @escaping (Result<Void, Error>) -> Void) {
        queue.async {
            do {
                try self.db.put(key: key, value: value)
                DispatchQueue.main.async {
                    completion(.success(()))
                }
            } catch {
                DispatchQueue.main.async {
                    completion(.failure(error))
                }
            }
        }
    }
}
```

## Troubleshooting

### Common Issues

1. **Framework Not Found**
   - Ensure XCFramework is properly embedded
   - Check deployment target compatibility
   - Verify code signing settings

2. **Performance Issues**
   - Increase cache size for large datasets
   - Use async sync mode for better performance
   - Implement proper batching for bulk operations

3. **Memory Issues**
   - Monitor database size growth
   - Implement regular compaction
   - Use appropriate cache sizes

### Debug Configuration

```swift
#if DEBUG
LightningDB.setLogLevel(.debug)
#endif
```

This comprehensive guide should help you integrate Lightning DB into your iOS applications effectively. For more detailed API documentation, see the [API Reference](../api/README.md).