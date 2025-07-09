// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "LightningDBExample",
    platforms: [
        .iOS(.v13),
        .macOS(.v10_15)
    ],
    products: [
        .library(
            name: "LightningDBSwift",
            targets: ["LightningDBSwift"]),
    ],
    dependencies: [],
    targets: [
        .target(
            name: "LightningDBSwift",
            dependencies: ["LightningDBFFI"],
            path: "Sources/LightningDBSwift"
        ),
        .binaryTarget(
            name: "LightningDBFFI",
            path: "../../../lightning_db_ffi/LightningDB.xcframework"
        ),
        .executableTarget(
            name: "LightningDBExample",
            dependencies: ["LightningDBSwift"],
            path: "Sources/Example"
        ),
        .testTarget(
            name: "LightningDBTests",
            dependencies: ["LightningDBSwift"],
            path: "Tests"
        ),
    ]
)