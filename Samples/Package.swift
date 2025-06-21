// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "PulsarClientSamples",
    platforms: [
        .macOS(.v13),
        .iOS(.v16),
        .watchOS(.v9),
        .tvOS(.v16),
        .visionOS(.v1)
    ],
    products: [
        // Executables for each sample
        .executable(name: "ProducerSample", targets: ["ProducerSample"]),
        .executable(name: "ConsumerSample", targets: ["ConsumerSample"]),
        .executable(name: "ReaderSample", targets: ["ReaderSample"]),
        .executable(name: "BatchProcessingSample", targets: ["BatchProcessingSample"]),
    ],
    dependencies: [
        // Reference to our PulsarClient
        .package(path: ".."),
    ],
    targets: [
        // Producer sample
        .executableTarget(
            name: "ProducerSample",
            dependencies: [
                .product(name: "PulsarClient", package: "pulsar-client")
            ],
            path: "Sources/ProducerSample"
        ),
        
        // Consumer sample
        .executableTarget(
            name: "ConsumerSample",
            dependencies: [
                .product(name: "PulsarClient", package: "pulsar-client")
            ],
            path: "Sources/ConsumerSample"
        ),
        
        // Reader sample
        .executableTarget(
            name: "ReaderSample",
            dependencies: [
                .product(name: "PulsarClient", package: "pulsar-client")
            ],
            path: "Sources/ReaderSample"
        ),
        
        // Batch processing sample
        .executableTarget(
            name: "BatchProcessingSample",
            dependencies: [
                .product(name: "PulsarClient", package: "pulsar-client")
            ],
            path: "Sources/BatchProcessingSample"
        ),
    ]
)