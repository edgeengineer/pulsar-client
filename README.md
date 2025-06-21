# PulsarClient for Swift

!!!WARNING STILL UNDER ACTIVE DEVELOPMENT!!!

![Swift](https://img.shields.io/badge/Swift-6.1-orange.svg)
![Platforms](https://img.shields.io/badge/Platforms-iOS%20%7C%20macOS%20%7C%20tvOS%20%7C%20watchOS%20%7C%20Linux-lightgray.svg)
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)
[![Swift Package Manager](https://img.shields.io/badge/SPM-compatible-brightgreen.svg)](https://swift.org/package-manager/)

A modern, async/await-based Apache Pulsar client for Swift, providing a native Swift experience for building event-driven applications.

</div>

## Features

- 🚀 **Pure Swift Implementation**: Built from the ground up using Swift 6.1 with full concurrency support
- 🔄 **Async/Await**: Modern Swift concurrency with actors and structured concurrency
- 📦 **Type-Safe Schemas**: Comprehensive schema support including primitives, JSON, and custom types
- 🛡️ **Fault Tolerant**: Built-in retry policies, automatic reconnection, and circuit breakers
- 🔐 **Security**: Multiple authentication methods and encryption policies
- 📱 **Cross-Platform**: Works on iOS, macOS, tvOS, watchOS, and Linux
- 🎯 **Producer Features**: Batching, compression, partitioned topics, exclusive access modes
- 📨 **Consumer Features**: Multiple subscription types, acknowledgment strategies, dead letter queues
- 📖 **Reader API**: Sequential message reading with precise position control

## Requirements

- Swift 6.1+
- iOS 15.0+ / macOS 12.0+ / tvOS 15.0+ / watchOS 8.0+ / Linux (Ubuntu 20.04+)

## Installation

### Swift Package Manager

Add PulsarClient to your `Package.swift`:

```swift
dependencies: [
    .package(url: "https://github.com/edgeengineer/pulsar-client.git", from: "0.0.1")
]
```

Then add it to your target dependencies:

```swift
.target(
    name: "YourApp",
    dependencies: ["PulsarClient"]
)
```

## Quick Start

```swift
import PulsarClient

// Create a client
let client = PulsarClient.builder { builder in
    builder.withServiceUrl("pulsar://localhost:6650")
}

// Create a producer
let producer = try await client.newProducer(
    topic: "my-topic",
    schema: Schemas.string
) { builder in
    builder.withProducerName("my-producer")
}

// Send a message
let messageId = try await producer.send("Hello, Pulsar!")
print("Message sent with ID: \(messageId)")

// Create a consumer
let consumer = try await client.newConsumer(
    topic: "my-topic",
    schema: Schemas.string
) { builder in
    builder.withSubscriptionName("my-subscription")
           .withSubscriptionType(.exclusive)
}

// Receive messages
let message = try await consumer.receive()
print("Received: \(message.value)")
try await consumer.acknowledge(message)

// Clean up
await producer.dispose()
await consumer.dispose()
await client.dispose()
```

## Detailed Usage

### Client Configuration

```swift
let client = PulsarClient.builder { builder in
    builder.withServiceUrl("pulsar://localhost:6650")
           .withAuthentication(TokenAuthentication(token: "your-token"))
           .withEncryptionPolicy(.enforceEncrypted)
           .withConnectionTimeout(30.0)
           .withOperationTimeout(30.0)
}
```

### Producer Patterns

#### Basic Producer

```swift
let producer = try await client.newProducer(
    topic: "persistent://public/default/my-topic",
    schema: Schemas.string
) { builder in
    builder.withProducerName("my-producer")
}

// Send with metadata
var metadata = MessageMetadata()
metadata.key = "partition-key"
metadata.properties["app"] = "my-app"
metadata.eventTime = Date()

let messageId = try await producer.send("Hello, World!", metadata: metadata)
```

#### Batching and Compression

```swift
let producer = try await client.newProducer(
    topic: "high-volume-topic",
    schema: Schemas.bytes
) { builder in
    builder.withBatchingEnabled(true)
           .withBatchingMaxMessages(100)
           .withBatchingMaxDelay(0.01) // 10ms
           .withCompressionType(.lz4)
}
```

#### Partitioned Topics

```swift
let producer = try await client.newProducer(
    topic: "partitioned-topic",
    schema: Schemas.string
) { builder in
    builder.withMessageRouter(KeyBasedMessageRouter())
}

// Messages with the same key go to the same partition
let metadata = MessageMetadata().withKey("user-123")
try await producer.send("User event", metadata: metadata)
```

### Consumer Patterns

#### Subscription Types

```swift
// Exclusive - Only one consumer can subscribe
let exclusiveConsumer = try await client.newConsumer(
    topic: "exclusive-topic",
    schema: Schemas.string
) { builder in
    builder.withSubscriptionName("exclusive-sub")
           .withSubscriptionType(.exclusive)
}

// Shared - Multiple consumers share messages
let sharedConsumer = try await client.newConsumer(
    topic: "shared-topic",
    schema: Schemas.string
) { builder in
    builder.withSubscriptionName("shared-sub")
           .withSubscriptionType(.shared)
}

// Key_Shared - Messages with same key go to same consumer
let keySharedConsumer = try await client.newConsumer(
    topic: "key-shared-topic",
    schema: Schemas.string
) { builder in
    builder.withSubscriptionName("key-shared-sub")
           .withSubscriptionType(.keyShared)
}
```

#### Batch Message Processing

```swift
let consumer = try await client.newConsumer(
    topic: "batch-topic",
    schema: Schemas.string
) { builder in
    builder.withSubscriptionName("batch-processor")
           .withReceiverQueueSize(1000)
}

// Process messages in batches
let messages = try await consumer.receiveBatch(maxMessages: 100)
for message in messages {
    // Process message
    print("Processing: \(message.value)")
}

// Acknowledge all at once
try await consumer.acknowledgeBatch(messages)
```

#### Negative Acknowledgment and Retry

```swift
let consumer = try await client.newConsumer(
    topic: "retry-topic",
    schema: Schemas.string
) { builder in
    builder.withSubscriptionName("retry-sub")
           .withNegativeAckRedeliveryDelay(5.0) // 5 seconds
}

do {
    let message = try await consumer.receive()
    // Process message
    try await processMessage(message)
    try await consumer.acknowledge(message)
} catch {
    // Message will be redelivered after delay
    try await consumer.negativeAcknowledge(message)
}
```

### Reader API

```swift
let reader = try await client.newReader(
    topic: "reader-topic",
    schema: Schemas.string
) { builder in
    builder.withStartMessageId(.earliest)
           .withReaderName("my-reader")
}

// Read messages sequentially
while try await reader.hasMessageAvailable() {
    let message = try await reader.readNext()
    print("Read: \(message.value) at \(message.publishTime)")
}

// Seek to specific position
try await reader.seek(to: MessageId.latest)
```

### Schema Types

#### Built-in Schemas

```swift
// Primitive types
let stringSchema = Schemas.string
let int32Schema = Schemas.int32
let int64Schema = Schemas.int64
let boolSchema = Schemas.boolean
let doubleSchema = Schemas.double
let bytesSchema = Schemas.bytes

// Date/Time schemas
let dateSchema = Schemas.date
let timeSchema = Schemas.time
let timestampSchema = Schemas.timestamp
```

#### JSON Schema

```swift
struct UserEvent: Codable {
    let userId: String
    let action: String
    let timestamp: Date
}

let jsonSchema = JSONSchema<UserEvent>()

let producer = try await client.newProducer(
    topic: "user-events",
    schema: jsonSchema
) { builder in
    builder.withProducerName("user-event-producer")
}

let event = UserEvent(
    userId: "user-123",
    action: "login",
    timestamp: Date()
)
try await producer.send(event)
```

### Error Handling

```swift
do {
    let message = try await consumer.receive()
    try await processMessage(message)
    try await consumer.acknowledge(message)
} catch PulsarClientError.timeout(let operation) {
    print("Operation timed out: \(operation)")
} catch PulsarClientError.consumerBusy(let reason) {
    print("Consumer busy: \(reason)")
} catch {
    print("Unexpected error: \(error)")
}
```

### State Management

Monitor component states:

```swift
// Producer states
producer.onStateChange { state in
    switch state {
    case .connected:
        print("Producer connected")
    case .disconnected:
        print("Producer disconnected")
    case .faulted(let error):
        print("Producer faulted: \(error)")
    default:
        break
    }
}

// Consumer states
consumer.onStateChange { state in
    switch state {
    case .active:
        print("Consumer active")
    case .inactive:
        print("Consumer inactive (failover)")
    case .reachedEndOfTopic:
        print("No more messages")
    default:
        break
    }
}
```

### Authentication

```swift
// Token Authentication
let tokenAuth = TokenAuthentication(token: "your-jwt-token")

// OAuth2 Authentication
let oauth2 = OAuth2Authentication(
    issuerUrl: "https://auth.example.com",
    audience: "pulsar",
    privateKey: privateKeyData
)

// TLS Authentication
let tlsAuth = TLSAuthentication(
    certPath: "/path/to/cert.pem",
    keyPath: "/path/to/key.pem"
)

let client = PulsarClient.builder { builder in
    builder.withServiceUrl("pulsar+ssl://localhost:6651")
           .withAuthentication(tlsAuth)
}
```

## Advanced Features

### Message Routing

```swift
// Round-robin routing
let roundRobinRouter = RoundRobinMessageRouter()

// Single partition routing
let singleRouter = SinglePartitionMessageRouter(partitionIndex: 0)

// Custom routing
class CustomRouter: MessageRouter {
    func choosePartition(messageMetadata: MessageMetadata, numberOfPartitions: Int) -> Int {
        // Your routing logic
        return messageMetadata.key?.hashValue ?? 0 % numberOfPartitions
    }
}
```

### Fault Tolerance

```swift
// Configure retry policy
let retryPolicy = RetryPolicy(
    maxRetries: 3,
    initialDelay: 1.0,
    maxDelay: 30.0,
    backoffMultiplier: 2.0
)

// Custom exception handler
class MyExceptionHandler: ExceptionHandler {
    func handleException(_ context: inout ExceptionContext) async {
        switch context.exception {
        case PulsarClientError.connectionFailed:
            context.result = .retryAfter(5.0)
        default:
            context.result = .fail
        }
    }
}
```

## Best Practices

1. **Always dispose resources**: Use `defer` or structured concurrency to ensure cleanup
2. **Handle backpressure**: Configure appropriate queue sizes for consumers
3. **Use schemas**: Type-safe schemas prevent serialization errors
4. **Monitor states**: React to state changes for robust applications
5. **Configure timeouts**: Set appropriate timeouts for your use case
6. **Use batching**: For high-throughput scenarios, enable producer batching
7. **Implement error handling**: Always handle errors appropriately

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

This Swift implementation is inspired by the official [Apache Pulsar DotNet client](https://github.com/apache/pulsar-dotpulsar) and follows similar patterns adapted for Swift's unique features.
