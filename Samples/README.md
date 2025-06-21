# PulsarClient Swift Samples

This directory contains sample applications demonstrating how to use the Swift PulsarClient library.

## Prerequisites

- Swift 6.0 or later
- Apache Pulsar server running (by default on `pulsar://localhost:6650`)
- macOS 13+ / iOS 16+ / Linux

## Sample Applications

### 1. ProducerSample

Demonstrates how to create a producer and send messages to a Pulsar topic.

**Features:**
- Creates a producer with compression and batching enabled
- Sends messages every 5 seconds with timestamps
- Handles graceful shutdown with Ctrl+C
- Shows proper resource cleanup

**Run:**
```bash
cd Samples
swift run ProducerSample
```

### 2. ConsumerSample

Shows how to create a consumer and receive messages from a Pulsar topic.

**Features:**
- Creates a shared subscription consumer
- Receives and acknowledges messages
- Displays message metadata (ID, publish time, properties)
- Handles timeouts and errors gracefully
- Monitors consumer state changes

**Run:**
```bash
cd Samples
swift run ConsumerSample
```

### 3. ReaderSample

Demonstrates how to use a reader to read messages from a topic starting from the earliest message.

**Features:**
- Creates a reader starting from earliest messages
- Checks message availability before reading
- Displays comprehensive message information
- Shows how to handle reading historical data
- Monitors reader state changes

**Run:**
```bash
cd Samples
swift run ReaderSample
```

## Quick Start

1. **Start Apache Pulsar:**
   ```bash
   # Using Docker
   docker run -it -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone
   
   # Or using local installation
   bin/pulsar standalone
   ```

2. **Build and run the producer:**
   ```bash
   cd Samples
   swift run ProducerSample
   ```

3. **In another terminal, run the consumer:**
   ```bash
   cd Samples
   swift run ConsumerSample
   ```

4. **Optionally, run the reader to see historical messages:**
   ```bash
   cd Samples
   swift run ReaderSample
   ```

## Configuration

The samples connect to a Pulsar server at `pulsar://localhost:6650` by default. You can modify the connection settings in each sample's `main.swift` file:

```swift
let client = try await PulsarClient.builder()
    .serviceUrl("pulsar://your-server:6650")  // Change this
    .authentication(AuthenticationFactory.token("your-token"))  // Add auth if needed
    .build()
```

## Sample Topic

All samples use the topic `persistent://public/default/mytopic`. You can change this in each sample if needed.

## Features Demonstrated

### Authentication
```swift
// No authentication (development)
.authentication(AuthenticationFactory.none())

// Token authentication
.authentication(AuthenticationFactory.token("your-jwt-token"))

// Token from environment variable
.authentication(AuthenticationFactory.tokenFromEnvironment("PULSAR_TOKEN"))

// Token from file
.authentication(AuthenticationFactory.tokenFromFile("/path/to/token.txt"))
```

### Producer Configuration
```swift
let producer = try await client.newProducer(schema: StringSchema())
    .topic("persistent://public/default/mytopic")
    .producerName("my-producer")
    .compression(.zlib)                    // Enable compression
    .batching(enabled: true)               // Enable batching
    .sendTimeout(30.0)                     // Set send timeout
    .create()
```

### Consumer Configuration
```swift
let consumer = try await client.newConsumer(schema: StringSchema())
    .topic("persistent://public/default/mytopic")
    .subscription("my-subscription")
    .subscriptionType(.shared)             // Shared subscription
    .subscriptionInitialPosition(.latest)  // Start from latest
    .ackTimeout(30.0)                      // Acknowledgment timeout
    .receiverQueueSize(1000)               // Buffer size
    .create()
```

### Reader Configuration
```swift
let reader = try await client.newReader(schema: StringSchema())
    .topic("persistent://public/default/mytopic")
    .startMessageId(.earliest)             // Start from beginning
    .readerName("my-reader")
    .create()
```

## Error Handling

All samples include proper error handling and logging:

```swift
// Exception handler for the client
.exceptionHandler { context in
    logger.error("PulsarClient exception: \(context.exception)")
}

// Try-catch blocks for operations
do {
    let messageId = try await producer.send(message)
    logger.info("Message sent: \(messageId)")
} catch {
    logger.error("Failed to send: \(error)")
}
```

## Graceful Shutdown

All samples handle SIGINT (Ctrl+C) for graceful shutdown:

```swift
// Setup signal handling
let signalSource = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
signalSource.setEventHandler {
    logger.info("Shutting down...")
    shouldStop = true
}
```

## Troubleshooting

### Connection Issues

1. **Check if Pulsar is running:**
   ```bash
   curl http://localhost:8080/admin/v2/clusters
   ```

2. **Verify port accessibility:**
   ```bash
   telnet localhost 6650
   ```

### Authentication Issues

1. **For token authentication, ensure the token is valid**
2. **Check Pulsar server authentication configuration**
3. **Verify network connectivity to the authentication service**

### Performance Tuning

1. **Enable batching for high throughput:**
   ```swift
   .batching(enabled: true)
   ```

2. **Adjust receiver queue size for consumers:**
   ```swift
   .receiverQueueSize(10000)
   ```

3. **Use compression for large messages:**
   ```swift
   .compression(.zlib)
   ```

## Next Steps

After running these samples, explore:

1. **Multi-topic subscriptions**
2. **Schema evolution**
3. **Message encryption**
4. **Dead letter topics**
5. **Transactions (when available)**

For more advanced usage, refer to the main PulsarClient documentation.