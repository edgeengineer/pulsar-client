import Foundation
import PulsarClient
import Metrics
import Logging

/// Example demonstrating how to use telemetry with the Pulsar Swift Client
@main
struct TelemetryExample {
  static func main() async throws {
    // Setup logging
    LoggingSystem.bootstrap(StreamLogHandler.standardOutput)
    let logger = Logger(label: "TelemetryExample")

    // Example 1: Default configuration (telemetry disabled)
    logger.info("Example 1: Default configuration (no telemetry)")
    try await runWithNoTelemetry()

    // Example 2: Using SwiftMetrics bridge with console output
    logger.info("Example 2: SwiftMetrics with console output")
    try await runWithConsoleMetrics()

    // Example 3: Custom telemetry provider
    logger.info("Example 3: Custom telemetry provider")
    try await runWithCustomProvider()

    // Example 4: Production-ready setup with Prometheus
    logger.info("Example 4: Production setup simulation")
    try await runWithProductionSetup()
  }

  // MARK: - Example 1: No Telemetry

  static func runWithNoTelemetry() async throws {
    let client = PulsarClient.builder { builder in
      builder.withServiceUrl("pulsar://localhost:6650")
      // Telemetry is disabled by default - zero overhead
    }

    let producer = try await client.newProducer { builder in
      builder
        .topic("persistent://public/default/telemetry-demo")
        .producerName("no-telemetry-producer")
    }

    // Send messages - no metrics collected
    for i in 0..<10 {
      let message = "Message \(i) without telemetry"
      _ = try await producer.send(message)
    }

    try await producer.close()
    try await client.close()
  }

  // MARK: - Example 2: Console Metrics

  static func runWithConsoleMetrics() async throws {
    // Bootstrap swift-metrics with a simple console handler
    MetricsSystem.bootstrap(ConsoleMetricsHandler.init)

    let client = PulsarClient.builder { builder in
      builder
        .withServiceUrl("pulsar://localhost:6650")
        .withTelemetry { telemetry in
          telemetry
            .provider(SwiftMetricsBridge())
            .tracingSampleRate(1.0)  // Sample all traces for demo
            .service(name: "telemetry-example", version: "1.0.0")
            .addDimension(key: "environment", value: "development")
        }
    }

    let topic = "persistent://public/default/telemetry-console"

    // Create producer - metrics interceptors automatically added
    let producer = try await client.newProducer { builder in
      builder
        .topic(topic)
        .producerName("console-metrics-producer")
    }

    // Create consumer - metrics interceptors automatically added
    let consumer = try await client.newConsumer { builder in
      builder
        .topic(topic)
        .subscriptionName("console-metrics-sub")
        .consumerName("console-metrics-consumer")
    }

    // Send and receive messages - metrics collected automatically
    for i in 0..<10 {
      let message = "Telemetry message \(i)"
      _ = try await producer.send(message)
    }

    for _ in 0..<10 {
      let message = try await consumer.receive()
      print("Received: \(message.value)")
      try await consumer.acknowledge(message)
    }

    try await producer.close()
    try await consumer.close()
    try await client.close()
  }

  // MARK: - Example 3: Custom Provider

  static func runWithCustomProvider() async throws {
    // Create a custom provider that logs all metrics
    let customProvider = LoggingTelemetryProvider()

    let client = PulsarClient.builder { builder in
      builder
        .withServiceUrl("pulsar://localhost:6650")
        .withTelemetry { telemetry in
          telemetry
            .provider(customProvider)
            .enableDetailedMetrics()
            .metricsInterval(10)  // Report every 10 seconds
        }
    }

    let producer = try await client.newProducer { builder in
      builder
        .topic("persistent://public/default/telemetry-custom")
        .producerName("custom-provider-producer")
        .batchingEnabled(true)
        .batchingMaxMessages(5)
    }

    // Send batched messages
    for i in 0..<20 {
      let message = "Custom provider message \(i)"
      _ = try await producer.sendAsync(message)

      // Small delay to demonstrate batching
      try await Task.sleep(nanoseconds: 100_000_000)  // 100ms
    }

    try await producer.close()
    try await client.close()
  }

  // MARK: - Example 4: Production Setup

  static func runWithProductionSetup() async throws {
    // Simulate a production setup with proper configuration
    let telemetryConfig = TelemetryConfiguration(
      enabled: true,
      provider: createProductionProvider(),
      metricsInterval: 60,  // Report every minute
      tracingSampleRate: 0.01,  // Sample 1% of traces
      customDimensions: [
        "datacenter": "us-west-2",
        "cluster": "production",
        "team": "platform"
      ],
      enableDetailedMetrics: false,  // Disable expensive metrics
      serviceName: "order-processing",
      serviceVersion: getServiceVersion(),
      serviceInstanceId: getInstanceId()
    )

    let client = PulsarClient.builder { builder in
      builder
        .withServiceUrl("pulsar://prod.example.com:6650")
        .withTelemetry(telemetryConfig)
        .withAuthentication(getAuthentication())
    }

    // Simulate production workload
    try await simulateProductionWorkload(client: client)

    try await client.close()
  }

  // MARK: - Helper Functions

  static func createProductionProvider() -> any TelemetryProvider {
    // In production, this would be your actual provider
    // e.g., OpenTelemetry, Datadog, New Relic, etc.
    return SwiftMetricsBridge()
  }

  static func getServiceVersion() -> String {
    // Read from Info.plist or environment
    return ProcessInfo.processInfo.environment["SERVICE_VERSION"] ?? "1.0.0"
  }

  static func getInstanceId() -> String {
    // Could be hostname, container ID, etc.
    return ProcessInfo.processInfo.hostName
  }

  static func getAuthentication() -> Authentication {
    // Production authentication
    return TokenAuthentication(token: "dummy-token")
  }

  static func simulateProductionWorkload(client: PulsarClient) async throws {
    // Create multiple producers/consumers to simulate real workload
    let topics = ["orders", "payments", "notifications"]

    for topic in topics {
      let fullTopic = "persistent://production/services/\(topic)"

      // Producer task
      Task {
        let producer = try await client.newProducer { builder in
          builder
            .topic(fullTopic)
            .producerName("\(topic)-producer")
            .compressionType(.lz4)
            .batchingEnabled(true)
        }

        // Simulate message production
        for i in 0..<100 {
          _ = try await producer.sendAsync("Production message \(i)")
          try await Task.sleep(nanoseconds: 10_000_000)  // 10ms
        }

        try await producer.close()
      }

      // Consumer task
      Task {
        let consumer = try await client.newConsumer { builder in
          builder
            .topic(fullTopic)
            .subscriptionName("\(topic)-processor")
            .consumerName("\(topic)-consumer")
            .subscriptionType(.shared)
        }

        // Simulate message processing
        for _ in 0..<100 {
          if let message = try? await consumer.receive(timeout: 1.0) {
            // Process message
            try await Task.sleep(nanoseconds: 5_000_000)  // 5ms processing time
            try await consumer.acknowledge(message)
          }
        }

        try await consumer.close()
      }
    }

    // Let workload run for a bit
    try await Task.sleep(nanoseconds: 5_000_000_000)  // 5 seconds
  }
}

// MARK: - Console Metrics Handler

/// Simple metrics handler that prints to console
struct ConsoleMetricsHandler: MetricsFactory {
  func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
    ConsoleCounterHandler(label: label, dimensions: dimensions)
  }

  func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> RecorderHandler {
    ConsoleRecorderHandler(label: label, dimensions: dimensions)
  }

  func makeTimer(label: String, dimensions: [(String, String)]) -> TimerHandler {
    ConsoleTimerHandler(label: label, dimensions: dimensions)
  }

  func destroyCounter(_ handler: CounterHandler) {}
  func destroyRecorder(_ handler: RecorderHandler) {}
  func destroyTimer(_ handler: TimerHandler) {}
}

class ConsoleCounterHandler: CounterHandler {
  let label: String
  let dimensions: [(String, String)]
  private var value: Int64 = 0

  init(label: String, dimensions: [(String, String)]) {
    self.label = label
    self.dimensions = dimensions
  }

  func increment(by amount: Int64) {
    value += amount
    print("📊 Counter: \(label) = \(value) \(formatDimensions())")
  }

  func reset() {
    value = 0
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    let dims = dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ",")
    return "{\(dims)}"
  }
}

class ConsoleRecorderHandler: RecorderHandler {
  let label: String
  let dimensions: [(String, String)]

  init(label: String, dimensions: [(String, String)]) {
    self.label = label
    self.dimensions = dimensions
  }

  func record(_ value: Int64) {
    print("📈 Recorder: \(label) = \(value) \(formatDimensions())")
  }

  func record(_ value: Double) {
    print("📈 Recorder: \(label) = \(value) \(formatDimensions())")
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    let dims = dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ",")
    return "{\(dims)}"
  }
}

class ConsoleTimerHandler: TimerHandler {
  let label: String
  let dimensions: [(String, String)]

  init(label: String, dimensions: [(String, String)]) {
    self.label = label
    self.dimensions = dimensions
  }

  func recordNanoseconds(_ duration: Int64) {
    let ms = Double(duration) / 1_000_000
    print("⏱️ Timer: \(label) = \(ms)ms \(formatDimensions())")
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    let dims = dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ",")
    return "{\(dims)}"
  }
}

// MARK: - Logging Telemetry Provider

/// Custom provider that logs all telemetry operations
class LoggingTelemetryProvider: TelemetryProvider {
  let logger = Logger(label: "LoggingTelemetryProvider")
  let metrics: any MetricsProvider
  let tracing: any TracingProvider

  init() {
    self.metrics = LoggingMetricsProvider()
    self.tracing = LoggingTracingProvider()
  }

  func configure() async throws {
    logger.info("Telemetry provider configured")
  }

  func shutdown() async {
    logger.info("Telemetry provider shutdown")
  }
}

class LoggingMetricsProvider: MetricsProvider {
  let logger = Logger(label: "LoggingMetrics")

  func counter(name: String, dimensions: MetricDimensions?) -> any MetricCounter {
    logger.debug("Created counter: \(name)")
    return NoOpCounter()
  }

  func gauge(name: String, dimensions: MetricDimensions?) -> any MetricGauge {
    logger.debug("Created gauge: \(name)")
    return NoOpGauge()
  }

  func histogram(name: String, dimensions: MetricDimensions?) -> any MetricHistogram {
    logger.debug("Created histogram: \(name)")
    return NoOpHistogram()
  }

  func timer(name: String, dimensions: MetricDimensions?) -> any MetricTimer {
    logger.debug("Created timer: \(name)")
    return NoOpTimer()
  }
}

class LoggingTracingProvider: TracingProvider {
  let logger = Logger(label: "LoggingTracing")

  func startSpan(
    name: String,
    parent: (any Span)?,
    options: SpanOptions?
  ) -> any Span {
    logger.debug("Started span: \(name)")
    return NoOpSpan()
  }

  func injectContext(from span: any Span, into headers: inout [String: String]) {
    logger.debug("Injected trace context")
  }

  func extractContext(from headers: [String: String]) -> SpanContext? {
    logger.debug("Extracted trace context")
    return nil
  }
}