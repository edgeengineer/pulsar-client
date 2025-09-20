import Foundation
import PulsarClient
import Metrics
import Tracing
import Logging

/// Example demonstrating telemetry with Pulsar Swift Client
@main
struct TelemetryExample {
  static func main() async throws {
    // Setup logging
    LoggingSystem.bootstrap(StreamLogHandler.standardOutput)
    let logger = Logger(label: "TelemetryExample")

    // Example 1: Default - No telemetry
    logger.info("Example 1: No telemetry backend")
    try await runWithNoTelemetry()

    // Example 2: Simple metrics with console output
    logger.info("Example 2: Console metrics backend")
    try await runWithConsoleMetrics()

    // Example 3: Production setup with Prometheus
    logger.info("Example 3: Prometheus backend")
    try await runWithPrometheus()

    // Example 4: Multiple backends
    logger.info("Example 4: Multiple backends")
    try await runWithMultipleBackends()
  }

  // MARK: - Example 1: No Telemetry Backend

  static func runWithNoTelemetry() async throws {
    // Metrics are automatically no-ops

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl("pulsar://localhost:6650")
    }

    let producer = try await client.newProducer { builder in
      builder
        .topic("persistent://public/default/no-telemetry")
        .producerName("example-producer")
    }

    // Send messages - no metrics collected
    for i in 0..<10 {
      _ = try await producer.send("Message \(i)")
    }

    try await producer.close()
    try await client.close()
  }

  // MARK: - Example 2: Console Metrics

  static func runWithConsoleMetrics() async throws {
    // Bootstrap swift-metrics with console output
    MetricsSystem.bootstrap(ConsoleMetricsFactory())

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl("pulsar://localhost:6650")
    }

    let topic = "persistent://public/default/console-metrics"

    // Create producer - metrics automatically collected
    let producer = try await client.newProducer { builder in
      builder
        .topic(topic)
        .producerName("console-producer")
    }

    // Create consumer - metrics automatically collected
    let consumer = try await client.newConsumer { builder in
      builder
        .topic(topic)
        .subscriptionName("console-sub")
        .consumerName("console-consumer")
    }

    // Send and receive messages - metrics flow automatically
    for i in 0..<5 {
      _ = try await producer.send("Console message \(i)")
    }

    for _ in 0..<5 {
      let message = try await consumer.receive()
      print("Received: \(message.value)")
      try await consumer.acknowledge(message)
    }

    try await producer.close()
    try await consumer.close()
    try await client.close()
  }

  // MARK: - Example 3: Prometheus Backend

  static func runWithPrometheus() async throws {
    // In a real app, you'd use the actual Prometheus library
    // For demo, we'll use our console factory
    MetricsSystem.bootstrap { label, dimensions in
      PrometheusLikeHandler(label: label, dimensions: dimensions)
    }

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl("pulsar://localhost:6650")
    }

    // Simulate production workload
    try await runProductionWorkload(client: client)

    // In a real app, you'd expose metrics endpoint:
    // app.get("/metrics") { req in prometheus.collect() }

    try await client.close()
  }

  // MARK: - Example 4: Multiple Backends

  static func runWithMultipleBackends() async throws {
    // Use a multiplexing factory to send to multiple backends
    MetricsSystem.bootstrap(MultiplexMetricsFactory(factories: [
      ConsoleMetricsFactory(),
      PrometheusLikeFactory(),
    ]))

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl("pulsar://localhost:6650")
    }

    let producer = try await client.newProducer { builder in
      builder.topic("persistent://public/default/multi-backend")
    }

    // Metrics go to all configured backends
    for i in 0..<10 {
      _ = try await producer.send("Multi-backend message \(i)")
    }

    try await producer.close()
    try await client.close()
  }

  // MARK: - Helper Functions

  static func runProductionWorkload(client: PulsarClient) async throws {
    let topics = ["orders", "payments", "inventory"]

    // Create producers and consumers for each topic
    await withTaskGroup(of: Void.self) { group in
      for topic in topics {
        let fullTopic = "persistent://public/default/\(topic)"

        // Producer task
        group.addTask {
          do {
            let producer = try await client.newProducer { builder in
              builder
                .topic(fullTopic)
                .producerName("\(topic)-producer")
                .batchingEnabled(true)
            }

            for i in 0..<20 {
              _ = try await producer.send("\(topic) message \(i)")
              try await Task.sleep(nanoseconds: 50_000_000) // 50ms
            }

            try await producer.close()
          } catch {
            print("Producer error: \(error)")
          }
        }

        // Consumer task
        group.addTask {
          do {
            let consumer = try await client.newConsumer { builder in
              builder
                .topic(fullTopic)
                .subscriptionName("\(topic)-subscription")
                .consumerName("\(topic)-consumer")
            }

            for _ in 0..<20 {
              if let message = try? await consumer.receive(timeout: 1.0) {
                try await consumer.acknowledge(message)
                try await Task.sleep(nanoseconds: 25_000_000) // 25ms processing
              }
            }

            try await consumer.close()
          } catch {
            print("Consumer error: \(error)")
          }
        }
      }
    }
  }
}

// MARK: - Simple Console Metrics Factory

struct ConsoleMetricsFactory: MetricsFactory {
  func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
    ConsoleCounterHandler(label: label, dimensions: dimensions)
  }

  func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> RecorderHandler {
    ConsoleRecorderHandler(label: label, dimensions: dimensions)
  }

  func makeTimer(label: String, dimensions: [(String, String)]) -> TimerHandler {
    ConsoleTimerHandler(label: label, dimensions: dimensions)
  }

  func makeGauge(label: String, dimensions: [(String, String)]) -> GaugeHandler {
    ConsoleGaugeHandler(label: label, dimensions: dimensions)
  }

  func destroyCounter(_ handler: CounterHandler) {}
  func destroyRecorder(_ handler: RecorderHandler) {}
  func destroyTimer(_ handler: TimerHandler) {}
  func destroyGauge(_ handler: GaugeHandler) {}
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
    print("Counter: \(label) = \(value) \(formatDimensions())")
  }

  func reset() {
    value = 0
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    return dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ", ")
  }
}

class ConsoleGaugeHandler: GaugeHandler {
  let label: String
  let dimensions: [(String, String)]
  private var value: Double = 0

  init(label: String, dimensions: [(String, String)]) {
    self.label = label
    self.dimensions = dimensions
  }

  func record(_ value: Double) {
    self.value = value
    print("Gauge: \(label) = \(value) \(formatDimensions())")
  }

  func record(_ value: Int64) {
    self.value = Double(value)
    print("Gauge: \(label) = \(value) \(formatDimensions())")
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    return dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ", ")
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
    print("Recorder: \(label) = \(value) \(formatDimensions())")
  }

  func record(_ value: Double) {
    print("Recorder: \(label) = \(value) \(formatDimensions())")
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    return dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ", ")
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
    print("Timer: \(label) = \(String(format: "%.2f", ms))ms \(formatDimensions())")
  }

  func recordSeconds(_ duration: Double) {
    print("Timer: \(label) = \(String(format: "%.3f", duration))s \(formatDimensions())")
  }

  private func formatDimensions() -> String {
    guard !dimensions.isEmpty else { return "" }
    return dimensions.map { "\($0.0)=\($0.1)" }.joined(separator: ", ")
  }
}

// MARK: - Prometheus-like Handler (for demo)

struct PrometheusLikeFactory: MetricsFactory {
  func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
    PrometheusLikeHandler(label: label, dimensions: dimensions)
  }

  func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> RecorderHandler {
    PrometheusLikeHandler(label: label, dimensions: dimensions)
  }

  func makeTimer(label: String, dimensions: [(String, String)]) -> TimerHandler {
    PrometheusLikeHandler(label: label, dimensions: dimensions)
  }

  func makeGauge(label: String, dimensions: [(String, String)]) -> GaugeHandler {
    PrometheusLikeHandler(label: label, dimensions: dimensions)
  }

  func destroyCounter(_ handler: CounterHandler) {}
  func destroyRecorder(_ handler: RecorderHandler) {}
  func destroyTimer(_ handler: TimerHandler) {}
  func destroyGauge(_ handler: GaugeHandler) {}
}

class PrometheusLikeHandler: CounterHandler, RecorderHandler, TimerHandler, GaugeHandler {
  let label: String
  let dimensions: [(String, String)]

  init(label: String, dimensions: [(String, String)]) {
    self.label = label
    self.dimensions = dimensions
  }

  func increment(by amount: Int64) {
    // In real implementation, would store in Prometheus format
  }

  func reset() {}

  func record(_ value: Int64) {
    // Store histogram/summary data
  }

  func record(_ value: Double) {
    // Store histogram/summary data
  }

  func recordNanoseconds(_ duration: Int64) {
    // Store timing data
  }

  func recordSeconds(_ duration: Double) {
    // Store timing data
  }
}

// MARK: - Multiplexing Factory

struct MultiplexMetricsFactory: MetricsFactory {
  let factories: [MetricsFactory]

  func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
    MultiplexCounterHandler(
      handlers: factories.map { $0.makeCounter(label: label, dimensions: dimensions) }
    )
  }

  func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> RecorderHandler {
    MultiplexRecorderHandler(
      handlers: factories.map { $0.makeRecorder(label: label, dimensions: dimensions, aggregate: aggregate) }
    )
  }

  func makeTimer(label: String, dimensions: [(String, String)]) -> TimerHandler {
    MultiplexTimerHandler(
      handlers: factories.map { $0.makeTimer(label: label, dimensions: dimensions) }
    )
  }

  func makeGauge(label: String, dimensions: [(String, String)]) -> GaugeHandler {
    MultiplexGaugeHandler(
      handlers: factories.map { $0.makeGauge(label: label, dimensions: dimensions) }
    )
  }

  func destroyCounter(_ handler: CounterHandler) {}
  func destroyRecorder(_ handler: RecorderHandler) {}
  func destroyTimer(_ handler: TimerHandler) {}
  func destroyGauge(_ handler: GaugeHandler) {}
}

class MultiplexCounterHandler: CounterHandler {
  let handlers: [CounterHandler]

  init(handlers: [CounterHandler]) {
    self.handlers = handlers
  }

  func increment(by amount: Int64) {
    handlers.forEach { $0.increment(by: amount) }
  }

  func reset() {
    handlers.forEach { $0.reset() }
  }
}

class MultiplexGaugeHandler: GaugeHandler {
  let handlers: [GaugeHandler]

  init(handlers: [GaugeHandler]) {
    self.handlers = handlers
  }

  func record(_ value: Double) {
    handlers.forEach { $0.record(value) }
  }

  func record(_ value: Int64) {
    handlers.forEach { $0.record(value) }
  }
}

class MultiplexRecorderHandler: RecorderHandler {
  let handlers: [RecorderHandler]

  init(handlers: [RecorderHandler]) {
    self.handlers = handlers
  }

  func record(_ value: Int64) {
    handlers.forEach { $0.record(value) }
  }

  func record(_ value: Double) {
    handlers.forEach { $0.record(value) }
  }
}

class MultiplexTimerHandler: TimerHandler {
  let handlers: [TimerHandler]

  init(handlers: [TimerHandler]) {
    self.handlers = handlers
  }

  func recordNanoseconds(_ duration: Int64) {
    handlers.forEach { $0.recordNanoseconds(duration) }
  }

  func recordSeconds(_ duration: Double) {
    handlers.forEach { $0.recordSeconds(duration) }
  }
}