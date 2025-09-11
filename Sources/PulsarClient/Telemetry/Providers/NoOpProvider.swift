import Foundation

/// No-operation implementation of metrics and tracing for zero-cost abstraction when disabled
public final class NoOpTelemetryProvider: TelemetryProvider, @unchecked Sendable {
  public let metrics: any MetricsProvider
  public let tracing: any TracingProvider

  public init() {
    self.metrics = NoOpMetricsProvider()
    self.tracing = NoOpTracingProvider()
  }

  /// Shared singleton instance
  public static let shared = NoOpTelemetryProvider()
}

// MARK: - NoOp Metrics Implementation

/// No-operation metrics provider
final class NoOpMetricsProvider: MetricsProvider, @unchecked Sendable {
  func counter(name: String, dimensions: MetricDimensions?) -> any MetricCounter {
    NoOpCounter()
  }

  func gauge(name: String, dimensions: MetricDimensions?) -> any MetricGauge {
    NoOpGauge()
  }

  func histogram(name: String, dimensions: MetricDimensions?) -> any MetricHistogram {
    NoOpHistogram()
  }

  func timer(name: String, dimensions: MetricDimensions?) -> any MetricTimer {
    NoOpTimer()
  }
}

/// No-operation counter
struct NoOpCounter: MetricCounter {
  func increment() {
    // No-op
  }

  func increment(by value: Int64) {
    // No-op
  }
}

/// No-operation gauge
struct NoOpGauge: MetricGauge {
  func record(_ value: Double) {
    // No-op
  }

  func increment(by value: Double) {
    // No-op
  }

  func decrement(by value: Double) {
    // No-op
  }
}

/// No-operation histogram
struct NoOpHistogram: MetricHistogram {
  func record(_ value: Double) {
    // No-op
  }

  func record(_ values: [Double]) {
    // No-op
  }
}

/// No-operation timer
struct NoOpTimer: MetricTimer {
  func recordSeconds(_ duration: Double) {
    // No-op
  }

  func recordNanoseconds(_ duration: UInt64) {
    // No-op
  }

  func record(_ duration: Duration) {
    // No-op
  }

  func time<T>(_ block: () async throws -> T) async rethrows -> T {
    try await block()
  }
}

// MARK: - NoOp Tracing Implementation

/// No-operation tracing provider
final class NoOpTracingProvider: TracingProvider, @unchecked Sendable {
  func startSpan(
    name: String,
    parent: (any Span)?,
    options: SpanOptions?
  ) -> any Span {
    NoOpSpan()
  }

  func injectContext(from span: any Span, into headers: inout [String: String]) {
    // No-op
  }

  func extractContext(from headers: [String: String]) -> SpanContext? {
    nil
  }
}

/// No-operation span
struct NoOpSpan: Span {
  let context = SpanContext(
    traceId: "00000000000000000000000000000000",
    spanId: "0000000000000000"
  )

  func setAttribute(key: String, value: String) {
    // No-op
  }

  func setAttribute(key: String, value: Int) {
    // No-op
  }

  func setAttribute(key: String, value: Double) {
    // No-op
  }

  func setAttribute(key: String, value: Bool) {
    // No-op
  }

  func addEvent(name: String, attributes: [String: String]?) {
    // No-op
  }

  func setStatus(_ status: SpanStatus) {
    // No-op
  }

  func end() {
    // No-op
  }

  func end(at time: Date) {
    // No-op
  }
}
