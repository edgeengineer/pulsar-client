import Foundation
import Metrics
import ServiceContextModule
import Tracing

/// Bridge provider that integrates with swift-metrics and swift-distributed-tracing
/// This allows users to plug in any backend that conforms to these Swift server standards
public final class SwiftMetricsBridge: TelemetryProvider {
  public let metrics: any MetricsProvider
  public let tracing: any TracingProvider

  /// Initialize with existing swift-metrics and swift-distributed-tracing setup
  /// Assumes MetricsSystem.bootstrap() and InstrumentationSystem.bootstrap() have been called
  public init() {
    self.metrics = SwiftMetricsAdapter()
    self.tracing = SwiftTracingAdapter()
  }

  /// Initialize with custom adapters
  public init(metrics: any MetricsProvider, tracing: any TracingProvider) {
    self.metrics = metrics
    self.tracing = tracing
  }
}

// MARK: - Swift Metrics Adapter

/// Adapter that bridges our MetricsProvider protocol to swift-metrics
final class SwiftMetricsAdapter: MetricsProvider {
  // Track gauge values since swift-metrics doesn't expose current values
  private let gaugeValues = GaugeValueTracker()

  func counter(name: String, dimensions: MetricDimensions?) -> any MetricCounter {
    SwiftMetricsCounter(
      counter: Counter(label: makeLabel(name: name, dimensions: dimensions))
    )
  }

  func gauge(name: String, dimensions: MetricDimensions?) -> any MetricGauge {
    let label = makeLabel(name: name, dimensions: dimensions)
    return SwiftMetricsGauge(
      gauge: Gauge(label: label),
      tracker: gaugeValues,
      label: label
    )
  }

  func histogram(name: String, dimensions: MetricDimensions?) -> any MetricHistogram {
    SwiftMetricsHistogram(
      recorder: Recorder(label: makeLabel(name: name, dimensions: dimensions))
    )
  }

  func timer(name: String, dimensions: MetricDimensions?) -> any MetricTimer {
    SwiftMetricsTimer(
      timer: Timer(label: makeLabel(name: name, dimensions: dimensions))
    )
  }

  private func makeLabel(name: String, dimensions: MetricDimensions?) -> String {
    guard let dimensions = dimensions, !dimensions.isEmpty else {
      return name
    }

    // Create label with dimensions as tags
    // Format: "name{key1=value1,key2=value2}"
    let tags = dimensions
      .sorted { $0.key < $1.key }
      .map { "\($0.key)=\($0.value)" }
      .joined(separator: ",")

    return "\(name){\(tags)}"
  }
}

// MARK: - Gauge Value Tracker

/// Tracks gauge values since swift-metrics doesn't expose them
actor GaugeValueTracker {
  private var values: [String: Double] = [:]

  func getValue(for label: String) -> Double {
    values[label] ?? 0
  }

  func setValue(_ value: Double, for label: String) {
    values[label] = value
  }

  func increment(_ value: Double, for label: String) {
    values[label] = (values[label] ?? 0) + value
  }

  func decrement(_ value: Double, for label: String) {
    values[label] = (values[label] ?? 0) - value
  }
}

// MARK: - Metric Type Adapters

struct SwiftMetricsCounter: MetricCounter {
  let counter: Counter

  func increment() {
    counter.increment()
  }

  func increment(by value: Int64) {
    counter.increment(by: value)
  }
}

struct SwiftMetricsGauge: MetricGauge {
  let gauge: Gauge
  let tracker: GaugeValueTracker
  let label: String

  func record(_ value: Double) {
    gauge.record(value)
    Task {
      await tracker.setValue(value, for: label)
    }
  }

  func increment(by value: Double) {
    Task {
      await tracker.increment(value, for: label)
      gauge.record(await tracker.getValue(for: label))
    }
  }

  func decrement(by value: Double) {
    Task {
      await tracker.decrement(value, for: label)
      gauge.record(await tracker.getValue(for: label))
    }
  }
}

struct SwiftMetricsHistogram: MetricHistogram {
  let recorder: Recorder

  func record(_ value: Double) {
    recorder.record(value)
  }

  func record(_ values: [Double]) {
    for value in values {
      recorder.record(value)
    }
  }
}

struct SwiftMetricsTimer: MetricTimer {
  let timer: Timer

  func recordSeconds(_ duration: Double) {
    timer.recordSeconds(duration)
  }

  func recordNanoseconds(_ duration: UInt64) {
    timer.recordNanoseconds(Int64(duration))
  }

  func record(_ duration: Duration) {
    let nanoseconds = duration.components.seconds * 1_000_000_000 + Int64(
      duration.components.attoseconds / 1_000_000_000)
    timer.recordNanoseconds(nanoseconds)
  }

  func time<T>(_ block: () async throws -> T) async rethrows -> T {
    let start = DispatchTime.now()
    defer {
      let end = DispatchTime.now()
      let nanos = end.uptimeNanoseconds - start.uptimeNanoseconds
      timer.recordNanoseconds(Int64(nanos))
    }
    return try await block()
  }
}

// MARK: - Swift Tracing Adapter

/// Adapter that bridges our TracingProvider protocol to swift-distributed-tracing
final class SwiftTracingAdapter: TracingProvider {
  func startSpan(
    name: String,
    parent: (any Span)?,
    options: SpanOptions?
  ) -> any Span {
    // Create a span using the global tracer from swift-distributed-tracing
    let tracer = InstrumentationSystem.tracer

    // Map our SpanKind to swift-distributed-tracing SpanKind
    let spanKind: Tracing.SpanKind = {
      switch options?.kind {
      case .client:
        return .client
      case .server:
        return .server
      case .producer:
        return .producer
      case .consumer:
        return .consumer
      default:
        return .internal
      }
    }()

    // Create context - either from parent or current
    var context: ServiceContext
    if let parentSpan = parent as? SwiftTracingSpan {
      // Create child span from parent's context
      context = parentSpan.serviceContext
    } else {
      context = ServiceContext.current ?? ServiceContext.topLevel
    }

    // Start span with appropriate context
    let span = tracer.startSpan(
      name,
      context: context,
      ofKind: spanKind
    )

    // Add initial attributes from options
    if let attributes = options?.attributes {
      for (key, value) in attributes {
        span.attributes[key] = value
      }
    }

    return SwiftTracingSpan(span: span, serviceContext: context)
  }

  func injectContext(from span: any Span, into headers: inout [String: String]) {
    // Inject W3C Trace Context headers
    if let swiftSpan = span as? SwiftTracingSpan {
      let context = swiftSpan.context
      
      // Format: version-traceid-spanid-traceflags
      // Version: 00 (current W3C version)
      // TraceID: 32 hex characters (128 bits)
      // SpanID: 16 hex characters (64 bits)
      // TraceFlags: 2 hex characters (8 bits)
      let traceparent = String(
        format: "00-%@-%@-%02x",
        context.traceId.lowercased().padding(toLength: 32, withPad: "0", startingAt: 0),
        context.spanId.lowercased().padding(toLength: 16, withPad: "0", startingAt: 0),
        context.traceFlags
      )
      
      headers[TraceContextHeaders.traceparent] = traceparent
      
      if let traceState = context.traceState {
        headers[TraceContextHeaders.tracestate] = traceState
      }
    }
  }

  func extractContext(from headers: [String: String]) -> SpanContext? {
    // Extract W3C trace context from headers
    guard let traceparent = headers[TraceContextHeaders.traceparent] else {
      return nil
    }

    // Parse W3C traceparent header
    // Format: version-traceid-spanid-traceflags
    let components = traceparent.split(separator: "-")
    guard components.count >= 4 else { return nil }
    
    // Validate version
    guard components[0] == "00" else { return nil }
    
    // Validate trace ID (32 hex chars)
    let traceId = String(components[1])
    guard traceId.count == 32,
          traceId.allSatisfy({ $0.isHexDigit }) else { return nil }
    
    // Validate span ID (16 hex chars)
    let spanId = String(components[2])
    guard spanId.count == 16,
          spanId.allSatisfy({ $0.isHexDigit }) else { return nil }
    
    // Parse trace flags
    let traceFlags = UInt8(String(components[3]), radix: 16) ?? 0

    return SpanContext(
      traceId: traceId,
      spanId: spanId,
      traceFlags: traceFlags,
      traceState: headers[TraceContextHeaders.tracestate],
      isRemote: true
    )
  }
}

// MARK: - Swift Tracing Span Wrapper

/// Wrapper around swift-distributed-tracing Span that maintains context
struct SwiftTracingSpan: Span {
  var span: any Tracing.Span
  let serviceContext: ServiceContext
  
  // Store IDs that are generated when span is created
  private let spanId: String
  private let traceId: String
  
  init(span: any Tracing.Span, serviceContext: ServiceContext) {
    self.span = span
    self.serviceContext = serviceContext
    
    // Generate IDs for this span
    self.traceId = Self.generateTraceId()
    self.spanId = Self.generateSpanId()
  }
  
  private static func generateTraceId() -> String {
    // Generate 128-bit trace ID (32 hex characters)
    let uuid = UUID()
    return uuid.uuidString.replacingOccurrences(of: "-", with: "").lowercased()
  }
  
  private static func generateSpanId() -> String {
    // Generate 64-bit span ID (16 hex characters)
    let uuid = UUID()
    return String(uuid.uuidString.replacingOccurrences(of: "-", with: "").prefix(16)).lowercased()
  }

  var context: SpanContext {
    // Return the stored context
    SpanContext(
      traceId: traceId,
      spanId: spanId,
      traceFlags: 1,  // Sampled flag set
      traceState: nil
    )
  }

  func setAttribute(key: String, value: String) {
    span.attributes[key] = value
  }

  func setAttribute(key: String, value: Int) {
    span.attributes[key] = value
  }

  func setAttribute(key: String, value: Double) {
    span.attributes[key] = value
  }

  func setAttribute(key: String, value: Bool) {
    span.attributes[key] = value
  }

  func addEvent(name: String, attributes: [String: String]?) {
    // Convert string attributes to SpanAttributes
    var spanAttributes = SpanAttributes()
    if let attributes = attributes {
      for (key, value) in attributes {
        spanAttributes[key] = value
      }
    }
    span.addEvent(SpanEvent(name: name, attributes: spanAttributes))
  }

  func setStatus(_ status: SpanStatus) {
    switch status {
    case .ok:
      span.setStatus(.init(code: .ok))
    case .error(let message):
      span.setStatus(.init(code: .error, message: message))
    case .unset:
      span.setStatus(.init(code: .ok))
    }
  }

  func end() {
    span.end()
  }

  func end(at time: Date) {
    span.end();
  }
}