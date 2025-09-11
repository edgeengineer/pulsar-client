import Foundation

/// Represents the status of a span
public enum SpanStatus: Sendable {
  case unset
  case ok
  case error(String)
}

/// Represents the kind of span
public enum SpanKind: Sendable {
  case unspecified
  case `internal`
  case server
  case client
  case producer
  case consumer
}

/// Context for a span - used for context propagation
public struct SpanContext: Sendable {
  public let traceId: String
  public let spanId: String
  public let traceFlags: UInt8
  public let traceState: String?
  public let isRemote: Bool

  public init(
    traceId: String,
    spanId: String,
    traceFlags: UInt8 = 0,
    traceState: String? = nil,
    isRemote: Bool = false
  ) {
    self.traceId = traceId
    self.spanId = spanId
    self.traceFlags = traceFlags
    self.traceState = traceState
    self.isRemote = isRemote
  }
}

/// Protocol for a span in distributed tracing
public protocol Span: Sendable {
  /// The span's context
  var context: SpanContext { get }

  /// Set an attribute on the span
  func setAttribute(key: String, value: String)
  func setAttribute(key: String, value: Int)
  func setAttribute(key: String, value: Double)
  func setAttribute(key: String, value: Bool)

  /// Add an event to the span
  func addEvent(name: String, attributes: [String: String]?)

  /// Set the span status
  func setStatus(_ status: SpanStatus)

  /// End the span
  func end()

  /// End the span at a specific time
  func end(at time: Date)
}

/// Extension providing convenience methods
extension Span {
  /// Add an event without attributes
  public func addEvent(name: String) {
    addEvent(name: name, attributes: nil)
  }

  /// Record an exception
  public func recordException(_ error: Error) {
    addEvent(
      name: "exception",
      attributes: [
        "exception.type": String(describing: type(of: error)),
        "exception.message": error.localizedDescription,
      ]
    )
    setStatus(.error(error.localizedDescription))
  }
}

/// Options for creating a span
public struct SpanOptions: Sendable {
  public let kind: SpanKind
  public let attributes: [String: String]
  public let startTime: Date?

  public init(
    kind: SpanKind = .internal,
    attributes: [String: String] = [:],
    startTime: Date? = nil
  ) {
    self.kind = kind
    self.attributes = attributes
    self.startTime = startTime
  }
}

/// Protocol for providing distributed tracing capabilities
public protocol TracingProvider: Sendable {
  /// Start a new span
  /// - Parameters:
  ///   - name: The span name
  ///   - parent: Optional parent span for creating a child span
  ///   - options: Options for creating the span
  /// - Returns: A new span
  func startSpan(
    name: String,
    parent: (any Span)?,
    options: SpanOptions?
  ) -> any Span

  /// Inject trace context into headers for propagation
  /// - Parameters:
  ///   - span: The span whose context to inject
  ///   - headers: Headers dictionary to inject into
  func injectContext(from span: any Span, into headers: inout [String: String])

  /// Extract trace context from headers
  /// - Parameter headers: Headers containing trace context
  /// - Returns: Extracted span context if present
  func extractContext(from headers: [String: String]) -> SpanContext?
}

/// Extension providing convenience methods
extension TracingProvider {
  /// Start a span without parent or options
  public func startSpan(name: String) -> any Span {
    startSpan(name: name, parent: nil, options: nil)
  }

  /// Start a span with parent but no options
  public func startSpan(name: String, parent: any Span) -> any Span {
    startSpan(name: name, parent: parent, options: nil)
  }

  /// Start a span with specific kind
  public func startSpan(name: String, kind: SpanKind) -> any Span {
    startSpan(name: name, parent: nil, options: SpanOptions(kind: kind))
  }
}

/// Standard span names and attributes for Pulsar client
public enum PulsarSpans {
  /// Span names
  public enum Names {
    public static let send = "pulsar.send"
    public static let receive = "pulsar.receive"
    public static let acknowledge = "pulsar.acknowledge"
    public static let connect = "pulsar.connect"
    public static let lookup = "pulsar.lookup"
    public static let retry = "pulsar.retry"
  }

  /// Span attributes
  public enum Attributes {
    public static let topic = "pulsar.topic"
    public static let producerName = "pulsar.producer.name"
    public static let consumerName = "pulsar.consumer.name"
    public static let subscription = "pulsar.subscription"
    public static let messageId = "pulsar.message.id"
    public static let messageSize = "pulsar.message.size"
    public static let batchSize = "pulsar.batch.size"
    public static let compressionType = "pulsar.compression.type"
    public static let retryCount = "pulsar.retry.count"
    public static let brokerUrl = "pulsar.broker.url"
  }
}

/// W3C Trace Context headers
public enum TraceContextHeaders {
  public static let traceparent = "traceparent"
  public static let tracestate = "tracestate"
}
