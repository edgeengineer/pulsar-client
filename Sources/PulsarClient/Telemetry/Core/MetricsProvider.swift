import Foundation

/// Protocol for metric counters - monotonically increasing values
public protocol MetricCounter: Sendable {
  /// Increment the counter by 1
  func increment()

  /// Increment the counter by a specific value
  func increment(by value: Int64)
}

/// Protocol for metric gauges - values that can go up and down
public protocol MetricGauge: Sendable {
  /// Set the gauge to a specific value
  func record(_ value: Double)

  /// Increment the gauge by a value
  func increment(by value: Double)

  /// Decrement the gauge by a value
  func decrement(by value: Double)
}

/// Protocol for metric histograms - distribution of values
public protocol MetricHistogram: Sendable {
  /// Record a value in the histogram
  func record(_ value: Double)

  /// Record multiple values
  func record(_ values: [Double])
}

/// Protocol for metric timers - measuring durations
public protocol MetricTimer: Sendable {
  /// Record a duration in seconds
  func recordSeconds(_ duration: Double)

  /// Record a duration in nanoseconds
  func recordNanoseconds(_ duration: UInt64)

  /// Record a duration
  func record(_ duration: Duration)

  /// Time a block of code and record the duration
  func time<T>(_ block: () async throws -> T) async rethrows -> T
}

/// Dimensions (tags/labels) for metrics
public typealias MetricDimensions = [String: String]

/// Protocol for providing metrics collection capabilities
public protocol MetricsProvider: Sendable {
  /// Create or get a counter metric
  /// - Parameters:
  ///   - name: The metric name
  ///   - dimensions: Optional dimensions/tags for the metric
  /// - Returns: A counter metric
  func counter(name: String, dimensions: MetricDimensions?) -> any MetricCounter

  /// Create or get a gauge metric
  /// - Parameters:
  ///   - name: The metric name
  ///   - dimensions: Optional dimensions/tags for the metric
  /// - Returns: A gauge metric
  func gauge(name: String, dimensions: MetricDimensions?) -> any MetricGauge

  /// Create or get a histogram metric
  /// - Parameters:
  ///   - name: The metric name
  ///   - dimensions: Optional dimensions/tags for the metric
  /// - Returns: A histogram metric
  func histogram(name: String, dimensions: MetricDimensions?) -> any MetricHistogram

  /// Create or get a timer metric
  /// - Parameters:
  ///   - name: The metric name
  ///   - dimensions: Optional dimensions/tags for the metric
  /// - Returns: A timer metric
  func timer(name: String, dimensions: MetricDimensions?) -> any MetricTimer
}

/// Extension providing convenience methods
extension MetricsProvider {
  /// Create a counter without dimensions
  public func counter(name: String) -> any MetricCounter {
    counter(name: name, dimensions: nil)
  }

  /// Create a gauge without dimensions
  public func gauge(name: String) -> any MetricGauge {
    gauge(name: name, dimensions: nil)
  }

  /// Create a histogram without dimensions
  public func histogram(name: String) -> any MetricHistogram {
    histogram(name: name, dimensions: nil)
  }

  /// Create a timer without dimensions
  public func timer(name: String) -> any MetricTimer {
    timer(name: name, dimensions: nil)
  }
}

/// Standard metric names for Pulsar client
public enum PulsarMetrics {
  /// Client metrics
  public enum Client {
    public static let connectionsActive = "pulsar.client.connections.active"
    public static let connectionsTotal = "pulsar.client.connections.total"
    public static let connectionsFailed = "pulsar.client.connections.failed"
    public static let lookupRequests = "pulsar.client.lookup.requests"
    public static let lookupLatency = "pulsar.client.lookup.latency"
  }

  /// Producer metrics
  public enum Producer {
    public static let messagesSent = "pulsar.producer.messages.sent"
    public static let messagesFailed = "pulsar.producer.messages.failed"
    public static let messagesPending = "pulsar.producer.messages.pending"
    public static let batchSize = "pulsar.producer.batch.size"
    public static let sendLatency = "pulsar.producer.send.latency"
    public static let acksLatency = "pulsar.producer.acks.latency"
  }

  /// Consumer metrics
  public enum Consumer {
    public static let messagesReceived = "pulsar.consumer.messages.received"
    public static let messagesAcknowledged = "pulsar.consumer.messages.acknowledged"
    public static let messagesNacked = "pulsar.consumer.messages.nacked"
    public static let messagesDlq = "pulsar.consumer.messages.dlq"
    public static let receiveLatency = "pulsar.consumer.receive.latency"
    public static let processLatency = "pulsar.consumer.process.latency"
    public static let backlog = "pulsar.consumer.backlog"
  }

  /// Connection pool metrics
  public enum Pool {
    public static let connectionsActive = "pulsar.pool.connections.active"
    public static let connectionsIdle = "pulsar.pool.connections.idle"
    public static let waitTime = "pulsar.pool.wait.time"
  }
}
