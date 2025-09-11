import Foundation

/// Manages telemetry for the Pulsar client
public actor TelemetryManager {
  private let configuration: TelemetryConfiguration
  private let provider: any TelemetryProvider

  // Client-level metrics
  private let connectionsActiveGauge: any MetricGauge
  private let connectionsTotalCounter: any MetricCounter
  private let connectionsFailedCounter: any MetricCounter
  private let lookupRequestsCounter: any MetricCounter
  private let lookupLatencyHistogram: any MetricHistogram

  public init(configuration: TelemetryConfiguration) async throws {
    self.configuration = configuration

    // Use provided provider or NoOp if disabled
    if configuration.enabled, let provider = configuration.provider {
      self.provider = provider
      try await provider.configure()
    } else {
      self.provider = NoOpTelemetryProvider.shared
    }

    // Initialize client metrics with service dimensions
    var baseDimensions = configuration.customDimensions
    baseDimensions["service.name"] = configuration.serviceName
    if let version = configuration.serviceVersion {
      baseDimensions["service.version"] = version
    }
    baseDimensions["service.instance.id"] = configuration.serviceInstanceId

    // Initialize metrics
    self.connectionsActiveGauge = provider.metrics.gauge(
      name: PulsarMetrics.Client.connectionsActive,
      dimensions: baseDimensions
    )
    self.connectionsTotalCounter = provider.metrics.counter(
      name: PulsarMetrics.Client.connectionsTotal,
      dimensions: baseDimensions
    )
    self.connectionsFailedCounter = provider.metrics.counter(
      name: PulsarMetrics.Client.connectionsFailed,
      dimensions: baseDimensions
    )
    self.lookupRequestsCounter = provider.metrics.counter(
      name: PulsarMetrics.Client.lookupRequests,
      dimensions: baseDimensions
    )
    self.lookupLatencyHistogram = provider.metrics.histogram(
      name: PulsarMetrics.Client.lookupLatency,
      dimensions: baseDimensions
    )
  }

  /// Shutdown the telemetry manager
  public func shutdown() async {
    await provider.shutdown()
  }

  /// Get the telemetry provider
  public var telemetryProvider: any TelemetryProvider {
    provider
  }

  /// Check if telemetry is enabled
  public var isEnabled: Bool {
    configuration.enabled
  }

  /// Record a connection established
  public func recordConnectionEstablished() {
    guard configuration.enabled else { return }
    connectionsTotalCounter.increment()
    connectionsActiveGauge.increment(by: 1)
  }

  /// Record a connection closed
  public func recordConnectionClosed() {
    guard configuration.enabled else { return }
    connectionsActiveGauge.decrement(by: 1)
  }

  /// Record a connection failure
  public func recordConnectionFailed() {
    guard configuration.enabled else { return }
    connectionsFailedCounter.increment()
  }

  /// Record a lookup request
  public func recordLookupRequest(duration: TimeInterval) {
    guard configuration.enabled else { return }
    lookupRequestsCounter.increment()
    lookupLatencyHistogram.record(duration)
  }

  /// Create producer interceptors if telemetry is enabled
  public func createProducerInterceptors<T>(
    topic: String,
    producerName: String
  ) -> [any ProducerInterceptor<T>] where T: Sendable {
    guard configuration.enabled else { return [] }

    var interceptors: [any ProducerInterceptor<T>] = []

    // Add metrics interceptor
    interceptors.append(
      ProducerMetricsInterceptor(
        metrics: provider.metrics,
        topic: topic,
        producerName: producerName,
        additionalDimensions: configuration.customDimensions
      )
    )

    // Add tracing interceptor if sampling allows
    if Double.random(in: 0..<1) <= configuration.tracingSampleRate {
      interceptors.append(
        ProducerTracingInterceptor(
          tracing: provider.tracing,
          topic: topic,
          producerName: producerName
        )
      )
    }

    return interceptors
  }

  /// Create consumer interceptors if telemetry is enabled
  public func createConsumerInterceptors<T>(
    topic: String,
    subscription: String,
    consumerName: String
  ) -> [any ConsumerInterceptor<T>] where T: Sendable {
    guard configuration.enabled else { return [] }

    var interceptors: [any ConsumerInterceptor<T>] = []

    // Add metrics interceptor
    interceptors.append(
      ConsumerMetricsInterceptor(
        metrics: provider.metrics,
        topic: topic,
        subscription: subscription,
        consumerName: consumerName,
        additionalDimensions: configuration.customDimensions
      )
    )

    // Add tracing interceptor if sampling allows
    if Double.random(in: 0..<1) <= configuration.tracingSampleRate {
      interceptors.append(
        ConsumerTracingInterceptor(
          tracing: provider.tracing,
          topic: topic,
          subscription: subscription,
          consumerName: consumerName
        )
      )
    }

    return interceptors
  }

  /// Start a trace span for an operation
  public func startSpan(
    name: String,
    kind: SpanKind = .internal,
    attributes: [String: String] = [:]
  ) -> any Span {
    guard configuration.enabled else {
      return NoOpSpan()
    }

    return provider.tracing.startSpan(
      name: name,
      parent: nil,
      options: SpanOptions(kind: kind, attributes: attributes)
    )
  }
}

// MARK: - Global Telemetry Access

/// Global telemetry instance for convenience
/// This should be set by the PulsarClient during initialization
public extension TelemetryManager {
  private static let globalKey = "PulsarClient.TelemetryManager"

  /// Set the global telemetry manager
  static func setGlobal(_ manager: TelemetryManager) {
    Thread.current.threadDictionary[globalKey] = manager
  }

  /// Get the global telemetry manager
  static var global: TelemetryManager? {
    Thread.current.threadDictionary[globalKey] as? TelemetryManager
  }
}