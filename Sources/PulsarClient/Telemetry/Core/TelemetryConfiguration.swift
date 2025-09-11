import Foundation

/// Configuration for telemetry in the Pulsar client
public struct TelemetryConfiguration: Sendable {
  /// Whether telemetry is enabled
  public let enabled: Bool

  /// The telemetry provider to use
  public let provider: (any TelemetryProvider)?

  /// Interval for metrics collection/reporting (in seconds)
  public let metricsInterval: TimeInterval

  /// Sample rate for tracing (0.0 to 1.0)
  public let tracingSampleRate: Double

  /// Custom dimensions to add to all metrics
  public let customDimensions: MetricDimensions

  /// Whether to enable detailed metrics (may have performance impact)
  public let enableDetailedMetrics: Bool

  /// Whether to enable auto-instrumentation of interceptors
  public let enableInterceptorInstrumentation: Bool

  /// Service name for telemetry identification
  public let serviceName: String

  /// Service version for telemetry identification
  public let serviceVersion: String?

  /// Service instance ID for telemetry identification
  public let serviceInstanceId: String

  /// Default initializer with sensible defaults
  public init(
    enabled: Bool = false,
    provider: (any TelemetryProvider)? = nil,
    metricsInterval: TimeInterval = 60.0,
    tracingSampleRate: Double = 0.1,
    customDimensions: MetricDimensions = [:],
    enableDetailedMetrics: Bool = false,
    enableInterceptorInstrumentation: Bool = true,
    serviceName: String = "pulsar-client",
    serviceVersion: String? = nil,
    serviceInstanceId: String? = nil
  ) {
    self.enabled = enabled
    self.provider = provider
    self.metricsInterval = metricsInterval
    self.tracingSampleRate = min(max(tracingSampleRate, 0.0), 1.0)  // Clamp to 0.0-1.0
    self.customDimensions = customDimensions
    self.enableDetailedMetrics = enableDetailedMetrics
    self.enableInterceptorInstrumentation = enableInterceptorInstrumentation
    self.serviceName = serviceName
    self.serviceVersion = serviceVersion
    self.serviceInstanceId = serviceInstanceId ?? UUID().uuidString
  }

  /// Create a disabled configuration
  public static var disabled: TelemetryConfiguration {
    TelemetryConfiguration(enabled: false)
  }

  /// Create a configuration with default settings and a provider
  public static func withProvider(_ provider: any TelemetryProvider) -> TelemetryConfiguration {
    TelemetryConfiguration(enabled: true, provider: provider)
  }
}

/// Builder for telemetry configuration
public class TelemetryConfigurationBuilder {
  private var enabled = false
  private var provider: (any TelemetryProvider)?
  private var metricsInterval: TimeInterval = 60.0
  private var tracingSampleRate = 0.1
  private var customDimensions: MetricDimensions = [:]
  private var enableDetailedMetrics = false
  private var enableInterceptorInstrumentation = true
  private var serviceName = "pulsar-client"
  private var serviceVersion: String?
  private var serviceInstanceId: String?

  public init() {}

  /// Enable telemetry
  @discardableResult
  public func enable(_ enabled: Bool = true) -> Self {
    self.enabled = enabled
    return self
  }

  /// Set the telemetry provider
  @discardableResult
  public func provider(_ provider: any TelemetryProvider) -> Self {
    self.provider = provider
    self.enabled = true  // Auto-enable when provider is set
    return self
  }

  /// Set metrics collection interval
  @discardableResult
  public func metricsInterval(_ interval: TimeInterval) -> Self {
    self.metricsInterval = interval
    return self
  }

  /// Set tracing sample rate (0.0 to 1.0)
  @discardableResult
  public func tracingSampleRate(_ rate: Double) -> Self {
    self.tracingSampleRate = rate
    return self
  }

  /// Add custom dimensions to all metrics
  @discardableResult
  public func customDimensions(_ dimensions: MetricDimensions) -> Self {
    self.customDimensions = dimensions
    return self
  }

  /// Add a single custom dimension
  @discardableResult
  public func addDimension(key: String, value: String) -> Self {
    self.customDimensions[key] = value
    return self
  }

  /// Enable detailed metrics collection
  @discardableResult
  public func enableDetailedMetrics(_ enable: Bool = true) -> Self {
    self.enableDetailedMetrics = enable
    return self
  }

  /// Enable interceptor auto-instrumentation
  @discardableResult
  public func enableInterceptorInstrumentation(_ enable: Bool = true) -> Self {
    self.enableInterceptorInstrumentation = enable
    return self
  }

  /// Set service identification
  @discardableResult
  public func service(name: String, version: String? = nil, instanceId: String? = nil) -> Self {
    self.serviceName = name
    self.serviceVersion = version
    self.serviceInstanceId = instanceId
    return self
  }

  /// Build the configuration
  public func build() -> TelemetryConfiguration {
    TelemetryConfiguration(
      enabled: enabled,
      provider: provider,
      metricsInterval: metricsInterval,
      tracingSampleRate: tracingSampleRate,
      customDimensions: customDimensions,
      enableDetailedMetrics: enableDetailedMetrics,
      enableInterceptorInstrumentation: enableInterceptorInstrumentation,
      serviceName: serviceName,
      serviceVersion: serviceVersion,
      serviceInstanceId: serviceInstanceId
    )
  }
}
