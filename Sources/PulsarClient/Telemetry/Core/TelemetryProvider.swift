import Foundation

/// Main protocol for telemetry providers that combine metrics and tracing capabilities
public protocol TelemetryProvider: Sendable {
  /// The metrics provider for collecting metrics
  var metrics: any MetricsProvider { get }

  /// The tracing provider for distributed tracing
  var tracing: any TracingProvider { get }

  /// Configure the telemetry provider
  func configure() async throws

  /// Shutdown the telemetry provider and clean up resources
  func shutdown() async
}

/// Extension providing default implementations
extension TelemetryProvider {
  /// Default configuration does nothing
  public func configure() async throws {
    // Default implementation: no-op
  }

  /// Default shutdown does nothing
  public func shutdown() async {
    // Default implementation: no-op
  }
}
