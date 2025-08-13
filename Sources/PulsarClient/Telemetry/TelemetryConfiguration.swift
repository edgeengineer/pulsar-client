import Foundation
import Metrics
import Tracing

/// Configuration for telemetry features in the Pulsar client
public struct TelemetryConfiguration: Sendable {
    
    // MARK: - Metrics Configuration
    
    /// Enable metrics collection
    public var metricsEnabled: Bool = false
    
    /// Metrics export configuration
    public var metricsExporter: MetricsExporter = .none
    
    /// Custom metrics handler factory name (optional)
    public var customMetricsHandlerFactory: String?
    
    /// Metrics collection interval (for periodic exports)
    public var metricsCollectionInterval: TimeInterval = 60.0
    
    // MARK: - Tracing Configuration
    
    /// Enable distributed tracing
    public var tracingEnabled: Bool = false
    
    /// Tracing sampler configuration
    public var tracingSampler: TracingSampler = .alwaysOff
    
    /// Attach trace context to messages
    public var attachTraceToMessages: Bool = false
    
    /// Link consumer traces to producer traces
    public var linkConsumerTraces: Bool = false
    
    /// Traces export configuration
    public var tracesExporter: TracesExporter = .none
    
    /// Custom tracer (optional)
    public var customTracer: (any Tracer)?
    
    // MARK: - Advanced Configuration
    
    /// Enable detailed performance metrics
    public var detailedMetricsEnabled: Bool = false
    
    /// Maximum number of metric samples to keep in memory
    public var maxMetricSamples: Int = 1000
    
    /// Enable metrics for connection pool
    public var connectionPoolMetricsEnabled: Bool = true
    
    /// Enable metrics for message batching
    public var batchingMetricsEnabled: Bool = true
    
    /// Custom tags to add to all metrics and traces
    public var globalTags: [String: String] = [:]
    
    // MARK: - Initialization
    
    public init() {}
    
    /// Create a configuration with metrics enabled
    public static func withMetrics(exporter: MetricsExporter = .logging()) -> TelemetryConfiguration {
        var config = TelemetryConfiguration()
        config.metricsEnabled = true
        config.metricsExporter = exporter
        return config
    }
    
    /// Create a configuration with tracing enabled
    public static func withTracing(
        sampler: TracingSampler = .probability(0.1),
        exporter: TracesExporter = .logging()
    ) -> TelemetryConfiguration {
        var config = TelemetryConfiguration()
        config.tracingEnabled = true
        config.tracingSampler = sampler
        config.tracesExporter = exporter
        return config
    }
    
    /// Create a configuration with both metrics and tracing enabled
    public static func full(
        metricsExporter: MetricsExporter = .logging(),
        tracesExporter: TracesExporter = .logging(),
        sampler: TracingSampler = .probability(0.1)
    ) -> TelemetryConfiguration {
        var config = TelemetryConfiguration()
        config.metricsEnabled = true
        config.metricsExporter = metricsExporter
        config.tracingEnabled = true
        config.tracingSampler = sampler
        config.tracesExporter = tracesExporter
        config.attachTraceToMessages = true
        config.linkConsumerTraces = true
        return config
    }
}

// MARK: - Metrics Exporter

/// Configuration for metrics export
public enum MetricsExporter: Sendable {
    /// No metrics export
    case none
    
    /// Export metrics to logs
    case logging(level: LogLevel = .info)
    
    /// Export metrics to Prometheus
    case prometheus(port: Int = 9090, path: String = "/metrics")
    
    /// Export metrics via OpenTelemetry Protocol (OTLP)
    case otlp(endpoint: String, headers: [String: String] = [:])
    
    /// Export metrics to StatsD
    case statsd(host: String = "localhost", port: Int = 8125)
}

// MARK: - Traces Exporter

/// Configuration for traces export
public enum TracesExporter: Sendable {
    /// No traces export
    case none
    
    /// Export traces to logs
    case logging(level: LogLevel = .info)
    
    /// Export traces to Jaeger
    case jaeger(endpoint: String, serviceName: String = "pulsar-client")
    
    /// Export traces to Zipkin
    case zipkin(endpoint: String, serviceName: String = "pulsar-client")
    
    /// Export traces via OpenTelemetry Protocol (OTLP)
    case otlp(endpoint: String, headers: [String: String] = [:])
    
    /// Export traces to AWS X-Ray
    case xray(region: String)
    
    /// Custom exporter
    case custom(handler: (any Tracer))
}

// MARK: - Tracing Sampler

/// Configuration for trace sampling
public indirect enum TracingSampler: Sendable {
    /// Always sample (record all traces)
    case alwaysOn
    
    /// Never sample (record no traces)
    case alwaysOff
    
    /// Sample based on probability (0.0 to 1.0)
    case probability(Double)
    
    /// Sample based on rate limit (traces per second)
    case rateLimited(tracesPerSecond: Int)
    
    /// Parent-based sampling (follow parent's decision)
    case parentBased(root: TracingSampler)
    
    /// Custom sampling logic
    case custom(sampler: @Sendable (SamplingContext) -> Bool)
    
    /// Determine if a trace should be sampled
    public func shouldSample(context: SamplingContext) -> Bool {
        switch self {
        case .alwaysOn:
            return true
        case .alwaysOff:
            return false
        case .probability(let probability):
            return Double.random(in: 0..<1) < probability
        case .rateLimited(let tracesPerSecond):
            // Simplified rate limiting - in production, use a token bucket
            return Int.random(in: 0..<1000) < tracesPerSecond
        case .parentBased(let root):
            if context.hasParent {
                return context.parentSampled
            } else {
                return root.shouldSample(context: context)
            }
        case .custom(let sampler):
            return sampler(context)
        }
    }
}

// MARK: - Sampling Context

/// Context for making sampling decisions
public struct SamplingContext: Sendable {
    public let operationName: String
    public let kind: SpanKind
    public let hasParent: Bool
    public let parentSampled: Bool
    public let attributes: [String: String]
    
    public init(
        operationName: String,
        kind: SpanKind,
        hasParent: Bool = false,
        parentSampled: Bool = false,
        attributes: [String: String] = [:]
    ) {
        self.operationName = operationName
        self.kind = kind
        self.hasParent = hasParent
        self.parentSampled = parentSampled
        self.attributes = attributes
    }
}

// MARK: - Log Level

/// Log level for telemetry exports
public enum LogLevel: String, Sendable {
    case trace
    case debug
    case info
    case warning
    case error
    case critical
}

// MARK: - Builder Extension

public extension PulsarClientBuilder {
    
    /// Configure telemetry for the Pulsar client
    func telemetry(_ configure: (inout TelemetryConfiguration) -> Void) -> PulsarClientBuilder {
        var config = TelemetryConfiguration()
        configure(&config)
        
        // Apply the configuration
        if config.metricsEnabled {
            PulsarMeter.shared.enable()
            setupMetricsExporter(config.metricsExporter)
        }
        
        if config.tracingEnabled {
            PulsarActivitySource.shared.enable(
                attachTraceToMessages: config.attachTraceToMessages,
                linkConsumerTraces: config.linkConsumerTraces
            )
            setupTracesExporter(config.tracesExporter)
        }
        
        // TODO: Store configuration in the builder when PulsarClientBuilder is updated
        // For now, the configuration is applied immediately above
        
        return self
    }
    
    private func setupMetricsExporter(_ exporter: MetricsExporter) {
        switch exporter {
        case .none:
            break
        case .logging(let level):
            // Setup logging metrics handler
            let handler = LoggingMetricsHandler(logLevel: level.rawValue)
            MetricsSystem.bootstrap(handler)
        case .prometheus:
            // Setup Prometheus metrics handler
            // This would require a Prometheus Swift library
            break
        case .otlp:
            // Setup OTLP metrics handler
            // This would require OpenTelemetry Swift SDK
            break
        case .statsd:
            // Setup StatsD metrics handler
            // This would require a StatsD Swift library
            break
        }
    }
    
    private func setupTracesExporter(_ exporter: TracesExporter) {
        switch exporter {
        case .none:
            break
        case .logging(let level):
            // Setup logging tracer
            let tracer = LoggingTracer(logLevel: level.rawValue)
            InstrumentationSystem.bootstrap(tracer)
        case .jaeger:
            // Setup Jaeger tracer
            // This would require a Jaeger Swift library
            break
        case .zipkin:
            // Setup Zipkin tracer
            // This would require a Zipkin Swift library
            break
        case .otlp:
            // Setup OTLP tracer
            // This would require OpenTelemetry Swift SDK
            break
        case .xray:
            // Setup AWS X-Ray tracer
            // This would require AWS SDK
            break
        case .custom(let tracer):
            InstrumentationSystem.bootstrap(tracer)
        }
    }
}

// MARK: - Logging Handlers (Simplified implementations)

/// Simple logging metrics handler for development
private struct LoggingMetricsHandler: MetricsFactory {
    let logLevel: String
    
    func makeCounter(label: String, dimensions: [(String, String)]) -> any CounterHandler {
        NOOPMetricsHandler.instance
    }
    
    func makeRecorder(label: String, dimensions: [(String, String)], aggregate: Bool) -> any RecorderHandler {
        NOOPMetricsHandler.instance
    }
    
    func makeTimer(label: String, dimensions: [(String, String)]) -> any TimerHandler {
        NOOPMetricsHandler.instance
    }
    
    func destroyCounter(_ handler: any CounterHandler) {}
    func destroyRecorder(_ handler: any RecorderHandler) {}
    func destroyTimer(_ handler: any TimerHandler) {}
}

/// Simple logging tracer for development
private struct LoggingTracer: Tracer {
    typealias Span = NoOpTracer.NoOpSpan
    
    let logLevel: String
    
    func startSpan(
        _ operationName: String,
        context: ServiceContext,
        ofKind kind: SpanKind,
        at instant: @autoclosure () -> some TracerInstant = DefaultTracerClock.now,
        links: [SpanLink] = []
    ) -> NoOpTracer.NoOpSpan {
        NoOpTracer.NoOpSpan(context: context)
    }
    
    func forceFlush() {}
    
    func extract<Carrier, Extract>(
        _ carrier: Carrier,
        into context: inout ServiceContext,
        using extractor: Extract
    ) where Extract: Extractor, Carrier == Extract.Carrier {
        // No-op implementation
    }
    
    func inject<Carrier, Inject>(
        _ context: ServiceContext,
        into carrier: inout Carrier,
        using injector: Inject
    ) where Inject: Injector, Carrier == Inject.Carrier {
        // No-op implementation
    }
}

