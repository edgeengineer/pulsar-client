# Pulsar Client Telemetry

The Pulsar Swift Client provides comprehensive telemetry support through a pluggable architecture that allows you to integrate with any metrics and tracing backend of your choice.

## Overview

The telemetry system is built on three core protocols:
- **TelemetryProvider**: Main interface combining metrics and tracing
- **MetricsProvider**: Interface for metrics collection (counters, gauges, histograms, timers)
- **TracingProvider**: Interface for distributed tracing with W3C Trace Context support

## Quick Start

### 1. Using the Default (NoOp) Provider

By default, telemetry is disabled with zero overhead:

```swift
let client = PulsarClient.builder { builder in
  builder.withServiceUrl("pulsar://localhost:6650")
  // Telemetry is disabled by default
}
```

### 2. Using Swift Metrics Bridge

The client provides a bridge to integrate with any swift-metrics backend:

```swift
import Metrics
import PulsarClient

// First, bootstrap your preferred metrics backend
// Example with Prometheus:
let prometheus = PrometheusMetricsFactory()
MetricsSystem.bootstrap(prometheus)

// Then configure PulsarClient with telemetry
let client = PulsarClient.builder { builder in
  builder
    .withServiceUrl("pulsar://localhost:6650")
    .withTelemetry { telemetry in
      telemetry
        .provider(SwiftMetricsBridge())
        .tracingSampleRate(0.1)  // Sample 10% of traces
        .service(name: "my-service", version: "1.0.0")
    }
}
```

### 3. Implementing a Custom Provider

You can implement your own telemetry provider by conforming to the protocols:

```swift
class MyCustomTelemetryProvider: TelemetryProvider {
  let metrics: any MetricsProvider
  let tracing: any TracingProvider
  
  init() {
    self.metrics = MyCustomMetricsProvider()
    self.tracing = MyCustomTracingProvider()
  }
  
  func configure() async throws {
    // Initialize your backend connections
  }
  
  func shutdown() async {
    // Clean up resources
  }
}

// Use your custom provider
let client = PulsarClient.builder { builder in
  builder
    .withServiceUrl("pulsar://localhost:6650")
    .withTelemetry { telemetry in
      telemetry.provider(MyCustomTelemetryProvider())
    }
}
```

## Metrics Collected

### Client Metrics
- `pulsar.client.connections.active` - Active connections (Gauge)
- `pulsar.client.connections.total` - Total connections created (Counter)
- `pulsar.client.connections.failed` - Failed connection attempts (Counter)
- `pulsar.client.lookup.requests` - Topic lookup requests (Counter)
- `pulsar.client.lookup.latency` - Topic lookup latency (Histogram)

### Producer Metrics
- `pulsar.producer.messages.sent` - Messages successfully sent (Counter)
- `pulsar.producer.messages.failed` - Messages failed to send (Counter)
- `pulsar.producer.messages.pending` - Messages pending acknowledgment (Gauge)
- `pulsar.producer.batch.size` - Message batch sizes (Histogram)
- `pulsar.producer.send.latency` - Message send latency (Histogram)
- `pulsar.producer.acks.latency` - Acknowledgment latency (Histogram)

### Consumer Metrics
- `pulsar.consumer.messages.received` - Messages received (Counter)
- `pulsar.consumer.messages.acknowledged` - Messages acknowledged (Counter)
- `pulsar.consumer.messages.nacked` - Messages negatively acknowledged (Counter)
- `pulsar.consumer.messages.dlq` - Messages sent to DLQ (Counter)
- `pulsar.consumer.receive.latency` - Message receive latency (Histogram)
- `pulsar.consumer.process.latency` - Message processing latency (Histogram)
- `pulsar.consumer.backlog` - Consumer backlog size (Gauge)

## Distributed Tracing

The telemetry system supports distributed tracing with automatic context propagation:

### Trace Spans
- `pulsar.send` - Producer send operation
- `pulsar.receive` - Consumer receive operation
- `pulsar.acknowledge` - Message acknowledgment
- `pulsar.connect` - Connection establishment
- `pulsar.lookup` - Topic lookup
- `pulsar.retry` - Retry operations

### Context Propagation
Trace context is automatically propagated through message properties using W3C Trace Context format.

## Integration Examples

### OpenTelemetry Integration

```swift
import OpenTelemetryApi
import OpenTelemetrySdk

// Create OpenTelemetry provider wrapper
class OTelProvider: TelemetryProvider {
  let metrics: any MetricsProvider
  let tracing: any TracingProvider
  
  init(endpoint: String) {
    // Configure OTLP exporter
    let exporter = OtlpHttpTraceExporter(endpoint: endpoint)
    
    // Setup tracer provider
    let tracerProvider = TracerProviderBuilder()
      .add(spanProcessor: BatchSpanProcessor(spanExporter: exporter))
      .build()
    
    OpenTelemetry.registerTracerProvider(tracerProvider: tracerProvider)
    
    self.metrics = OTelMetricsAdapter()
    self.tracing = OTelTracingAdapter()
  }
}

// Use with PulsarClient
let client = PulsarClient.builder { builder in
  builder
    .withTelemetry { telemetry in
      telemetry.provider(OTelProvider(endpoint: "http://localhost:4318"))
    }
}
```

### Prometheus Integration

```swift
import PrometheusMetrics

// Bootstrap Prometheus
let registry = PrometheusRegistry()
MetricsSystem.bootstrap(PrometheusMetricsFactory(registry: registry))

// Configure PulsarClient
let client = PulsarClient.builder { builder in
  builder
    .withTelemetry { telemetry in
      telemetry
        .provider(SwiftMetricsBridge())
        .metricsInterval(60)  // Report every 60 seconds
    }
}

// Expose metrics endpoint
let metricsEndpoint = registry.collect()
```

### Datadog Integration

```swift
// Create Datadog provider wrapper
class DatadogProvider: TelemetryProvider {
  // Implementation details for Datadog APM integration
}

let client = PulsarClient.builder { builder in
  builder
    .withTelemetry { telemetry in
      telemetry
        .provider(DatadogProvider(apiKey: "your-api-key"))
        .service(name: "pulsar-service", version: "1.0.0")
        .addDimension(key: "environment", value: "production")
    }
}
```

## Configuration Options

### TelemetryConfiguration

| Option | Description | Default |
|--------|-------------|---------|
| `enabled` | Enable/disable telemetry | `false` |
| `provider` | Telemetry provider implementation | `NoOpTelemetryProvider` |
| `metricsInterval` | Metrics collection interval (seconds) | `60.0` |
| `tracingSampleRate` | Trace sampling rate (0.0-1.0) | `0.1` |
| `customDimensions` | Additional dimensions for all metrics | `[:]` |
| `enableDetailedMetrics` | Enable detailed/expensive metrics | `false` |
| `serviceName` | Service name for identification | `"pulsar-client"` |
| `serviceVersion` | Service version | `nil` |
| `serviceInstanceId` | Unique instance identifier | Auto-generated UUID |

## Performance Considerations

1. **Zero-Cost When Disabled**: When telemetry is disabled, the NoOp provider ensures zero overhead
2. **Sampling**: Use trace sampling to reduce overhead in production
3. **Batching**: Metrics are batched according to your backend's configuration
4. **Async Collection**: All telemetry operations are non-blocking

## Troubleshooting

### Metrics Not Appearing
- Verify telemetry is enabled in configuration
- Check that your metrics backend is properly bootstrapped
- Ensure the metrics endpoint/collector is accessible

### High Memory Usage
- Reduce `tracingSampleRate` to sample fewer traces
- Disable `enableDetailedMetrics` if not needed
- Check for metric cardinality explosion in dimensions

### Missing Trace Context
- Verify both producer and consumer have tracing enabled
- Check that trace sampling didn't exclude the operation
- Ensure your tracing backend supports W3C Trace Context

## Advanced Usage

### Custom Dimensions

Add custom dimensions to all metrics:

```swift
builder.withTelemetry { telemetry in
  telemetry
    .customDimensions([
      "datacenter": "us-west-2",
      "team": "platform",
      "service.namespace": "messaging"
    ])
}
```

### Selective Metric Collection

Enable only specific metrics:

```swift
class SelectiveMetricsProvider: MetricsProvider {
  func counter(name: String, dimensions: MetricDimensions?) -> any MetricCounter {
    // Only collect send/receive metrics
    if name.contains("messages.sent") || name.contains("messages.received") {
      return ActualCounter(name: name, dimensions: dimensions)
    }
    return NoOpCounter()
  }
  // ... implement other methods
}
```

### Dynamic Configuration

Update telemetry configuration at runtime:

```swift
// Start with disabled telemetry
var config = TelemetryConfiguration.disabled

// Enable telemetry later
config = TelemetryConfiguration.withProvider(SwiftMetricsBridge())

// Recreate client with new configuration
client = PulsarClient.builder { builder in
  builder.withTelemetry(config)
}
```