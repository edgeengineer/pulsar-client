# Pulsar Client Telemetry

The Pulsar Swift Client automatically emits metrics and traces using the standard Swift observability libraries - no configuration needed!

## Overview

The telemetry system uses:
- **swift-metrics** for metrics (counters, gauges, timers, recorders)
- **swift-distributed-tracing** for distributed tracing

When you bootstrap a metrics or tracing backend, the Pulsar client automatically starts emitting telemetry.

## Quick Start

### Step 1: Bootstrap Your Backend

Choose any backend that works with swift-metrics and/or swift-distributed-tracing:

```swift
import Metrics
import Prometheus  // or StatsD, OpenTelemetry, etc.

// Bootstrap your preferred backend once at app startup
let prometheus = PrometheusMetricsFactory()
MetricsSystem.bootstrap(prometheus)

// That's it! Pulsar will automatically emit metrics
```

### Step 2: Use PulsarClient Normally

```swift
// Create client
let client = PulsarClient.builder { builder in
  builder.withServiceUrl("pulsar://localhost:6650")
}

// Create producer/consumer - metrics flow automatically
let producer = try await client.newProducer { builder in
  builder.topic("my-topic")
}

// All operations are automatically measured
_ = try await producer.send("Hello, World!")
```

## Metrics Reference

All metrics are emitted automatically when a metrics backend is configured.

### Client Metrics
- `pulsar.client.connections.active` - Active connections (Gauge)
- `pulsar.client.connections.total` - Total connections created (Counter)
- `pulsar.client.connections.failed` - Failed connection attempts (Counter)
- `pulsar.client.lookup.requests` - Topic lookup requests (Counter)
- `pulsar.client.lookup.latency` - Topic lookup latency (Timer)
- `pulsar.pool.connections.active` - Active pooled connections (Gauge)
- `pulsar.pool.connections.idle` - Idle pooled connections (Gauge)
- `pulsar.pool.wait.time` - Connection pool wait time (Timer)

### Producer Metrics
All producer metrics include dimensions: `topic`, `producer`

- `pulsar.producer.messages.sent` - Messages successfully sent (Counter)
- `pulsar.producer.messages.failed` - Messages failed to send (Counter)
- `pulsar.producer.messages.pending` - Messages pending acknowledgment (Gauge)
- `pulsar.producer.send.latency` - Message send latency (Timer)
- `pulsar.producer.batch.size` - Message batch sizes (Recorder)

### Consumer Metrics
All consumer metrics include dimensions: `topic`, `subscription`, `consumer`

- `pulsar.consumer.messages.received` - Messages received (Counter)
- `pulsar.consumer.messages.acknowledged` - Messages acknowledged (Counter)
- `pulsar.consumer.messages.nacked` - Messages negatively acknowledged (Counter)
- `pulsar.consumer.receive.latency` - Message receive latency (Timer)
- `pulsar.consumer.process.latency` - Message processing latency (Timer)
- `pulsar.consumer.backlog` - Consumer backlog size (Gauge)

## Distributed Tracing

When you bootstrap a tracing backend, spans are automatically created:

```swift
import Tracing
import OpenTelemetry

// Bootstrap tracing
let tracer = OTelTracer()
InstrumentationSystem.bootstrap(tracer)

// All Pulsar operations are now traced!
```

### Trace Spans
- `pulsar.send` - Producer send operations
- `pulsar.receive` - Consumer receive operations
- `pulsar.acknowledge` - Message acknowledgments
- `pulsar.connect` - Connection establishment
- `pulsar.lookup` - Topic lookups

Each span includes relevant attributes like topic, producer/consumer name, message ID, etc.

## Backend Examples

### Prometheus

```swift
import Metrics
import Prometheus

// Bootstrap Prometheus
let prometheus = PrometheusMetricsFactory()
MetricsSystem.bootstrap(prometheus)

// Use PulsarClient normally - metrics flow automatically
let client = PulsarClient.builder { builder in
  builder.withServiceUrl("pulsar://localhost:6650")
}

// Expose metrics endpoint
app.get("/metrics") { req in
  prometheus.collect()
}
```


### OpenTelemetry

```swift
import Metrics
import Tracing
import OpenTelemetryApi
import OpenTelemetrySdk

// Bootstrap OpenTelemetry metrics
let metricsExporter = OtlpHttpMetricExporter(endpoint: "http://localhost:4318")
let meterProvider = MeterProviderBuilder()
  .with(metricReader: PeriodicMetricReader(exporter: metricsExporter))
  .build()
meterProvider.setAsGlobal()

// Bootstrap OpenTelemetry tracing
let tracingExporter = OtlpGrpcTraceExporter(endpoint: "localhost:4317")
let tracerProvider = TracerProviderBuilder()
  .add(spanProcessor: SimpleSpanProcessor(spanExporter: tracingExporter))
  .build()
OpenTelemetry.registerTracerProvider(tracerProvider: tracerProvider)
```

### Custom Backend

```swift
import Metrics

// Implement MetricsFactory
class MyMetricsFactory: MetricsFactory {
  func makeCounter(label: String, dimensions: [(String, String)]) -> CounterHandler {
    MyCounterHandler(label: label, dimensions: dimensions)
  }
  // ... other methods
}

// Bootstrap your custom backend
MetricsSystem.bootstrap(MyMetricsFactory())
```

## Zero Configuration

- **No Telemetry by Default**: If you don't bootstrap a backend, there's zero overhead
- **Automatic When Enabled**: Bootstrap a backend and telemetry flows automatically
- **Backend Flexibility**: Switch backends without changing any Pulsar code

## FAQ

**Q: How do I disable telemetry?**
A: Don't bootstrap any metrics/tracing backend. Simple!

**Q: Can I use multiple backends?**
A: Yes! Use a multiplexing factory that forwards to multiple backends.

**Q: What about sampling?**
A: Configure sampling in your backend, not in PulsarClient.

**Q: Is telemetry global or per-client?**
A: Global - all PulsarClient instances in your process share the same metrics.

## Troubleshooting

### Metrics Not Appearing
- Verify you've called `MetricsSystem.bootstrap()`
- Check your backend is properly configured
- Ensure your metrics collector is running

### Missing Traces
- Verify you've called `InstrumentationSystem.bootstrap()`
- Check your tracing backend configuration
- Ensure sampling isn't excluding your traces

## Summary

The Pulsar Swift Client telemetry is:
- **Automatic** - No configuration needed
- **Standard** - Uses swift-metrics and swift-distributed-tracing
- **Flexible** - Works with any backend
- **Zero-overhead** - No cost when disabled
- **Global** - Serves all client instances

Just bootstrap your backend and go!