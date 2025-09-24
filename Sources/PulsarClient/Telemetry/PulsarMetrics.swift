import Foundation
import Metrics
import Tracing

/// Global metrics for Pulsar client
/// The Pulsar client will automatically emit metrics if a backend is configured
internal enum PulsarMetrics {
  // MARK: - Client Metrics

  static let clientConnectionsActive = Gauge(
    label: "pulsar.client.connections.active",
    dimensions: [("type", "active")]
  )

  static let clientConnectionsTotal = Counter(
    label: "pulsar.client.connections.total"
  )

  static let clientConnectionsFailed = Counter(
    label: "pulsar.client.connections.failed"
  )

  static let clientLookupRequests = Counter(
    label: "pulsar.client.lookup.requests"
  )

  static let clientLookupLatency = Timer(
    label: "pulsar.client.lookup.latency"
  )

  // MARK: - Producer Metrics

  static func producerMessagesSent(topic: String, producer: String) -> Counter {
    Counter(
      label: "pulsar.producer.messages.sent",
      dimensions: [("topic", topic), ("producer", producer)]
    )
  }

  static func producerMessagesFailed(topic: String, producer: String) -> Counter {
    Counter(
      label: "pulsar.producer.messages.failed",
      dimensions: [("topic", topic), ("producer", producer)]
    )
  }

  static func producerMessagesPending(topic: String, producer: String) -> Gauge {
    Gauge(
      label: "pulsar.producer.messages.pending",
      dimensions: [("topic", topic), ("producer", producer)]
    )
  }

  static func producerSendLatency(topic: String, producer: String) -> Timer {
    Timer(
      label: "pulsar.producer.send.latency",
      dimensions: [("topic", topic), ("producer", producer)]
    )
  }

  static func producerBatchSize(topic: String, producer: String) -> Recorder {
    Recorder(
      label: "pulsar.producer.batch.size",
      dimensions: [("topic", topic), ("producer", producer)]
    )
  }

  // MARK: - Consumer Metrics

  static func consumerMessagesReceived(topic: String, subscription: String, consumer: String) -> Counter {
    Counter(
      label: "pulsar.consumer.messages.received",
      dimensions: [("topic", topic), ("subscription", subscription), ("consumer", consumer)]
    )
  }

  static func consumerMessagesAcknowledged(topic: String, subscription: String, consumer: String) -> Counter {
    Counter(
      label: "pulsar.consumer.messages.acknowledged",
      dimensions: [("topic", topic), ("subscription", subscription), ("consumer", consumer)]
    )
  }

  static func consumerMessagesNacked(topic: String, subscription: String, consumer: String) -> Counter {
    Counter(
      label: "pulsar.consumer.messages.nacked",
      dimensions: [("topic", topic), ("subscription", subscription), ("consumer", consumer)]
    )
  }

  static func consumerReceiveLatency(topic: String, subscription: String, consumer: String) -> Timer {
    Timer(
      label: "pulsar.consumer.receive.latency",
      dimensions: [("topic", topic), ("subscription", subscription), ("consumer", consumer)]
    )
  }

  static func consumerProcessLatency(topic: String, subscription: String, consumer: String) -> Timer {
    Timer(
      label: "pulsar.consumer.process.latency",
      dimensions: [("topic", topic), ("subscription", subscription), ("consumer", consumer)]
    )
  }

  static func consumerBacklog(topic: String, subscription: String, consumer: String) -> Gauge {
    Gauge(
      label: "pulsar.consumer.backlog",
      dimensions: [("topic", topic), ("subscription", subscription), ("consumer", consumer)]
    )
  }

  // MARK: - Connection Pool Metrics

  static let poolConnectionsActive = Gauge(
    label: "pulsar.pool.connections.active"
  )

  static let poolConnectionsIdle = Gauge(
    label: "pulsar.pool.connections.idle"
  )

  static let poolWaitTime = Timer(
    label: "pulsar.pool.wait.time"
  )
}

// MARK: - Tracing Support

internal enum PulsarTracing {
  /// Start a span for a send operation
  static func startSendSpan(topic: String, producer: String) -> any Span {
    let span = InstrumentationSystem.tracer.startSpan(
      "pulsar.send",
      context: ServiceContext.current ?? .topLevel,
      ofKind: .producer
    )
    span.attributes["pulsar.topic"] = topic
    span.attributes["pulsar.producer"] = producer
    return span
  }

  /// Start a span for a receive operation
  static func startReceiveSpan(topic: String, consumer: String, parentContext: ServiceContext? = nil) -> any Span {
    let span = InstrumentationSystem.tracer.startSpan(
      "pulsar.receive",
      context: parentContext ?? ServiceContext.current ?? .topLevel,
      ofKind: .consumer
    )
    span.attributes["pulsar.topic"] = topic
    span.attributes["pulsar.consumer"] = consumer
    return span
  }

  /// Start a span for an acknowledge operation
  static func startAcknowledgeSpan(messageId: MessageId) -> any Span {
    let span = InstrumentationSystem.tracer.startSpan(
      "pulsar.acknowledge",
      context: ServiceContext.current ?? .topLevel,
      ofKind: .client
    )
    span.attributes["pulsar.message.id"] = messageId.description
    return span
  }

  /// Start a span for a connection operation
  static func startConnectSpan(url: String) -> any Span {
    let span = InstrumentationSystem.tracer.startSpan(
      "pulsar.connect",
      context: ServiceContext.current ?? .topLevel,
      ofKind: .client
    )
    span.attributes["pulsar.broker.url"] = url
    return span
  }

  /// Start a span for a lookup operation
  static func startLookupSpan(topic: String) -> any Span {
    let span = InstrumentationSystem.tracer.startSpan(
      "pulsar.lookup",
      context: ServiceContext.current ?? .topLevel,
      ofKind: .client
    )
    span.attributes["pulsar.topic"] = topic
    return span
  }
}