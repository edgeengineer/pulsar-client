import Foundation

/// Interceptor for collecting producer metrics
public final class ProducerMetricsInterceptor<T>: ProducerInterceptor where T: Sendable {
  public typealias T = T

  private let metrics: any MetricsProvider
  private let dimensions: MetricDimensions

  // Metrics
  private let messagesSentCounter: any MetricCounter
  private let messagesFailedCounter: any MetricCounter
  private let messagesPendingGauge: any MetricGauge
  private let sendLatencyHistogram: any MetricHistogram
  private let messageSizeHistogram: any MetricHistogram

  // Track pending messages
  private let pendingMessages = PendingMessageTracker()

  public init(
    metrics: any MetricsProvider,
    topic: String,
    producerName: String,
    additionalDimensions: MetricDimensions = [:]
  ) {
    self.metrics = metrics

    // Build dimensions
    var dims = additionalDimensions
    dims["topic"] = topic
    dims["producer"] = producerName
    self.dimensions = dims

    // Initialize metrics
    self.messagesSentCounter = metrics.counter(
      name: PulsarMetrics.Producer.messagesSent,
      dimensions: dimensions
    )
    self.messagesFailedCounter = metrics.counter(
      name: PulsarMetrics.Producer.messagesFailed,
      dimensions: dimensions
    )
    self.messagesPendingGauge = metrics.gauge(
      name: PulsarMetrics.Producer.messagesPending,
      dimensions: dimensions
    )
    self.sendLatencyHistogram = metrics.histogram(
      name: PulsarMetrics.Producer.sendLatency,
      dimensions: dimensions
    )
    self.messageSizeHistogram = metrics.histogram(
      name: PulsarMetrics.Producer.batchSize,
      dimensions: dimensions
    )
  }

  public func beforeSend(
    producer: any ProducerProtocol<T>,
    message: Message<T>
  ) async throws -> Message<T> {
    // Track message as pending
    await pendingMessages.addPending(message: message)

    // Update pending gauge
    let pendingCount = await pendingMessages.count
    messagesPendingGauge.record(Double(pendingCount))

    // Record message size if available
    if let data = message.data {
      messageSizeHistogram.record(Double(data.count))
    }

    return message
  }

  public func onSendAcknowledgement(
    producer: any ProducerProtocol<T>,
    message: Message<T>,
    messageId: MessageId?,
    error: Error?
  ) async {
    // Calculate and record latency
    if let sendTime = await pendingMessages.removePending(message: message) {
      let latency = Date().timeIntervalSince(sendTime)
      sendLatencyHistogram.record(latency)
    }

    // Update counters
    if error == nil {
      messagesSentCounter.increment()
    } else {
      messagesFailedCounter.increment()
    }

    // Update pending gauge
    let pendingCount = await pendingMessages.count
    messagesPendingGauge.record(Double(pendingCount))
  }
}

/// Helper to track pending messages and their send times
private actor PendingMessageTracker {
  private var pendingMessages: [MessageId: Date] = [:]

  var count: Int {
    pendingMessages.count
  }

  func addPending<T>(message: Message<T>) {
    pendingMessages[message.id] = Date()
  }

  func removePending<T>(message: Message<T>) -> Date? {
    pendingMessages.removeValue(forKey: message.id)
  }
}
