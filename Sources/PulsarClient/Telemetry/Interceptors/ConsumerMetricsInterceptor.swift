import Foundation

/// Interceptor for collecting consumer metrics
public final class ConsumerMetricsInterceptor<T>: ConsumerInterceptor where T: Sendable {
  public typealias T = T

  private let metrics: any MetricsProvider
  private let dimensions: MetricDimensions

  // Metrics
  private let messagesReceivedCounter: any MetricCounter
  private let messagesAcknowledgedCounter: any MetricCounter
  private let messagesNackedCounter: any MetricCounter
  private let messagesDlqCounter: any MetricCounter
  private let receiveLatencyHistogram: any MetricHistogram
  private let processLatencyHistogram: any MetricHistogram
  private let backlogGauge: any MetricGauge

  // Track message receive times for process latency
  private let receiveTimeTracker = MessageReceiveTimeTracker()

  public init(
    metrics: any MetricsProvider,
    topic: String,
    subscription: String,
    consumerName: String,
    additionalDimensions: MetricDimensions = [:]
  ) {
    self.metrics = metrics

    // Build dimensions
    var dims = additionalDimensions
    dims["topic"] = topic
    dims["subscription"] = subscription
    dims["consumer"] = consumerName
    self.dimensions = dims

    // Initialize metrics
    self.messagesReceivedCounter = metrics.counter(
      name: PulsarMetrics.Consumer.messagesReceived,
      dimensions: dimensions
    )
    self.messagesAcknowledgedCounter = metrics.counter(
      name: PulsarMetrics.Consumer.messagesAcknowledged,
      dimensions: dimensions
    )
    self.messagesNackedCounter = metrics.counter(
      name: PulsarMetrics.Consumer.messagesNacked,
      dimensions: dimensions
    )
    self.messagesDlqCounter = metrics.counter(
      name: PulsarMetrics.Consumer.messagesDlq,
      dimensions: dimensions
    )
    self.receiveLatencyHistogram = metrics.histogram(
      name: PulsarMetrics.Consumer.receiveLatency,
      dimensions: dimensions
    )
    self.processLatencyHistogram = metrics.histogram(
      name: PulsarMetrics.Consumer.processLatency,
      dimensions: dimensions
    )
    self.backlogGauge = metrics.gauge(
      name: PulsarMetrics.Consumer.backlog,
      dimensions: dimensions
    )
  }

  public func beforeConsume(
    consumer: any ConsumerProtocol<T>,
    message: Message<T>
  ) async throws -> Message<T> {
    // Increment received counter
    messagesReceivedCounter.increment()

    // Track receive time for process latency calculation
    await receiveTimeTracker.trackReceiveTime(messageId: message.id)

    // Calculate receive latency (publishTime is non-optional)
    let receiveLatency = Date().timeIntervalSince(message.publishTime)
    receiveLatencyHistogram.record(receiveLatency)

    return message
  }

  public func onAcknowledge(
    consumer: any ConsumerProtocol<T>,
    messageId: MessageId,
    error: Error?
  ) async {
    if error == nil {
      messagesAcknowledgedCounter.increment()

      // Calculate and record process latency
      if let receiveTime = await receiveTimeTracker.removeReceiveTime(messageId: messageId) {
        let processLatency = Date().timeIntervalSince(receiveTime)
        processLatencyHistogram.record(processLatency)
      }
    }
  }

  public func onAcknowledgeCumulative(
    consumer: any ConsumerProtocol<T>,
    messageId: MessageId,
    error: Error?
  ) async {
    if error == nil {
      // For cumulative ack, we don't have exact count but increment by 1
      // In a real implementation, we might track message ranges
      messagesAcknowledgedCounter.increment()
    }
  }

  public func onNegativeAcksSend(
    consumer: any ConsumerProtocol<T>,
    messageIds: Set<MessageId>
  ) async {
    messagesNackedCounter.increment(by: Int64(messageIds.count))

    // Clean up tracked receive times
    for messageId in messageIds {
      await receiveTimeTracker.removeReceiveTime(messageId: messageId)
    }
  }

  public func onAckTimeoutSend(
    consumer: any ConsumerProtocol<T>,
    messageIds: Set<MessageId>
  ) async {
    // Track timeout as negative acknowledgments
    messagesNackedCounter.increment(by: Int64(messageIds.count))

    // Clean up tracked receive times
    for messageId in messageIds {
      await receiveTimeTracker.removeReceiveTime(messageId: messageId)
    }
  }

  /// Update backlog metric (should be called periodically)
  public func updateBacklog(_ backlogSize: Int) async {
    backlogGauge.record(Double(backlogSize))
  }
}

/// Helper to track message receive times
private actor MessageReceiveTimeTracker {
  private var receiveTimes: [MessageId: Date] = [:]

  func trackReceiveTime(messageId: MessageId) {
    receiveTimes[messageId] = Date()
  }

  @discardableResult
  func removeReceiveTime(messageId: MessageId) -> Date? {
    receiveTimes.removeValue(forKey: messageId)
  }

  func cleanup(olderThan: Date) {
    receiveTimes = receiveTimes.filter { $0.value > olderThan }
  }
}
