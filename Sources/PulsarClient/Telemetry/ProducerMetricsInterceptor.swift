import Foundation
import Metrics
import Tracing

internal final class ProducerMetricsInterceptor<T>: ProducerInterceptor where T: Sendable {
  public typealias T = T

  private let topic: String
  private let producerName: String

  // Metrics
  private let messagesSentCounter: Counter
  private let messagesFailedCounter: Counter
  private let messagesPendingGauge: Gauge
  private let sendLatencyTimer: Timer
  private let batchSizeRecorder: Recorder

  // Track pending messages for latency calculation
  private let pendingMessages = PendingMessageTracker()

  internal init(topic: String, producerName: String) {
    self.topic = topic
    self.producerName = producerName

    // Initialize metrics using global Pulsar metrics
    self.messagesSentCounter = PulsarMetrics.producerMessagesSent(topic: topic, producer: producerName)
    self.messagesFailedCounter = PulsarMetrics.producerMessagesFailed(topic: topic, producer: producerName)
    self.messagesPendingGauge = PulsarMetrics.producerMessagesPending(topic: topic, producer: producerName)
    self.sendLatencyTimer = PulsarMetrics.producerSendLatency(topic: topic, producer: producerName)
    self.batchSizeRecorder = PulsarMetrics.producerBatchSize(topic: topic, producer: producerName)
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

    // Record message size
    if let data = message.data {
      batchSizeRecorder.record(Double(data.count))
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
      sendLatencyTimer.recordSeconds(latency)
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