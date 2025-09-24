import Foundation
import Metrics
import Tracing

internal final class ConsumerMetricsInterceptor<T>: ConsumerInterceptor where T: Sendable {
  public typealias T = T

  private let topic: String
  private let subscription: String
  private let consumerName: String

  // Metrics
  private let messagesReceivedCounter: Counter
  private let messagesAcknowledgedCounter: Counter
  private let messagesNackedCounter: Counter
  private let receiveLatencyTimer: Timer
  private let processLatencyTimer: Timer

  // Track message receive times for process latency
  private let receiveTimeTracker = MessageReceiveTimeTracker()

  internal init(topic: String, subscription: String, consumerName: String) {
    self.topic = topic
    self.subscription = subscription
    self.consumerName = consumerName

    // Initialize metrics using global Pulsar metrics
    self.messagesReceivedCounter = PulsarMetrics.consumerMessagesReceived(
      topic: topic,
      subscription: subscription,
      consumer: consumerName
    )
    self.messagesAcknowledgedCounter = PulsarMetrics.consumerMessagesAcknowledged(
      topic: topic,
      subscription: subscription,
      consumer: consumerName
    )
    self.messagesNackedCounter = PulsarMetrics.consumerMessagesNacked(
      topic: topic,
      subscription: subscription,
      consumer: consumerName
    )
    self.receiveLatencyTimer = PulsarMetrics.consumerReceiveLatency(
      topic: topic,
      subscription: subscription,
      consumer: consumerName
    )
    self.processLatencyTimer = PulsarMetrics.consumerProcessLatency(
      topic: topic,
      subscription: subscription,
      consumer: consumerName
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

    // Calculate receive latency
    let receiveLatency = Date().timeIntervalSince(message.publishTime)
    receiveLatencyTimer.recordSeconds(receiveLatency)

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
        processLatencyTimer.recordSeconds(processLatency)
      }
    }
  }

  public func onAcknowledgeCumulative(
    consumer: any ConsumerProtocol<T>,
    messageId: MessageId,
    error: Error?
  ) async {
    if error == nil {
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
}