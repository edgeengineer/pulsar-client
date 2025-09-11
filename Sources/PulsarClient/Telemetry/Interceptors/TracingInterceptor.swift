import Foundation

/// Producer interceptor for distributed tracing
public final class ProducerTracingInterceptor<T>: ProducerInterceptor where T: Sendable {
  public typealias T = T

  private let tracing: any TracingProvider
  private let topic: String
  private let producerName: String

  // Track spans for messages
  private let spanTracker = MessageSpanTracker()

  public init(
    tracing: any TracingProvider,
    topic: String,
    producerName: String
  ) {
    self.tracing = tracing
    self.topic = topic
    self.producerName = producerName
  }

  public func beforeSend(
    producer: any ProducerProtocol<T>,
    message: Message<T>
  ) async throws -> Message<T> {
    // Start a new span for sending
    let span = tracing.startSpan(
      name: PulsarSpans.Names.send,
      kind: .producer
    )

    // Set span attributes
    span.setAttribute(key: PulsarSpans.Attributes.topic, value: topic)
    span.setAttribute(key: PulsarSpans.Attributes.producerName, value: producerName)

    if let data = message.data {
      span.setAttribute(key: PulsarSpans.Attributes.messageSize, value: data.count)
    }

    // For now, we can't modify message properties as it's a struct
    // In a real implementation, we'd need a mutable message builder
    // TODO: Add trace context injection when message building is supported

    // Track the span by message ID
    await spanTracker.trackSpanById(messageId: message.id, span: span)

    return message
  }

  public func onSendAcknowledgement(
    producer: any ProducerProtocol<T>,
    message: Message<T>,
    messageId: MessageId?,
    error: Error?
  ) async {
    // Complete the span
    if let span = await spanTracker.removeSpanById(messageId: message.id) {
      if let messageId = messageId {
        span.setAttribute(key: PulsarSpans.Attributes.messageId, value: messageId.description)
        span.setStatus(.ok)
      } else if let error = error {
        span.recordException(error)
      }
      span.end()
    }
  }
}

/// Consumer interceptor for distributed tracing
public final class ConsumerTracingInterceptor<T>: ConsumerInterceptor where T: Sendable {
  public typealias T = T

  private let tracing: any TracingProvider
  private let topic: String
  private let subscription: String
  private let consumerName: String

  // Track spans for messages
  private let spanTracker = MessageSpanTracker()

  public init(
    tracing: any TracingProvider,
    topic: String,
    subscription: String,
    consumerName: String
  ) {
    self.tracing = tracing
    self.topic = topic
    self.subscription = subscription
    self.consumerName = consumerName
  }

  public func beforeConsume(
    consumer: any ConsumerProtocol<T>,
    message: Message<T>
  ) async throws -> Message<T> {
    // Extract parent context from message properties
    let parentContext = tracing.extractContext(from: message.properties)

    // Start a new span for receiving
    let span: any Span
    if let parentContext {
      // Create span with extracted parent context
      span = tracing.startSpan(
        name: PulsarSpans.Names.receive,
        parent: nil,  // Would need to create span from context
        options: SpanOptions(kind: .consumer)
      )
    } else {
      span = tracing.startSpan(
        name: PulsarSpans.Names.receive,
        kind: .consumer
      )
    }

    // Set span attributes
    span.setAttribute(key: PulsarSpans.Attributes.topic, value: topic)
    span.setAttribute(key: PulsarSpans.Attributes.subscription, value: subscription)
    span.setAttribute(key: PulsarSpans.Attributes.consumerName, value: consumerName)
    span.setAttribute(key: PulsarSpans.Attributes.messageId, value: message.id.description)

    if let data = message.data {
      span.setAttribute(key: PulsarSpans.Attributes.messageSize, value: data.count)
    }

    // Track the span for acknowledgment
    await spanTracker.trackSpanById(messageId: message.id, span: span)

    return message
  }

  public func onAcknowledge(
    consumer: any ConsumerProtocol<T>,
    messageId: MessageId,
    error: Error?
  ) async {
    // Complete the span
    if let span = await spanTracker.removeSpanById(messageId: messageId) {
      if error == nil {
        span.setStatus(.ok)
      } else if let error = error {
        span.recordException(error)
      }
      span.end()
    }
  }

  public func onNegativeAcksSend(
    consumer: any ConsumerProtocol<T>,
    messageIds: Set<MessageId>
  ) async {
    // Complete spans with error status
    for messageId in messageIds {
      if let span = await spanTracker.removeSpanById(messageId: messageId) {
        span.setStatus(.error("Message negatively acknowledged"))
        span.end()
      }
    }
  }

  public func onAckTimeoutSend(
    consumer: any ConsumerProtocol<T>,
    messageIds: Set<MessageId>
  ) async {
    // Complete spans with timeout status
    for messageId in messageIds {
      if let span = await spanTracker.removeSpanById(messageId: messageId) {
        span.setStatus(.error("Acknowledgment timeout"))
        span.end()
      }
    }
  }
}

/// Helper to track spans for messages
private actor MessageSpanTracker {
  private var messageIdSpans: [MessageId: any Span] = [:]

  func trackSpanById(messageId: MessageId, span: any Span) {
    messageIdSpans[messageId] = span
  }

  func removeSpanById(messageId: MessageId) -> (any Span)? {
    messageIdSpans.removeValue(forKey: messageId)
  }
}
