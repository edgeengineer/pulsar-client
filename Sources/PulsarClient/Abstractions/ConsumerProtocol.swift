import Foundation

/// Consumer interface for receiving messages from Pulsar
/// 
/// Consumers conform to AsyncSequence, allowing you to iterate over messages:
/// ```swift
/// for try await message in consumer {
///     // Process message
///     try await consumer.acknowledge(message)
/// }
/// ```
public protocol ConsumerProtocol<MessageType>: StateHolder, AsyncSequence, Sendable
where MessageType: Sendable, T == ClientState, Element == Message<MessageType> {
  associatedtype MessageType: Sendable

  /// The topic(s) this consumer is subscribed to
  var topics: [String] { get }

  /// The subscription name
  var subscription: String { get }

  /// Acknowledge a message
  /// - Parameter message: The message to acknowledge
  func acknowledge(_ message: Message<MessageType>) async throws

  /// Acknowledge messages cumulatively up to and including the given message
  /// - Parameter message: The message to acknowledge cumulatively
  func acknowledgeCumulative(_ message: Message<MessageType>) async throws

  /// Acknowledge multiple messages
  /// - Parameter messages: The messages to acknowledge
  func acknowledgeBatch(_ messages: [Message<MessageType>]) async throws

  /// Negative acknowledge a message for redelivery
  /// - Parameter message: The message to negative acknowledge
  func negativeAcknowledge(_ message: Message<MessageType>) async throws

  /// Seek to a specific message ID
  /// - Parameter messageId: The message ID to seek to
  func seek(to messageId: MessageId) async throws

  /// Seek to a specific timestamp
  /// - Parameter timestamp: The timestamp to seek to
  func seek(to timestamp: Date) async throws

  /// Unsubscribe from the topic
  func unsubscribe() async throws

  /// Dispose of the consumer
  func dispose() async

  /// Check if there are messages available in the local buffer
  /// - Returns: Number of buffered messages
  func getBufferedMessageCount() async -> Int

  /// Get the last message ID available on the broker
  /// - Returns: The last message ID response
  func getLastMessageId() async throws -> GetLastMessageIdResponse

  /// Get the current read position (last received message ID)
  /// - Returns: The current read position, or nil if no messages have been read
  func getCurrentPosition() async -> MessageId?
}

/// Consumer subscription type
public enum SubscriptionType: String, Sendable {
  case exclusive
  case shared
  case keyShared = "key_shared"
  case failover
}

/// Consumer subscription initial position
public enum SubscriptionInitialPosition: Sendable {
  case latest
  case earliest
  case messageId(MessageId)
  case timestamp(Date)
}
