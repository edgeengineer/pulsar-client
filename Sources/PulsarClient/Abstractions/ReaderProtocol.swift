import Foundation

/// Reader interface for reading messages from Pulsar without subscription management
///
/// Readers conform to AsyncSequence, allowing you to iterate over messages:
/// ```swift
/// for try await message in reader {
///     // Process message
/// }
/// ```
public protocol ReaderProtocol<MessageType>: StateHolder, AsyncSequence, Sendable
where MessageType: Sendable, T == ClientState, Element == Message<MessageType> {
  associatedtype MessageType: Sendable

  /// The topic this reader is reading from
  var topic: String { get }

  /// Check if there are more messages to read
  /// - Returns: True if there are more messages
  func hasMessageAvailable() async throws -> Bool

  /// Seek to a specific message ID
  /// - Parameter messageId: The message ID to seek to
  func seek(to messageId: MessageId) async throws

  /// Seek to a specific timestamp
  /// - Parameter timestamp: The timestamp to seek to
  func seek(to timestamp: Date) async throws

  /// Dispose of the reader
  func dispose() async
}
