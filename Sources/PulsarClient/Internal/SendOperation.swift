import Foundation

/// Represents a pending send operation, similar to C# SendOp
final class SendOperation<T: Sendable>: Sendable {
  let metadata: Pulsar_Proto_MessageMetadata
  let payload: Data
  let continuation: CheckedContinuation<MessageId, Error>
  let sequenceId: UInt64

  init(
    metadata: Pulsar_Proto_MessageMetadata,
    payload: Data,
    continuation: CheckedContinuation<MessageId, Error>
  ) {
    self.metadata = metadata
    self.payload = payload
    self.continuation = continuation
    self.sequenceId = metadata.sequenceID
  }

  /// Complete the send operation with a message ID
  func complete(with messageId: MessageId) {
    continuation.resume(returning: messageId)
  }

  /// Fail the send operation with an error
  func fail(with error: Error) {
    continuation.resume(throwing: error)
  }
}
