import Foundation

/// Producer interface for sending messages to Pulsar
public protocol ProducerProtocol<T>: StateHolder, Sendable, HandleException {
    associatedtype T: Sendable
    
    /// The topic this producer is publishing to
    var topic: String { get }
    
    /// Send a message
    /// - Parameter message: The message to send
    /// - Returns: The message ID assigned by the broker
    @discardableResult
    func send(_ message: T) async throws -> MessageId
    
    /// Send a message with metadata
    /// - Parameters:
    ///   - message: The message to send
    ///   - metadata: Message metadata
    /// - Returns: The message ID assigned by the broker
    @discardableResult
    func send(_ message: T, metadata: MessageMetadata) async throws -> MessageId
    
    /// Send a batch of messages
    /// - Parameter messages: The messages to send
    /// - Returns: The message IDs assigned by the broker
    func sendBatch(_ messages: [T]) async throws -> [MessageId]
    
    /// Flush any pending messages
    func flush() async throws
    
    /// Dispose of the producer
    func dispose() async
}