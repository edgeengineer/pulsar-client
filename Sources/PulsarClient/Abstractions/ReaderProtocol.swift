import Foundation

/// Reader interface for reading messages from Pulsar without subscription management
public protocol ReaderProtocol<T>: StateHolder, Sendable {
    associatedtype T: Sendable
    
    /// The topic this reader is reading from
    var topic: String { get }
    
    /// Read the next message
    /// - Returns: The next message
    func readNext() async throws -> Message<T>
    
    /// Read a batch of messages
    /// - Parameter maxMessages: Maximum number of messages to read
    /// - Returns: The read messages
    func readBatch(maxMessages: Int) async throws -> [Message<T>]
    
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