import Foundation

/// The main client interface for Apache Pulsar
public protocol PulsarClientProtocol: Sendable {
    /// Create a new producer
    /// - Parameters:
    ///   - topic: The topic to produce messages to
    ///   - schema: The schema for message serialization
    ///   - configure: Configuration closure for the producer
    /// - Returns: A configured producer instance
    func newProducer<T>(
        topic: String,
        schema: Schema<T>,
        configure: (ProducerBuilder<T>) -> Void
    ) async throws -> any ProducerProtocol<T> where T: Sendable
    
    /// Create a new consumer
    /// - Parameters:
    ///   - topic: The topic(s) to consume messages from
    ///   - schema: The schema for message deserialization
    ///   - configure: Configuration closure for the consumer
    /// - Returns: A configured consumer instance
    func newConsumer<T>(
        topic: String,
        schema: Schema<T>,
        configure: (ConsumerBuilder<T>) -> Void
    ) async throws -> any ConsumerProtocol<T> where T: Sendable
    
    /// Create a new reader
    /// - Parameters:
    ///   - topic: The topic to read messages from
    ///   - schema: The schema for message deserialization
    ///   - configure: Configuration closure for the reader
    /// - Returns: A configured reader instance
    func newReader<T>(
        topic: String,
        schema: Schema<T>,
        configure: (ReaderBuilder<T>) -> Void
    ) async throws -> any ReaderProtocol<T> where T: Sendable
    
    /// Dispose of the client and all its resources
    func dispose() async
}

/// Client state
public enum ClientState: Sendable, Equatable {
    case disconnected
    case initializing
    case connecting
    case connected
    case reconnecting
    case closing
    case closed
    case faulted(Error)
    
    public static func ==(lhs: ClientState, rhs: ClientState) -> Bool {
        switch (lhs, rhs) {
        case (.disconnected, .disconnected),
             (.initializing, .initializing),
             (.connecting, .connecting),
             (.connected, .connected),
             (.reconnecting, .reconnecting),
             (.closing, .closing),
             (.closed, .closed):
            return true
        case (.faulted(_), .faulted(_)):
            return true
        default:
            return false
        }
    }
}