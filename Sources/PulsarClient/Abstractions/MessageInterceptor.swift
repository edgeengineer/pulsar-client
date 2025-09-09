import Foundation

/// Protocol for producer message interceptors
public protocol ProducerInterceptor<T>: Sendable where T: Sendable {
    associatedtype T
    
    /// Called before sending a message
    /// - Parameters:
    ///   - producer: The producer sending the message
    ///   - message: The message to be sent
    /// - Returns: The message to send (can be modified)
    func beforeSend(
        producer: any ProducerProtocol<T>,
        message: Message<T>
    ) async throws -> Message<T>
    
    /// Called after a message is acknowledged or fails
    /// - Parameters:
    ///   - producer: The producer that sent the message
    ///   - message: The original message
    ///   - messageId: The message ID if successful, nil if failed
    ///   - error: The error if failed, nil if successful
    func onSendAcknowledgement(
        producer: any ProducerProtocol<T>,
        message: Message<T>,
        messageId: MessageId?,
        error: Error?
    ) async
    
    /// Check if this interceptor should process the message
    /// - Parameter message: The message to check
    /// - Returns: true if the interceptor should process this message
    func eligible(message: Message<T>) async -> Bool
    
    /// Called when partitions change for a topic
    /// - Parameters:
    ///   - topicName: The topic name
    ///   - partitions: The new number of partitions
    func onPartitionsChange(topicName: String, partitions: Int) async
    
    /// Close the interceptor and release resources
    func close() async
}

/// Default implementations for ProducerInterceptor
public extension ProducerInterceptor {
    func eligible(message: Message<T>) async -> Bool {
        return true
    }
    
    func onPartitionsChange(topicName: String, partitions: Int) async {
        // Default: no-op
    }
    
    func close() async {
        // Default: no-op
    }
}

/// Protocol for consumer message interceptors
public protocol ConsumerInterceptor<T>: Sendable where T: Sendable {
    associatedtype T
    
    /// Called before a message is delivered to the application
    /// - Parameters:
    ///   - consumer: The consumer receiving the message
    ///   - message: The message to be consumed
    /// - Returns: The message to deliver (can be modified)
    func beforeConsume(
        consumer: any ConsumerProtocol<T>,
        message: Message<T>
    ) async throws -> Message<T>
    
    /// Called when a message is acknowledged
    /// - Parameters:
    ///   - consumer: The consumer acknowledging the message
    ///   - messageId: The message ID being acknowledged
    ///   - error: Error if acknowledgment failed
    func onAcknowledge(
        consumer: any ConsumerProtocol<T>,
        messageId: MessageId,
        error: Error?
    ) async
    
    /// Called when messages are acknowledged cumulatively
    /// - Parameters:
    ///   - consumer: The consumer acknowledging messages
    ///   - messageId: The message ID up to which messages are acknowledged
    ///   - error: Error if acknowledgment failed
    func onAcknowledgeCumulative(
        consumer: any ConsumerProtocol<T>,
        messageId: MessageId,
        error: Error?
    ) async
    
    /// Called when messages are negatively acknowledged
    /// - Parameters:
    ///   - consumer: The consumer negatively acknowledging messages
    ///   - messageIds: The message IDs being negatively acknowledged
    func onNegativeAcksSend(
        consumer: any ConsumerProtocol<T>,
        messageIds: Set<MessageId>
    ) async
    
    /// Called when messages timeout and are redelivered
    /// - Parameters:
    ///   - consumer: The consumer with timed out messages
    ///   - messageIds: The message IDs that timed out
    func onAckTimeoutSend(
        consumer: any ConsumerProtocol<T>,
        messageIds: Set<MessageId>
    ) async
    
    /// Called when partitions change for a topic
    /// - Parameters:
    ///   - topicName: The topic name
    ///   - partitions: The new number of partitions
    func onPartitionsChange(topicName: String, partitions: Int) async
    
    /// Close the interceptor and release resources
    func close() async
}

/// Default implementations for ConsumerInterceptor
public extension ConsumerInterceptor {
    func onAcknowledge(
        consumer: any ConsumerProtocol<T>,
        messageId: MessageId,
        error: Error?
    ) async {
        // Default: no-op
    }
    
    func onAcknowledgeCumulative(
        consumer: any ConsumerProtocol<T>,
        messageId: MessageId,
        error: Error?
    ) async {
        // Default: no-op
    }
    
    func onNegativeAcksSend(
        consumer: any ConsumerProtocol<T>,
        messageIds: Set<MessageId>
    ) async {
        // Default: no-op
    }
    
    func onAckTimeoutSend(
        consumer: any ConsumerProtocol<T>,
        messageIds: Set<MessageId>
    ) async {
        // Default: no-op
    }
    
    func onPartitionsChange(topicName: String, partitions: Int) async {
        // Default: no-op
    }
    
    func close() async {
        // Default: no-op
    }
}