import Foundation
import Logging

/// Manages a chain of producer interceptors
actor ProducerInterceptors<T: Sendable> {
    private let logger = Logger(label: "ProducerInterceptors")
    private let interceptors: [any ProducerInterceptor<T>]
    
    init(interceptors: [any ProducerInterceptor<T>]) {
        self.interceptors = interceptors
    }
    
    /// Process message through all interceptors before sending
    func beforeSend(
        producer: any ProducerProtocol<T>,
        message: Message<T>
    ) async throws -> Message<T> {
        var processedMessage = message
        
        for interceptor in interceptors {
            do {
                let eligible = await interceptor.eligible(message: processedMessage)
                if eligible {
                    processedMessage = try await interceptor.beforeSend(
                        producer: producer,
                        message: processedMessage
                    )
                }
            } catch {
                logger.error("Interceptor beforeSend failed", metadata: [
                    "interceptor": "\(type(of: interceptor))",
                    "error": "\(error)"
                ])
                // Continue with other interceptors
            }
        }
        
        return processedMessage
    }
    
    /// Notify all interceptors of send acknowledgment
    func onSendAcknowledgement(
        producer: any ProducerProtocol<T>,
        message: Message<T>,
        messageId: MessageId?,
        error: Error?
    ) async {
        for interceptor in interceptors {
            await interceptor.onSendAcknowledgement(
                producer: producer,
                message: message,
                messageId: messageId,
                error: error
            )
        }
    }
    
    /// Notify all interceptors of partition changes
    func onPartitionsChange(topicName: String, partitions: Int) async {
        for interceptor in interceptors {
            await interceptor.onPartitionsChange(
                topicName: topicName,
                partitions: partitions
            )
        }
    }
    
    /// Close all interceptors
    func close() async {
        for interceptor in interceptors {
            await interceptor.close()
        }
    }
}

/// Manages a chain of consumer interceptors
actor ConsumerInterceptors<T: Sendable> {
    private let logger = Logger(label: "ConsumerInterceptors")
    private let interceptors: [any ConsumerInterceptor<T>]
    
    init(interceptors: [any ConsumerInterceptor<T>]) {
        self.interceptors = interceptors
    }
    
    /// Process message through all interceptors before consuming
    func beforeConsume(
        consumer: any ConsumerProtocol<T>,
        message: Message<T>
    ) async throws -> Message<T> {
        var processedMessage = message
        
        for interceptor in interceptors {
            do {
                processedMessage = try await interceptor.beforeConsume(
                    consumer: consumer,
                    message: processedMessage
                )
            } catch {
                logger.error("Interceptor beforeConsume failed", metadata: [
                    "interceptor": "\(type(of: interceptor))",
                    "error": "\(error)"
                ])
                // Continue with other interceptors
            }
        }
        
        return processedMessage
    }
    
    /// Notify all interceptors of acknowledgment
    func onAcknowledge(
        consumer: any ConsumerProtocol<T>,
        messageId: MessageId,
        error: Error?
    ) async {
        for interceptor in interceptors {
            await interceptor.onAcknowledge(
                consumer: consumer,
                messageId: messageId,
                error: error
            )
        }
    }
    
    /// Notify all interceptors of cumulative acknowledgment
    func onAcknowledgeCumulative(
        consumer: any ConsumerProtocol<T>,
        messageId: MessageId,
        error: Error?
    ) async {
        for interceptor in interceptors {
            await interceptor.onAcknowledgeCumulative(
                consumer: consumer,
                messageId: messageId,
                error: error
            )
        }
    }
    
    /// Notify all interceptors of negative acknowledgments
    func onNegativeAcksSend(
        consumer: any ConsumerProtocol<T>,
        messageIds: Set<MessageId>
    ) async {
        for interceptor in interceptors {
            await interceptor.onNegativeAcksSend(
                consumer: consumer,
                messageIds: messageIds
            )
        }
    }
    
    /// Notify all interceptors of acknowledgment timeouts
    func onAckTimeoutSend(
        consumer: any ConsumerProtocol<T>,
        messageIds: Set<MessageId>
    ) async {
        for interceptor in interceptors {
            await interceptor.onAckTimeoutSend(
                consumer: consumer,
                messageIds: messageIds
            )
        }
    }
    
    /// Notify all interceptors of partition changes
    func onPartitionsChange(topicName: String, partitions: Int) async {
        for interceptor in interceptors {
            await interceptor.onPartitionsChange(
                topicName: topicName,
                partitions: partitions
            )
        }
    }
    
    /// Close all interceptors
    func close() async {
        for interceptor in interceptors {
            await interceptor.close()
        }
    }
}