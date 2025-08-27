import Foundation
import NIOCore

/// Represents a pending send operation, similar to C# SendOp
final class SendOperation<T: Sendable>: Sendable {
    let metadata: Pulsar_Proto_MessageMetadata
    let payload: ByteBuffer
    let continuation: CheckedContinuation<MessageId, Error>
    let sequenceId: UInt64
    
    // Interceptor support
    let interceptorMessage: Message<T>?
    let interceptors: ProducerInterceptors<T>?
    let producer: (any ProducerProtocol<T>)?
    
    init(metadata: Pulsar_Proto_MessageMetadata, 
         payload: ByteBuffer, 
         continuation: CheckedContinuation<MessageId, Error>,
         interceptorMessage: Message<T>? = nil,
         interceptors: ProducerInterceptors<T>? = nil,
         producer: (any ProducerProtocol<T>)? = nil) {
        self.metadata = metadata
        self.payload = payload
        self.continuation = continuation
        self.sequenceId = metadata.sequenceID
        self.interceptorMessage = interceptorMessage
        self.interceptors = interceptors
        self.producer = producer
    }
    
    /// Complete the send operation with a message ID
    func complete(with messageId: MessageId) {
        // Notify interceptors if configured
        if let interceptors = interceptors,
           let message = interceptorMessage,
           let producer = producer {
            Task {
                await interceptors.onSendAcknowledgement(
                    producer: producer,
                    message: message,
                    messageId: messageId,
                    error: nil
                )
            }
        }
        
        continuation.resume(returning: messageId)
    }
    
    /// Fail the send operation with an error
    func fail(with error: Error) {
        // Notify interceptors if configured
        if let interceptors = interceptors,
           let message = interceptorMessage,
           let producer = producer {
            Task {
                await interceptors.onSendAcknowledgement(
                    producer: producer,
                    message: message,
                    messageId: nil,
                    error: error
                )
            }
        }
        
        continuation.resume(throwing: error)
    }
}