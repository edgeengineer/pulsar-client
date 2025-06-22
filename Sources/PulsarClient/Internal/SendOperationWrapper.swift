import Foundation

/// Type-erased wrapper for SendOperation to allow storage in ProducerChannel
protocol SendOperationWrapper: Sendable {
    var sequenceId: UInt64 { get }
    func complete(with messageId: MessageId)
    func fail(with error: Error)
}

/// Concrete wrapper implementation
struct AnySendOperationWrapper<T: Sendable>: SendOperationWrapper {
    private let operation: SendOperation<T>
    
    var sequenceId: UInt64 {
        operation.sequenceId
    }
    
    init(_ operation: SendOperation<T>) {
        self.operation = operation
    }
    
    func complete(with messageId: MessageId) {
        operation.complete(with: messageId)
    }
    
    func fail(with error: Error) {
        operation.fail(with: error)
    }
}