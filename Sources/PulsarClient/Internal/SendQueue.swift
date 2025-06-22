import Foundation

/// A queue for managing send operations
actor SendQueue<T: Sendable> {
    private var queue: [SendOperation<T>] = []
    private var waiters: [CheckedContinuation<SendOperation<T>, Error>] = []
    private let maxSize: Int
    
    init(maxSize: Int = 1000) {
        self.maxSize = maxSize
    }
    
    /// Enqueue a send operation
    func enqueue(_ operation: SendOperation<T>) async throws {
        guard queue.count < maxSize else {
            throw PulsarClientError.producerQueueFull
        }
        
        // If there are waiters, give it to them directly
        if !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.resume(returning: operation)
        } else {
            queue.append(operation)
        }
    }
    
    /// Dequeue the next operation (waits if queue is empty)
    func dequeue() async throws -> SendOperation<T> {
        if !queue.isEmpty {
            return queue.removeFirst()
        }
        
        // Wait for an operation
        return try await withCheckedThrowingContinuation { continuation in
            waiters.append(continuation)
        }
    }
    
    /// Peek at the first operation without removing it
    func peek() -> SendOperation<T>? {
        return queue.first
    }
    
    /// Remove the first operation
    func removeFirst() {
        if !queue.isEmpty {
            queue.removeFirst()
        }
    }
    
    /// Check if queue is empty
    var isEmpty: Bool {
        return queue.isEmpty
    }
    
    /// Cancel all pending operations
    func cancelAll() {
        // Cancel all queued operations
        for operation in queue {
            operation.fail(with: PulsarClientError.producerBusy("Producer closing"))
        }
        queue.removeAll()
        
        // Cancel all waiters
        for waiter in waiters {
            waiter.resume(throwing: PulsarClientError.producerBusy("Producer closing"))
        }
        waiters.removeAll()
    }
}