/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

// MARK: - Consumer Convenience Extensions

public extension ConsumerProtocol {
    
    /// Acknowledge a message directly (without needing to extract MessageId)
    func acknowledge(_ message: Message<MessageType>) async throws {
        try await acknowledge(message)
    }
    
    /// Process a message and automatically acknowledge it
    func process(_ handler: (Message<MessageType>) async throws -> Void) async throws {
        let message = try await receive()
        do {
            try await handler(message)
            try await acknowledge(message)
        } catch {
            try await negativeAcknowledge(message)
            throw error
        }
    }
    
    /// Process messages continuously
    func processMessages(_ handler: (Message<MessageType>) async throws -> Void) async throws {
        while true {
            try await process(handler)
        }
    }
    
    /// Try to receive a message without blocking
    func tryReceive() async -> Message<MessageType>? {
        do {
            return try await receive()
        } catch {
            return nil
        }
    }
}

// MARK: - Batch Processing Extensions

public extension ConsumerProtocol {
    
    /// Process a batch of messages with automatic acknowledgment
    func processBatch(
        maxMessages: Int,
        handler: ([Message<MessageType>]) async throws -> Void
    ) async throws {
        let messages = try await receiveBatch(maxMessages: maxMessages)
        
        do {
            try await handler(messages)
            try await acknowledgeBatch(messages)
        } catch {
            // Negative acknowledge all messages in the batch
            for message in messages {
                try await negativeAcknowledge(message)
            }
            throw error
        }
    }
    
    /// Receive messages with a timeout
    func receive(timeout: TimeInterval) async throws -> Message<MessageType> {
        try await withThrowingTaskGroup(of: Message<MessageType>.self) { group in
            // Add receive task
            group.addTask {
                try await self.receive()
            }
            
            // Add timeout task
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                throw PulsarClientError.timeout("Receive operation timed out")
            }
            
            // Return first result (either message or timeout)
            let result = try await group.next()!
            group.cancelAll()
            return result
        }
    }
}

// MARK: - State Monitoring Extensions

public extension ConsumerProtocol where Self: StateHolder, Self.T == ClientState {
    
    /// Wait for the consumer to reach a specific state
    @discardableResult
    func waitForState(_ targetState: ClientState, timeout: TimeInterval = 30.0) async throws -> ClientState {
        if state == targetState {
            return state
        }
        
        return try await stateChangedTo(targetState, timeout: timeout)
    }
    
    /// Wait for the consumer to leave a specific state
    @discardableResult
    func waitToLeaveState(_ currentState: ClientState, timeout: TimeInterval = 30.0) async throws -> ClientState {
        if state != currentState {
            return state
        }
        
        return try await stateChangedFrom(currentState, timeout: timeout)
    }
}

// MARK: - AsyncSequence Support

public extension ConsumerProtocol {
    
    /// Returns an AsyncSequence of messages
    var messages: AsyncThrowingStream<Message<MessageType>, Error> {
        AsyncThrowingStream { continuation in
            let task = Task {
                do {
                    while !Task.isCancelled {
                        let message = try await receive()
                        continuation.yield(message)
                    }
                    continuation.finish()
                } catch {
                    continuation.finish(throwing: error)
                }
            }
            
            continuation.onTermination = { _ in
                task.cancel()
            }
        }
    }
}

// MARK: - Seek Extensions

public extension ConsumerProtocol {
    
    /// Seek to the earliest available message
    func seekToEarliest() async throws {
        try await seek(to: .earliest)
    }
    
    /// Seek to the latest message
    func seekToLatest() async throws {
        try await seek(to: .latest)
    }
    
    /// Seek to a message published after the given date
    func seek(after date: Date) async throws {
        try await seek(to: date)
    }
}

// MARK: - Filtering Extensions

public extension ConsumerProtocol {
    
    /// Receive messages that match a predicate
    func receiveWhere(_ predicate: (Message<MessageType>) -> Bool) async throws -> Message<MessageType> {
        while true {
            let message = try await receive()
            if predicate(message) {
                return message
            } else {
                // Acknowledge messages that don't match
                try await acknowledge(message)
            }
        }
    }
    
    /// Receive messages with a specific key
    func receive(withKey key: String) async throws -> Message<MessageType> {
        return try await receiveWhere { $0.key == key }
    }
}