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

/// Protocol for advanced state monitoring
public protocol StateProtocol<T>: Sendable where T: Equatable & Sendable {
    associatedtype T
    
    /// The current state
    var currentState: T { get async }
    
    /// Check if the current state is a final state
    func isFinalState() async -> Bool
    
    /// Check if the current state matches the given state
    func isState(_ state: T) async -> Bool
    
    /// Wait for a specific state
    func waitForState(_ state: T, timeout: TimeInterval?) async throws
    
    /// Wait for any of the given states
    func waitForAnyState(_ states: T..., timeout: TimeInterval?) async throws -> T
    
    /// Wait for a final state
    func waitForFinalState(timeout: TimeInterval?) async throws -> T
    
    /// Get an async stream of state changes
    var stateChanges: AsyncStream<T> { get }

    func onStateChange(_ handler: @escaping @Sendable (T) -> Void)
    func handleException(_ error: any Error)

    func stateChangedTo(_ state: T, timeout: TimeInterval) async throws -> T
    func stateChangedFrom(_ state: T, timeout: TimeInterval) async throws -> T
}

/// Extension for ClientState to define final states
public extension ClientState {
    /// Check if this is a final state
    var isFinal: Bool {
        switch self {
        case .closed, .faulted:
            return true
        default:
            return false
        }
    }
}

/// Extension for ConnectionState to define final states
public extension ConnectionState {
    /// Check if this is a final state
    var isFinal: Bool {
        switch self {
        case .closed, .faulted:
            return true
        default:
            return false
        }
    }
}

/// Default implementation of StateProtocol
public actor StateContainer<T>: StateProtocol where T: Equatable & Sendable {
    private var state: T
    private let finalStateChecker: (T) -> Bool
    private let stream: AsyncStream<T>
    private let continuation: AsyncStream<T>.Continuation
    
    public init(initialState: T, isFinalState: @escaping (T) -> Bool) {
        self.state = initialState
        self.finalStateChecker = isFinalState
        
        (self.stream, self.continuation) = AsyncStream<T>.makeStream()
    }
    
    public var currentState: T {
        return state
    }
    
    public func isFinalState() async -> Bool {
        return finalStateChecker(state)
    }
    
    public func isState(_ state: T) async -> Bool {
        return self.state == state
    }
    
    public func waitForState(_ targetState: T, timeout: TimeInterval? = nil) async throws {
        guard state != targetState else { return }
        
        let task = Task {
            for await newState in stream {
                if newState == targetState {
                    return
                }
            }
            throw PulsarClientError.timeout("State change timeout")
        }
        
        if let timeout = timeout {
            try await withTimeout(seconds: timeout) {
                try await task.value
            }
        } else {
            try await task.value
        }
    }
    
    public func waitForAnyState(_ states: T..., timeout: TimeInterval? = nil) async throws -> T {
        if states.contains(state) {
            return state
        }
        
        let task = Task {
            for await newState in stream {
                if states.contains(newState) {
                    return newState
                }
            }
            throw PulsarClientError.timeout("State change timeout")
        }
        
        if let timeout = timeout {
            return try await withTimeout(seconds: timeout) {
                try await task.value
            }
        } else {
            return try await task.value
        }
    }
    
    public func waitForFinalState(timeout: TimeInterval? = nil) async throws -> T {
        if finalStateChecker(state) {
            return state
        }
        
        let task = Task {
            for await newState in stream {
                if finalStateChecker(newState) {
                    return newState
                }
            }
            throw PulsarClientError.timeout("State change timeout")
        }
        
        if let timeout = timeout {
            return try await withTimeout(seconds: timeout) {
                try await task.value
            }
        } else {
            return try await task.value
        }
    }
    
    public nonisolated var stateChanges: AsyncStream<T> {
        return stream
    }
    
    /// Update the state (internal use)
    func updateState(_ newState: T) {
        guard newState != state else { return }
        state = newState
        continuation.yield(newState)
        
        if finalStateChecker(newState) {
            continuation.finish()
        }
    }
    
    deinit {
        continuation.finish()
    }

    func onStateChange(_ handler: @escaping @Sendable (T) -> Void) {
        Task {
            for await state in stateChanges {
                handler(state)
            }
        }
    }

    func handleException(_ error: any Error) {
        // Implementation needed
    }

    func stateChangedTo(_ state: T, timeout: TimeInterval) async throws -> T {
        if self.state == state {
            return self.state
        }
        return try await stateChanges.first { $0 == state }.async(timeout: timeout) ?? self.state
    }

    func stateChangedFrom(_ state: T, timeout: TimeInterval) async throws -> T {
        if self.state != state {
            return self.state
        }
        return try await stateChanges.first { $0 != state }.async(timeout: timeout) ?? self.state
    }
}

public protocol StateHolder: Sendable where T: Equatable & Sendable {
    var state: T { get }
    var stateStream: AsyncStream<T> { get }

    func onStateChange(_ handler: @escaping @Sendable (T) -> Void)
    func isFinal() -> Bool
    func handleException(_ error: any Error)

    func stateChangedTo(_ state: T, timeout: TimeInterval) async throws -> T
    func stateChangedFrom(_ state: T, timeout: TimeInterval) async throws -> T
}