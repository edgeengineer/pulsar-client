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

// MARK: - State Change Direction

/// Represents the direction of a state change
public enum StateChangeDirection: Sendable {
    /// The state changed to this value
    case to
    /// The state changed from this value
    case from
}

// MARK: - Generic State Transition

/// Generic state transition representation with timestamp
public struct StateTransition<State: Sendable>: Sendable {
    /// The previous state
    public let from: State
    /// The new state
    public let to: State
    /// When the change occurred
    public let timestamp: Date
    
    public init(from: State, to: State, timestamp: Date = Date()) {
        self.from = from
        self.to = to
        self.timestamp = timestamp
    }
}

// MARK: - Consumer State Change

/// Representation of a consumer state change
public struct ConsumerStateChanged<T>: Sendable where T: Sendable {
    /// The consumer that changed state
    public let consumer: any ConsumerProtocol<T>
    /// The state that it changed to
    public let state: ClientState
    /// The previous state
    public let previousState: ClientState
    /// When the change occurred
    public let timestamp: Date
    
    public init(
        consumer: any ConsumerProtocol<T>,
        state: ClientState,
        previousState: ClientState,
        timestamp: Date = Date()
    ) {
        self.consumer = consumer
        self.state = state
        self.previousState = previousState
        self.timestamp = timestamp
    }
    
    /// Get the state change as a generic StateTransition
    public var stateTransition: StateTransition<ClientState> {
        StateTransition(from: previousState, to: state, timestamp: timestamp)
    }
}

// MARK: - Producer State Change

/// Representation of a producer state change
public struct ProducerStateChanged<T>: Sendable where T: Sendable {
    /// The producer that changed state
    public let producer: any ProducerProtocol<T>
    /// The state that it changed to
    public let state: ClientState
    /// The previous state
    public let previousState: ClientState
    /// When the change occurred
    public let timestamp: Date
    
    public init(
        producer: any ProducerProtocol<T>,
        state: ClientState,
        previousState: ClientState,
        timestamp: Date = Date()
    ) {
        self.producer = producer
        self.state = state
        self.previousState = previousState
        self.timestamp = timestamp
    }
    
    /// Get the state change as a generic StateTransition
    public var stateTransition: StateTransition<ClientState> {
        StateTransition(from: previousState, to: state, timestamp: timestamp)
    }
}

// MARK: - Reader State Change

/// Representation of a reader state change
public struct ReaderStateChanged<T>: Sendable where T: Sendable {
    /// The reader that changed state
    public let reader: any ReaderProtocol<T>
    /// The state that it changed to
    public let state: ClientState
    /// The previous state
    public let previousState: ClientState
    /// When the change occurred
    public let timestamp: Date
    
    public init(
        reader: any ReaderProtocol<T>,
        state: ClientState,
        previousState: ClientState,
        timestamp: Date = Date()
    ) {
        self.reader = reader
        self.state = state
        self.previousState = previousState
        self.timestamp = timestamp
    }
    
    /// Get the state change as a generic StateTransition
    public var stateTransition: StateTransition<ClientState> {
        StateTransition(from: previousState, to: state, timestamp: timestamp)
    }
}

// MARK: - State Change Handler Protocol

/// Protocol for handling state changes
public protocol StateChangeHandler: Sendable {
    associatedtype T: Sendable
    
    /// Handle a state change
    func handleStateChange(_ stateChange: StateChange<ClientState>) async
}

// MARK: - Type-Specific State Change Handlers

/// Consumer state change handler
public protocol ConsumerStateChangeHandler: Sendable {
    associatedtype T: Sendable
    
    /// Handle a consumer state change
    func handleConsumerStateChange(_ change: ConsumerStateChanged<T>) async
}

/// Producer state change handler
public protocol ProducerStateChangeHandler: Sendable {
    associatedtype T: Sendable
    
    /// Handle a producer state change
    func handleProducerStateChange(_ change: ProducerStateChanged<T>) async
}

/// Reader state change handler
public protocol ReaderStateChangeHandler: Sendable {
    associatedtype T: Sendable
    
    /// Handle a reader state change
    func handleReaderStateChange(_ change: ReaderStateChanged<T>) async
}

// MARK: - Closure-based State Change Handlers

/// Closure-based consumer state change handler
public struct ClosureConsumerStateChangeHandler<T: Sendable>: ConsumerStateChangeHandler {
    private let handler: @Sendable (ConsumerStateChanged<T>) async -> Void
    
    public init(_ handler: @escaping @Sendable (ConsumerStateChanged<T>) async -> Void) {
        self.handler = handler
    }
    
    public func handleConsumerStateChange(_ change: ConsumerStateChanged<T>) async {
        await handler(change)
    }
}

/// Closure-based producer state change handler
public struct ClosureProducerStateChangeHandler<T: Sendable>: ProducerStateChangeHandler {
    private let handler: @Sendable (ProducerStateChanged<T>) async -> Void
    
    public init(_ handler: @escaping @Sendable (ProducerStateChanged<T>) async -> Void) {
        self.handler = handler
    }
    
    public func handleProducerStateChange(_ change: ProducerStateChanged<T>) async {
        await handler(change)
    }
}

/// Closure-based reader state change handler
public struct ClosureReaderStateChangeHandler<T: Sendable>: ReaderStateChangeHandler {
    private let handler: @Sendable (ReaderStateChanged<T>) async -> Void
    
    public init(_ handler: @escaping @Sendable (ReaderStateChanged<T>) async -> Void) {
        self.handler = handler
    }
    
    public func handleReaderStateChange(_ change: ReaderStateChanged<T>) async {
        await handler(change)
    }
}

// MARK: - State Extensions

public extension ClientState {
    /// Check if the state represents a connected state
    var isConnected: Bool {
        self == .connected
    }
    
    /// Check if the state represents a terminal state
    var isTerminal: Bool {
        switch self {
        case .closed, .faulted:
            return true
        default:
            return false
        }
    }
    
    /// Check if the state represents a transitional state
    var isTransitional: Bool {
        switch self {
        case .initializing, .connecting, .reconnecting, .closing:
            return true
        default:
            return false
        }
    }
    
    /// Check if the state represents an error state
    var isError: Bool {
        if case .faulted = self {
            return true
        }
        return false
    }
    
    /// Get the error if the state is faulted
    var error: Error? {
        if case .faulted(let error) = self {
            return error
        }
        return nil
    }
}

// MARK: - State Monitoring

/// State monitor for tracking state changes
public actor StateMonitor<T: Sendable> {
    private var stateHistory: [StateTransition<ClientState>] = []
    private let maxHistorySize: Int
    
    public init(maxHistorySize: Int = 100) {
        self.maxHistorySize = maxHistorySize
    }
    
    /// Record a state change
    public func recordStateChange(_ change: StateTransition<ClientState>) {
        stateHistory.append(change)
        
        // Trim history if needed
        if stateHistory.count > maxHistorySize {
            stateHistory.removeFirst(stateHistory.count - maxHistorySize)
        }
    }
    
    /// Get the state history
    public func getHistory() -> [StateTransition<ClientState>] {
        return stateHistory
    }
    
    /// Get state changes within a time range
    public func getHistory(from startTime: Date, to endTime: Date? = nil) -> [StateTransition<ClientState>] {
        let endTime = endTime ?? Date()
        return stateHistory.filter { change in
            change.timestamp >= startTime && change.timestamp <= endTime
        }
    }
    
    /// Clear the state history
    public func clearHistory() {
        stateHistory.removeAll()
    }
    
    /// Get the last state change
    public func getLastStateChange() -> StateTransition<ClientState>? {
        return stateHistory.last
    }
    
    /// Get the number of times a specific state was entered
    public func getStateEntryCount(_ state: ClientState) -> Int {
        return stateHistory.filter { $0.to == state }.count
    }
}

// MARK: - State Transition Validation

public extension ClientState {
    /// Check if a transition from this state to another is valid
    func canTransition(to newState: ClientState) -> Bool {
        switch (self, newState) {
        // From disconnected
        case (.disconnected, .initializing),
             (.disconnected, .connecting):
            return true
            
        // From initializing
        case (.initializing, .connecting),
             (.initializing, .closed),
             (.initializing, .faulted):
            return true
            
        // From connecting
        case (.connecting, .connected),
             (.connecting, .disconnected),
             (.connecting, .closed),
             (.connecting, .faulted):
            return true
            
        // From connected
        case (.connected, .reconnecting),
             (.connected, .closing),
             (.connected, .disconnected),
             (.connected, .faulted):
            return true
            
        // From reconnecting
        case (.reconnecting, .connected),
             (.reconnecting, .disconnected),
             (.reconnecting, .closing),
             (.reconnecting, .faulted):
            return true
            
        // From closing
        case (.closing, .closed),
             (.closing, .faulted):
            return true
            
        // From closed - no valid transitions
        case (.closed, _):
            return false
            
        // From faulted
        case (.faulted, .closing),
             (.faulted, .closed):
            return true
            
        default:
            return false
        }
    }
}