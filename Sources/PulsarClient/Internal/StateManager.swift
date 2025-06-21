import Foundation
import Logging

/// Generic state manager for component lifecycle
public actor StateManager<State: Equatable & Sendable> {
    private var currentState: State
    private var stateListeners: [any StateListener<State>] = []
    private let logger: Logger
    private let componentName: String
    
    public init(initialState: State, componentName: String, logger: Logger? = nil) {
        self.currentState = initialState
        self.componentName = componentName
        self.logger = logger ?? Logger(label: "StateManager.\(componentName)")
    }
    
    /// Get current state
    public var state: State {
        return currentState
    }
    
    /// Transition to a new state
    public func transitionTo(_ newState: State) {
        guard newState != currentState else { return }
        
        let oldState = currentState
        currentState = newState
        
        logger.debug("\(componentName) state changed: \(oldState) -> \(newState)")
        
        // Notify all listeners
        let change = StateChange(from: oldState, to: newState)
        for listener in stateListeners {
            Task {
                await listener.onStateChange(change)
            }
        }
    }
    
    /// Check if in a specific state
    public func isInState(_ state: State) -> Bool {
        return currentState == state
    }
    
    /// Check if in any of the specified states
    public func isInAnyState(_ states: State...) -> Bool {
        return states.contains(currentState)
    }
    
    /// Wait for a specific state
    public func waitForState(_ targetState: State, timeout: TimeInterval = 30.0) async throws {
        guard currentState != targetState else { return }
        
        let listener = StateWaiter(targetState: targetState)
        addListener(listener)
        
        do {
            try await withTimeout(seconds: timeout) {
                try await listener.wait()
            }
        } catch {
            removeListener(listener)
            throw error
        }
        
        removeListener(listener)
    }
    
    /// Wait for any of the specified states
    public func waitForAnyState(_ states: State..., timeout: TimeInterval = 30.0) async throws -> State {
        guard !states.contains(currentState) else { return currentState }
        
        let listener = StateWaiter(targetStates: states)
        addListener(listener)
        
        do {
            try await withTimeout(seconds: timeout) {
                try await listener.wait()
            }
            let result = currentState
            removeListener(listener)
            return result
        } catch {
            removeListener(listener)
            throw error
        }
    }
    
    /// Add a state change listener
    public func addListener(_ listener: any StateListener<State>) {
        stateListeners.append(listener)
    }
    
    /// Remove a state change listener
    public func removeListener(_ listener: any StateListener<State>) {
        if let index = stateListeners.firstIndex(where: { $0 === listener }) {
            stateListeners.remove(at: index)
        }
    }
}

/// State change event
public struct StateChange<State: Sendable>: Sendable {
    public let from: State
    public let to: State
    
    public init(from: State, to: State) {
        self.from = from
        self.to = to
    }
}

/// State change listener protocol
public protocol StateListener<State>: AnyObject, Sendable {
    associatedtype State: Sendable
    func onStateChange(_ change: StateChange<State>) async
}

/// Internal state waiter for waiting on state transitions
private final class StateWaiter<State: Equatable & Sendable>: StateListener, @unchecked Sendable {
    private let targetStates: [State]
    private var continuation: CheckedContinuation<Void, Error>?
    
    init(targetState: State) {
        self.targetStates = [targetState]
    }
    
    init(targetStates: [State]) {
        self.targetStates = targetStates
    }
    
    func onStateChange(_ change: StateChange<State>) async {
        if targetStates.contains(change.to) {
            continuation?.resume()
            continuation = nil
        }
    }
    
    func wait() async throws {
        try await withCheckedThrowingContinuation { continuation in
            self.continuation = continuation
        }
    }
}

/// Connection state manager
public typealias ConnectionStateManager = StateManager<ConnectionState>

/// Producer state manager  
public typealias ProducerStateManager = StateManager<ClientState>

/// Consumer state manager
public typealias ConsumerStateManager = StateManager<ClientState>

/// State manager factory
public enum StateManagerFactory {
    
    /// Create a connection state manager
    public static func createConnectionStateManager(
        initialState: ConnectionState = .disconnected,
        componentName: String = "Connection"
    ) -> ConnectionStateManager {
        return StateManager(
            initialState: initialState,
            componentName: componentName
        )
    }
    
    /// Create a producer state manager
    public static func createProducerStateManager(
        initialState: ClientState = .initializing,
        componentName: String = "Producer"
    ) -> ProducerStateManager {
        return StateManager(
            initialState: initialState,
            componentName: componentName
        )
    }
    
    /// Create a consumer state manager
    public static func createConsumerStateManager(
        initialState: ClientState = .initializing,
        componentName: String = "Consumer"
    ) -> ConsumerStateManager {
        return StateManager(
            initialState: initialState,
            componentName: componentName
        )
    }
}

/// State change notification system
public actor StateChangeNotifier {
    private var globalListeners: [any StateListener] = []
    
    /// Add a global state listener that gets notified of all state changes
    public func addGlobalListener<State>(_ listener: any StateListener<State>) {
        globalListeners.append(listener)
    }
    
    /// Notify all global listeners of a state change
    public func notify<State>(_ change: StateChange<State>) {
        for listener in globalListeners {
            if let typedListener = listener as? any StateListener<State> {
                Task {
                    await typedListener.onStateChange(change)
                }
            }
        }
    }
}