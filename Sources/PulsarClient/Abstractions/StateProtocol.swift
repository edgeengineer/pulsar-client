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
extension ClientState {
  /// Check if this is a final state
  public var isFinal: Bool {
    switch self {
    case .closed, .faulted:
      return true
    default:
      return false
    }
  }
}

/// Extension for ConnectionState to define final states
extension ConnectionState {
  /// Check if this is a final state
  public var isFinal: Bool {
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
  private let finalStateChecker: @Sendable (T) -> Bool
  private var stateChangeContinuations = [UUID: AsyncStream<T>.Continuation]()

  public var currentState: T {
    state
  }

  public nonisolated var stateChanges: AsyncStream<T> {
    AsyncStream { continuation in
      Task { [weak self] in
        guard let self = self else { return }
        for await state in await self.stateStream {
          continuation.yield(state)
        }
        continuation.finish()
      }
    }
  }

  public var stateStream: AsyncStream<T> {
    AsyncStream { continuation in
      let id = UUID()
      stateChangeContinuations[id] = continuation
      continuation.onTermination = { @Sendable _ in
        Task {
          await self.removeContinuation(id: id)
        }
      }
    }
  }

  public init(initialState: T, finalStateChecker: @escaping @Sendable (T) -> Bool) {
    self.state = initialState
    self.finalStateChecker = finalStateChecker
  }

  public func updateState(_ newState: T) {
    state = newState
    for continuation in stateChangeContinuations.values {
      continuation.yield(newState)
      if finalStateChecker(newState) {
        continuation.finish()
      }
    }
  }

  private func removeContinuation(id: UUID) {
    stateChangeContinuations.removeValue(forKey: id)
  }

  public nonisolated func onStateChange(_ handler: @escaping @Sendable (T) -> Void) {
    Task { [weak self] in
      guard let self = self else { return }
      for await state in await self.stateStream {
        handler(state)
      }
    }
  }

  public nonisolated func handleException(_ error: any Error) {
    // Implementation needed
  }

  public func stateChangedTo(_ state: T, timeout: TimeInterval) async throws -> T {
    if self.state == state {
      return self.state
    }
    // Wait for state change with timeout
    return try await withTimeout(seconds: timeout) { [self] in
      for await currentState in await self.stateStream {
        if currentState == state {
          return currentState
        }
      }
      return await self.state
    }
  }

  public func stateChangedFrom(_ state: T, timeout: TimeInterval) async throws -> T {
    if self.state != state {
      return self.state
    }
    // Wait for state change with timeout
    return try await withTimeout(seconds: timeout) { [self] in
      for await currentState in await self.stateStream {
        if currentState != state {
          return currentState
        }
      }
      return await self.state
    }
  }

  public func isFinalState() async -> Bool {
    finalStateChecker(state)
  }

  public func isState(_ state: T) async -> Bool {
    self.state == state
  }

  public func waitForState(_ state: T, timeout: TimeInterval?) async throws {
    guard self.state != state else { return }

    let timeoutValue = timeout ?? 30.0
    try await withThrowingTaskGroup(of: Void.self) { group in
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeoutValue * 1_000_000_000))
        throw PulsarClientError.timeout("Timeout waiting for state \(state)")
      }

      group.addTask { [self] in
        for await currentState in await self.stateStream {
          if currentState == state {
            return
          }
        }
      }

      try await group.next()
      group.cancelAll()
    }
  }

  public func waitForAnyState(_ states: T..., timeout: TimeInterval?) async throws -> T {
    if states.contains(state) {
      return state
    }

    let timeoutValue = timeout ?? 30.0
    return try await withThrowingTaskGroup(of: T?.self) { group in
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeoutValue * 1_000_000_000))
        return nil
      }

      group.addTask { [self] in
        for await currentState in await self.stateStream {
          if states.contains(currentState) {
            return currentState
          }
        }
        return nil
      }

      if let result = try await group.next(), let state = result {
        group.cancelAll()
        return state
      } else {
        throw PulsarClientError.timeout("Timeout waiting for any of states: \(states)")
      }
    }
  }

  public func waitForFinalState(timeout: TimeInterval?) async throws -> T {
    if finalStateChecker(state) {
      return state
    }

    let timeoutValue = timeout ?? 30.0
    return try await withThrowingTaskGroup(of: T?.self) { group in
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeoutValue * 1_000_000_000))
        return nil
      }

      group.addTask { [self] in
        for await currentState in await self.stateStream {
          if self.finalStateChecker(currentState) {
            return currentState
          }
        }
        return nil
      }

      if let result = try await group.next(), let state = result {
        group.cancelAll()
        return state
      } else {
        throw PulsarClientError.timeout("Timeout waiting for final state")
      }
    }
  }
}

public protocol StateHolder: Sendable {
  associatedtype T: Equatable & Sendable
  var state: T { get }
  var stateStream: AsyncStream<T> { get }

  func onStateChange(_ handler: @escaping @Sendable (T) -> Void)
  func isFinal() -> Bool
  func handleException(_ error: any Error)

  func stateChangedTo(_ state: T, timeout: TimeInterval) async throws -> T
  func stateChangedFrom(_ state: T, timeout: TimeInterval) async throws -> T
}
