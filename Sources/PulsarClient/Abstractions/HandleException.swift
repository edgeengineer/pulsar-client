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

/// Protocol for handling exceptions
public protocol HandleExceptionProtocol: Sendable {
  /// Handle an exception
  /// - Parameter context: The exception context
  func onException(_ context: ExceptionContext) async
}

/// Protocol for handling state changes
public protocol HandleStateChangedProtocol<TStateChanged>: Sendable {
  associatedtype TStateChanged: Sendable

  /// Handle a state change
  /// - Parameter stateChanged: The state change event
  func onStateChanged(_ stateChanged: TStateChanged) async
}

/// Simple exception handler implementation
public struct SimpleExceptionHandler: HandleExceptionProtocol {
  public init() {}

  public func onException(_ context: ExceptionContext) async {
    // Default implementation - just log the error
    print("[PulsarClient] Exception in \(context.operationType): \(context.exception)")
  }
}

/// Closure-based exception handler
public struct ClosureExceptionHandler: HandleExceptionProtocol {
  private let handler: @Sendable (ExceptionContext) async -> Void

  public init(_ handler: @escaping @Sendable (ExceptionContext) async -> Void) {
    self.handler = handler
  }

  public func onException(_ context: ExceptionContext) async {
    await handler(context)
  }
}

/// Closure-based state change handler
public struct ClosureStateChangeHandler<TStateChanged: Sendable>: HandleStateChangedProtocol {
  private let handler: @Sendable (TStateChanged) async -> Void

  public init(_ handler: @escaping @Sendable (TStateChanged) async -> Void) {
    self.handler = handler
  }

  public func onStateChanged(_ stateChanged: TStateChanged) async {
    await handler(stateChanged)
  }
}
