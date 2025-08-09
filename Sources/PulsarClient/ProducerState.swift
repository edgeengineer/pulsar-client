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

/// The state of the producer
public enum ProducerState: Sendable, Equatable {
  /// The producer is closed
  case closed

  /// The producer is connected
  case connected

  /// The producer is disconnected
  case disconnected

  /// The producer is faulted
  case faulted(Error)

  /// The producer is partially connected (multi-topic)
  case partiallyConnected

  /// The producer is waiting for exclusive access
  case waitingForExclusive

  /// The producer has been fenced by the broker
  case fenced

  public static func == (lhs: ProducerState, rhs: ProducerState) -> Bool {
    switch (lhs, rhs) {
    case (.closed, .closed),
      (.connected, .connected),
      (.disconnected, .disconnected),
      (.partiallyConnected, .partiallyConnected),
      (.waitingForExclusive, .waitingForExclusive),
      (.fenced, .fenced):
      return true
    case (.faulted(_), .faulted(_)):
      // Consider all faulted states equal for comparison
      return true
    default:
      return false
    }
  }

  /// Indicates if this is a final state (cannot transition from this state)
  public var isFinal: Bool {
    switch self {
    case .closed, .fenced:
      return true
    default:
      return false
    }
  }

  /// Indicates if the producer is in an active state
  public var isActive: Bool {
    switch self {
    case .connected, .partiallyConnected:
      return true
    default:
      return false
    }
  }
}

// MARK: - CustomStringConvertible

extension ProducerState: CustomStringConvertible {
  public var description: String {
    switch self {
    case .closed:
      return "Closed"
    case .connected:
      return "Connected"
    case .disconnected:
      return "Disconnected"
    case .faulted(let error):
      return "Faulted: \(error)"
    case .partiallyConnected:
      return "PartiallyConnected"
    case .waitingForExclusive:
      return "WaitingForExclusive"
    case .fenced:
      return "Fenced"
    }
  }
}

// MARK: - Conversion to/from ClientState

extension ProducerState {
  /// Convert from generic ClientState to ProducerState
  public init(from clientState: ClientState) {
    switch clientState {
    case .disconnected:
      self = .disconnected
    case .initializing, .connecting:
      self = .disconnected
    case .connected:
      self = .connected
    case .reconnecting:
      self = .disconnected
    case .closing, .closed:
      self = .closed
    case .faulted(let error):
      self = .faulted(error)
    }
  }

  /// Convert to generic ClientState
  public var toClientState: ClientState {
    switch self {
    case .closed:
      return .closed
    case .connected:
      return .connected
    case .disconnected:
      return .disconnected
    case .faulted(let error):
      return .faulted(error)
    case .partiallyConnected:
      return .connected  // Map to closest equivalent
    case .waitingForExclusive:
      return .connecting  // Map to closest equivalent
    case .fenced:
      return .closed  // Fenced is effectively closed
    }
  }
}
