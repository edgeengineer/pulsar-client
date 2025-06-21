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

/// The possible states a reader can be in
public enum ReaderState: Sendable, Equatable {
    /// The reader is closed. This is a final state.
    case closed
    
    /// The reader is connected
    case connected
    
    /// The reader is disconnected
    case disconnected
    
    /// The reader is faulted. This is a final state.
    case faulted(Error)
    
    /// Some of the sub-readers are disconnected
    case partiallyConnected
    
    /// The reader has reached the end of the topic. This is a final state.
    case reachedEndOfTopic
    
    public static func ==(lhs: ReaderState, rhs: ReaderState) -> Bool {
        switch (lhs, rhs) {
        case (.closed, .closed),
             (.connected, .connected),
             (.disconnected, .disconnected),
             (.partiallyConnected, .partiallyConnected),
             (.reachedEndOfTopic, .reachedEndOfTopic):
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
        case .closed, .faulted, .reachedEndOfTopic:
            return true
        default:
            return false
        }
    }
    
    /// Indicates if the reader is in an active reading state
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

extension ReaderState: CustomStringConvertible {
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
        case .reachedEndOfTopic:
            return "ReachedEndOfTopic"
        }
    }
}

// MARK: - Conversion to/from ClientState

extension ReaderState {
    /// Convert from generic ClientState to ReaderState
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
        case .closing:
            self = .disconnected
        case .closed:
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
            return .connected
        case .reachedEndOfTopic:
            return .closed
        }
    }
}