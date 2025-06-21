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

/// The possible states a consumer can be in
public enum ConsumerState: Sendable, Equatable {
    /// The consumer is connected and active. The subscription type is 'Failover' and this consumer is the active consumer.
    case active
    
    /// The consumer is closed. This is a final state.
    case closed
    
    /// The consumer is disconnected
    case disconnected
    
    /// The consumer is faulted. This is a final state.
    case faulted(Error)
    
    /// Some of the sub-consumers are disconnected
    case partiallyConnected
    
    /// The consumer is connected and inactive. The subscription type is 'Failover' and this consumer is not the active consumer.
    case inactive
    
    /// The consumer has reached the end of the topic. This is a final state.
    case reachedEndOfTopic
    
    /// The consumer has unsubscribed. This is a final state.
    case unsubscribed
    
    public static func ==(lhs: ConsumerState, rhs: ConsumerState) -> Bool {
        switch (lhs, rhs) {
        case (.active, .active),
             (.closed, .closed),
             (.disconnected, .disconnected),
             (.partiallyConnected, .partiallyConnected),
             (.inactive, .inactive),
             (.reachedEndOfTopic, .reachedEndOfTopic),
             (.unsubscribed, .unsubscribed):
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
        case .closed, .faulted, .reachedEndOfTopic, .unsubscribed:
            return true
        default:
            return false
        }
    }
    
    /// Indicates if the consumer is in an active receiving state
    public var isActive: Bool {
        switch self {
        case .active, .partiallyConnected:
            return true
        case .inactive:
            // Inactive consumers in failover mode can still receive messages if they become active
            return false
        default:
            return false
        }
    }
}

// MARK: - CustomStringConvertible

extension ConsumerState: CustomStringConvertible {
    public var description: String {
        switch self {
        case .active:
            return "Active"
        case .closed:
            return "Closed"
        case .disconnected:
            return "Disconnected"
        case .faulted(let error):
            return "Faulted: \(error)"
        case .partiallyConnected:
            return "PartiallyConnected"
        case .inactive:
            return "Inactive"
        case .reachedEndOfTopic:
            return "ReachedEndOfTopic"
        case .unsubscribed:
            return "Unsubscribed"
        }
    }
}

// MARK: - Conversion to/from ClientState

extension ConsumerState {
    /// Convert from generic ClientState to ConsumerState
    public init(from clientState: ClientState) {
        switch clientState {
        case .disconnected:
            self = .disconnected
        case .initializing, .connecting:
            self = .disconnected
        case .connected:
            self = .active // Assume active by default
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
        case .active:
            return .connected
        case .closed:
            return .closed
        case .disconnected:
            return .disconnected
        case .faulted(let error):
            return .faulted(error)
        case .partiallyConnected:
            return .connected
        case .inactive:
            return .connected // Still connected, just not active
        case .reachedEndOfTopic:
            return .closed
        case .unsubscribed:
            return .closed
        }
    }
}