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

/// A message routing abstraction
public protocol MessageRouter: Sendable {
    /// Choose a partition for the given message metadata
    /// - Parameters:
    ///   - messageMetadata: The metadata of the message to route
    ///   - numberOfPartitions: The total number of partitions available
    /// - Returns: The partition index (0-based) to send the message to
    func choosePartition(messageMetadata: MessageMetadata, numberOfPartitions: Int) -> Int
}

/// Default message router implementations
public enum MessageRouterType {
    /// Round-robin routing across partitions
    case roundRobin
    
    /// Single partition routing (always partition 0)
    case singlePartition
    
    /// Custom routing with a closure
    case custom(@Sendable (MessageMetadata, Int) -> Int)
    
    /// Convert to a MessageRouter implementation
    public func toRouter() -> any MessageRouter {
        switch self {
        case .roundRobin:
            return RoundRobinMessageRouter()
        case .singlePartition:
            return SinglePartitionMessageRouter()
        case .custom(let chooser):
            return CustomMessageRouter(chooser: chooser)
        }
    }
}

/// Single partition message router (always returns partition 0)
public struct SinglePartitionMessageRouter: MessageRouter {
    public init() {}
    
    public func choosePartition(messageMetadata: MessageMetadata, numberOfPartitions: Int) -> Int {
        return 0
    }
}

/// Custom message router with a closure
public struct CustomMessageRouter: MessageRouter {
    private let chooser: @Sendable (MessageMetadata, Int) -> Int
    
    public init(chooser: @escaping @Sendable (MessageMetadata, Int) -> Int) {
        self.chooser = chooser
    }
    
    public func choosePartition(messageMetadata: MessageMetadata, numberOfPartitions: Int) -> Int {
        return chooser(messageMetadata, numberOfPartitions)
    }
}

/// Key-based message router that uses the partition key for consistent hashing
public struct KeyBasedMessageRouter: MessageRouter {
    public init() {}
    
    public func choosePartition(messageMetadata: MessageMetadata, numberOfPartitions: Int) -> Int {
        // If there's a partition key, use it for consistent hashing
        if let key = messageMetadata.key {
            let hash = key.hashValue
            return abs(hash) % numberOfPartitions
        }
        
        // If there's an ordering key, use it
        if let orderingKey = messageMetadata.orderingKey, !orderingKey.isEmpty {
            let hash = orderingKey.hashValue
            return abs(hash) % numberOfPartitions
        }
        
        // Default to partition 0 if no key is present
        return 0
    }
}