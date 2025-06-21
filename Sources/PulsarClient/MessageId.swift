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

/// Unique identifier of a single message.
public struct MessageId: Sendable, Hashable, Comparable, CustomStringConvertible {
    
    // MARK: - Properties
    
    /// The id of the ledger.
    public let ledgerId: UInt64
    
    /// The id of the entry.
    public let entryId: UInt64
    
    /// The partition.
    public let partition: Int32
    
    /// The batch index.
    public let batchIndex: Int32
    
    /// The full topic name of the message.
    public let topic: String
    
    // MARK: - Static Properties
    
    /// The oldest message available in the topic.
    public static let earliest = MessageId(
        ledgerId: UInt64.max,
        entryId: UInt64.max,
        partition: -1,
        batchIndex: -1
    )
    
    /// The next message published in the topic.
    public static let latest = MessageId(
        ledgerId: Int64.max.magnitude,
        entryId: Int64.max.magnitude,
        partition: -1,
        batchIndex: -1
    )
    
    // MARK: - Initialization
    
    /// Initializes a new instance using the specified ledgerId, entryId, partition, and batchIndex.
    public init(
        ledgerId: UInt64,
        entryId: UInt64,
        partition: Int32 = -1,
        batchIndex: Int32 = -1,
        topic: String = ""
    ) {
        self.ledgerId = ledgerId
        self.entryId = entryId
        self.partition = partition
        self.batchIndex = batchIndex
        self.topic = topic
    }
    
    // MARK: - Comparable
    
    public static func < (lhs: MessageId, rhs: MessageId) -> Bool {
        // Compare ledger ID
        if lhs.ledgerId != rhs.ledgerId {
            return lhs.ledgerId < rhs.ledgerId
        }
        
        // Compare entry ID
        if lhs.entryId != rhs.entryId {
            return lhs.entryId < rhs.entryId
        }
        
        // Compare partition
        if lhs.partition != rhs.partition {
            return lhs.partition < rhs.partition
        }
        
        // Compare topic
        let topicComparison = lhs.topic.compare(rhs.topic)
        if topicComparison != .orderedSame {
            return topicComparison == .orderedAscending
        }
        
        // Compare batch index
        return lhs.batchIndex < rhs.batchIndex
    }
    
    // MARK: - CustomStringConvertible
    
    public var description: String {
        if topic.isEmpty {
            return "\(ledgerId):\(entryId):\(partition):\(batchIndex)"
        }
        return "\(ledgerId):\(entryId):\(partition):\(batchIndex):\(topic)"
    }
    
    // MARK: - Parsing
    
    /// Converts the string representation of a message id to its object equivalent.
    /// - Parameter string: A string containing a message id to convert.
    /// - Returns: The MessageId equivalent of the string, or nil if the conversion failed.
    public static func parse(_ string: String) -> MessageId? {
        guard !string.isEmpty else { return nil }
        
        let components = string.split(separator: ":", maxSplits: 4)
        guard components.count >= 4 else { return nil }
        
        // Parse ledger ID
        guard let ledgerId = UInt64(components[0]) else { return nil }
        
        // Parse entry ID
        guard let entryId = UInt64(components[1]) else { return nil }
        
        // Parse partition
        guard let partition = Int32(components[2]) else { return nil }
        
        // Parse batch index
        guard let batchIndex = Int32(components[3]) else { return nil }
        
        // Parse topic (optional, everything after the 4th colon)
        let topic = components.count > 4 ? String(components[4]) : ""
        
        return MessageId(
            ledgerId: ledgerId,
            entryId: entryId,
            partition: partition,
            batchIndex: batchIndex,
            topic: topic
        )
    }
    
    /// Converts the string representation of a message id to its object equivalent.
    /// - Parameters:
    ///   - string: A string containing a message id to convert.
    ///   - result: When this method returns, contains the MessageId equivalent of the string if the conversion succeeded, or MessageId.earliest if the conversion failed.
    /// - Returns: true if the string was converted successfully; otherwise, false.
    public static func tryParse(_ string: String, result: inout MessageId) -> Bool {
        if let parsed = parse(string) {
            result = parsed
            return true
        } else {
            result = .earliest
            return false
        }
    }
}

// MARK: - Internal Extensions

extension MessageId {
    /// Convert to Protocol Buffer MessageIdData
    internal func toProto() -> Pulsar_Proto_MessageIdData {
        var proto = Pulsar_Proto_MessageIdData()
        proto.ledgerID = ledgerId
        proto.entryID = entryId
        
        if partition >= 0 {
            proto.partition = partition
        }
        
        if batchIndex >= 0 {
            proto.batchIndex = batchIndex
        }
        
        return proto
    }
    
    /// Create from Protocol Buffer MessageIdData
    internal init(from proto: Pulsar_Proto_MessageIdData, topic: String = "") {
        self.ledgerId = proto.ledgerID
        self.entryId = proto.entryID
        self.partition = proto.hasPartition ? proto.partition : -1
        self.batchIndex = proto.hasBatchIndex ? proto.batchIndex : -1
        self.topic = topic
    }
}

// MARK: - Operators

extension MessageId {
    public static func > (lhs: MessageId, rhs: MessageId) -> Bool {
        return !(lhs <= rhs)
    }
    
    public static func >= (lhs: MessageId, rhs: MessageId) -> Bool {
        return !(lhs < rhs)
    }
    
    public static func <= (lhs: MessageId, rhs: MessageId) -> Bool {
        return lhs < rhs || lhs == rhs
    }
}