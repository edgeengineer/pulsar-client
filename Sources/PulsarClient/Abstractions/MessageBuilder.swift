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

/// Protocol for building messages with metadata
public protocol MessageBuilderProtocol<T>: Sendable where T: Sendable {
    associatedtype T
    
    /// The producer to send the message with
    var producer: any ProducerProtocol<T> { get }
    
    /// Set the message value
    func withValue(_ value: T) -> Self
    
    /// Set the key
    func withKey(_ key: String) -> Self
    
    /// Set the key as bytes
    func withKeyBytes(_ keyBytes: Data) -> Self
    
    /// Set the ordering key
    func withOrderingKey(_ orderingKey: Data) -> Self
    
    /// Set the event time
    func withEventTime(_ eventTime: Date) -> Self
    
    /// Set the deliver at time
    func withDeliverAt(_ deliverAt: Date) -> Self
    
    /// Set the deliver after delay
    func withDeliverAfter(_ delay: TimeInterval) -> Self
    
    /// Set the schema version
    func withSchemaVersion(_ schemaVersion: Data) -> Self
    
    /// Set the sequence id
    func withSequenceId(_ sequenceId: UInt64) -> Self
    
    /// Add a property
    func withProperty(_ key: String, _ value: String) -> Self
    
    /// Set all properties
    func withProperties(_ properties: [String: String]) -> Self
    
    /// Set the partition key
    func withPartitionKey(_ partitionKey: String) -> Self
    
    /// Set the disable replication flag
    func withDisableReplication(_ disable: Bool) -> Self
    
    /// Set the transaction for this message
    func withTransaction(_ transaction: Transaction) -> Self
    
    /// Send the message
    func send() async throws -> MessageId
}

/// Default message builder implementation
public struct MessageBuilder<T>: MessageBuilderProtocol where T: Sendable {
    public let producer: any ProducerProtocol<T>
    private var metadata: MessageMetadata
    private var value: T?
    private var transaction: Transaction?
    
    public init(producer: any ProducerProtocol<T>) {
        self.producer = producer
        self.metadata = MessageMetadata()
    }
    
    public func withValue(_ value: T) -> MessageBuilder<T> {
        var builder = self
        builder.value = value
        return builder
    }
    
    public func withKey(_ key: String) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withKey(key)
        return builder
    }
    
    public func withKeyBytes(_ keyBytes: Data) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withKeyBytes(keyBytes)
        return builder
    }
    
    public func withOrderingKey(_ orderingKey: Data) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withOrderingKey(orderingKey)
        return builder
    }
    
    public func withEventTime(_ eventTime: Date) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withEventTime(eventTime)
        return builder
    }
    
    public func withDeliverAt(_ deliverAt: Date) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withDeliverAt(deliverAt)
        return builder
    }
    
    public func withDeliverAfter(_ delay: TimeInterval) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withDeliverAfter(delay)
        return builder
    }
    
    public func withSchemaVersion(_ schemaVersion: Data) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withSchemaVersion(schemaVersion)
        return builder
    }
    
    public func withSequenceId(_ sequenceId: UInt64) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withSequenceId(sequenceId)
        return builder
    }
    
    public func withProperty(_ key: String, _ value: String) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withProperty(key, value)
        return builder
    }
    
    public func withProperties(_ properties: [String: String]) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withProperties(properties)
        return builder
    }
    
    public func withPartitionKey(_ partitionKey: String) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withKey(partitionKey)
        return builder
    }
    
    public func withDisableReplication(_ disable: Bool) -> MessageBuilder<T> {
        var builder = self
        builder.metadata = metadata.withDisableReplication(disable)
        return builder
    }
    
    public func withTransaction(_ transaction: Transaction) -> MessageBuilder<T> {
        var builder = self
        builder.transaction = transaction
        return builder
    }
    
    public func send() async throws -> MessageId {
        guard let value = value else {
            throw PulsarClientError.invalidConfiguration("Message value not set")
        }
        
        return try await producer.send(value, metadata: metadata)
    }

    return try await producer.send(value, metadata: metadata)
  }
}

/// Extension to ProducerProtocol to provide message builder
public extension ProducerProtocol {
    /// Create a new message builder
    func newMessage() -> MessageBuilder<MessageType> {
        return MessageBuilder(producer: self)
    }
    
    /// Create a new message builder with a transaction
    func newMessage(transaction: Transaction) -> MessageBuilder<MessageType> {
        return MessageBuilder(producer: self).withTransaction(transaction)
    }
}