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

/// The reader building options.
public struct ReaderOptions<TMessage>: Sendable where TMessage: Sendable {
    
    // MARK: - Default Values
    
    /// The default message prefetch count.
    public static var defaultMessagePrefetchCount: UInt { 1000 }
    
    /// The default of whether to read compacted.
    public static var defaultReadCompacted: Bool { false }
    
    /// The default subscription role prefix.
    public static var defaultSubscriptionRolePrefix: String { "Reader" }
    
    // MARK: - Required Properties
    
    /// The initial reader position is set to the specified message id. This is required.
    public let startMessageId: MessageId
    
    /// Set the topic for this reader. This is required.
    public let topic: String
    
    /// Set the schema. This is required.
    public let schema: Schema<TMessage>
    
    // MARK: - Optional Properties
    
    /// Number of messages that will be prefetched. The default is 1000.
    public var messagePrefetchCount: UInt
    
    /// Whether to read from the compacted topic. The default is 'false'.
    public var readCompacted: Bool
    
    /// Set the reader name. This is optional.
    public var readerName: String?
    
    /// Register a state changed handler. This is optional.
    public var stateChangedHandler: (@Sendable (ReaderStateChanged<TMessage>) -> Void)?
    
    /// Set the subscription name for this reader. This is optional.
    public var subscriptionName: String?
    
    /// Set the subscription role prefix for this reader. The default is 'Reader'. This is optional.
    public var subscriptionRolePrefix: String
    
    // MARK: - Additional Swift-specific Properties
    
    /// Receiver queue size. Default is 1000.
    public var receiverQueueSize: Int
    
    /// Crypto key reader for message decryption.
    public var cryptoKeyReader: CryptoKeyReader?
    
    /// Properties for the reader.
    public var properties: [String: String]
    
    // MARK: - Initialization
    
    /// Initializes a new instance using the specified startMessageId, topic and schema.
    public init(startMessageId: MessageId, topic: String, schema: Schema<TMessage>) {
        self.startMessageId = startMessageId
        self.topic = topic
        self.schema = schema
        self.messagePrefetchCount = Self.defaultMessagePrefetchCount
        self.readCompacted = Self.defaultReadCompacted
        self.readerName = nil
        self.stateChangedHandler = nil
        self.subscriptionName = nil
        self.subscriptionRolePrefix = Self.defaultSubscriptionRolePrefix
        self.receiverQueueSize = 1000
        self.cryptoKeyReader = nil
        self.properties = [:]
    }
    
    // MARK: - Validation
    
    /// Validate that the reader options are properly configured.
    public func validate() throws {
        guard !topic.isEmpty else {
            throw PulsarClientError.invalidConfiguration("Topic cannot be empty")
        }
        
        guard messagePrefetchCount > 0 else {
            throw PulsarClientError.invalidConfiguration("Message prefetch count must be greater than 0")
        }
        
        guard receiverQueueSize > 0 else {
            throw PulsarClientError.invalidConfiguration("Receiver queue size must be greater than 0")
        }
    }
    
    // MARK: - Builder Pattern Methods (Functional Style)
    
    /// Returns a new ReaderOptions with the message prefetch count set
    public func withMessagePrefetchCount(_ count: UInt) -> ReaderOptions {
        var copy = self
        copy.messagePrefetchCount = count
        return copy
    }
    
    /// Returns a new ReaderOptions with read compacted setting
    public func withReadCompacted(_ enabled: Bool) -> ReaderOptions {
        var copy = self
        copy.readCompacted = enabled
        return copy
    }
    
    /// Returns a new ReaderOptions with the reader name set
    public func withReaderName(_ name: String?) -> ReaderOptions {
        var copy = self
        copy.readerName = name
        return copy
    }
    
    /// Returns a new ReaderOptions with the state changed handler set
    public func withStateChangedHandler(_ handler: @escaping @Sendable (ReaderStateChanged<TMessage>) -> Void) -> ReaderOptions {
        var copy = self
        copy.stateChangedHandler = handler
        return copy
    }
    
    /// Returns a new ReaderOptions with the subscription name set
    public func withSubscriptionName(_ name: String?) -> ReaderOptions {
        var copy = self
        copy.subscriptionName = name
        return copy
    }
    
    /// Returns a new ReaderOptions with the subscription role prefix set
    public func withSubscriptionRolePrefix(_ prefix: String) -> ReaderOptions {
        var copy = self
        copy.subscriptionRolePrefix = prefix
        return copy
    }
    
    /// Returns a new ReaderOptions with the receiver queue size set
    public func withReceiverQueueSize(_ size: Int) -> ReaderOptions {
        var copy = self
        copy.receiverQueueSize = size
        return copy
    }
    
    /// Returns a new ReaderOptions with crypto key reader set
    public func withCryptoKeyReader(_ reader: CryptoKeyReader?) -> ReaderOptions {
        var copy = self
        copy.cryptoKeyReader = reader
        return copy
    }
    
    /// Returns a new ReaderOptions with a property added
    public func withProperty(_ key: String, _ value: String) -> ReaderOptions {
        var copy = self
        copy.properties[key] = value
        return copy
    }
    
    /// Returns a new ReaderOptions with all properties replaced
    public func withProperties(_ properties: [String: String]) -> ReaderOptions {
        var copy = self
        copy.properties = properties
        return copy
    }
}

