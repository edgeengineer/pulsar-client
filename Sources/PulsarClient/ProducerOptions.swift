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
import Atomics

/// The producer building options.
public struct ProducerOptions<TMessage>: Sendable where TMessage: Sendable {
    
    // MARK: - Default Values
    
    public static var defaultInitialSequenceId: UInt64 { 0 }
    public static var defaultSendTimeout: TimeInterval { 30.0 }
    public static var defaultBatchingEnabled: Bool { true }
    public static var defaultBatchingMaxMessages: UInt { 1000 }
    public static var defaultBatchingMaxDelay: TimeInterval { 0.01 }
    public static var defaultBatchingMaxBytes: UInt { 128 * 1024 }
    public static var defaultCompressionType: CompressionType { .none }
    public static var defaultHashingScheme: HashingScheme { .javaStringHash }
    public static var defaultProducerAccessMode: ProducerAccessMode { .shared }
    public static var defaultAttachTraceInfoToMessages: Bool { false }
    public static var defaultMaxPendingMessages: UInt { 0 }
    
    // MARK: - Required Properties
    
    /// The topic this producer will publish to.
    public var topic: String
    
    /// The schema for the messages.
    public var schema: Schema<TMessage>
    
    // MARK: - Optional Properties
    
    /// The name of the producer.
    public var producerName: String?
    
    /// The initial sequence ID for the producer.
    public var initialSequenceId: UInt64
    
    /// The timeout for sending a message.
    public var sendTimeout: TimeInterval
    
    /// Whether to enable message batching.
    public var batchingEnabled: Bool
    
    /// The maximum number of messages in a batch.
    public var batchingMaxMessages: UInt
    
    /// The maximum delay before sending a batch.
    public var batchingMaxDelay: TimeInterval
    
    /// The maximum size of a batch in bytes.
    public var batchingMaxBytes: UInt
    
    /// The compression type for the messages.
    public var compressionType: CompressionType
    
    /// The hashing scheme for message routing.
    public var hashingScheme: HashingScheme
    
    /// The message router.
    public var messageRouter: MessageRouter
    
    /// The crypto key reader.
    public var cryptoKeyReader: CryptoKeyReader?
    
    /// The encryption keys.
    public var encryptionKeys: [String]
    
    /// The producer access mode.
    public var producerAccessMode: ProducerAccessMode
    
    /// Whether to attach trace info to messages.
    public var attachTraceInfoToMessages: Bool
    
    /// The maximum number of pending messages.
    public var maxPendingMessages: UInt
    
    /// Properties to be sent with the producer.
    public var producerProperties: [String: String]
    
    /// State change handler for the producer.
    public var stateChangedHandler: (@Sendable (ProducerStateChanged<TMessage>) -> Void)?
    
    // MARK: - Initialization
    
    public init(topic: String, schema: Schema<TMessage>) {
        self.topic = topic
        self.schema = schema
        
        self.initialSequenceId = Self.defaultInitialSequenceId
        self.sendTimeout = Self.defaultSendTimeout
        self.batchingEnabled = Self.defaultBatchingEnabled
        self.batchingMaxMessages = Self.defaultBatchingMaxMessages
        self.batchingMaxDelay = Self.defaultBatchingMaxDelay
        self.batchingMaxBytes = Self.defaultBatchingMaxBytes
        self.compressionType = Self.defaultCompressionType
        self.hashingScheme = Self.defaultHashingScheme
        self.messageRouter = RoundRobinMessageRouter()
        self.producerAccessMode = Self.defaultProducerAccessMode
        self.attachTraceInfoToMessages = Self.defaultAttachTraceInfoToMessages
        self.maxPendingMessages = Self.defaultMaxPendingMessages
        self.encryptionKeys = []
        self.producerProperties = [:]
    }
    
    // MARK: - Validation
    
    public func validate() throws {
        guard !topic.isEmpty else {
            throw PulsarClientError.invalidConfiguration("Topic name cannot be empty")
        }
    }
    
    // MARK: - Builder Methods
    
    @discardableResult
    public func withProducerName(_ name: String?) -> Self {
        var copy = self
        copy.producerName = name
        return copy
    }
    
    @discardableResult
    public func withInitialSequenceId(_ id: UInt64) -> Self {
        var copy = self
        copy.initialSequenceId = id
        return copy
    }
    
    @discardableResult
    public func withSendTimeout(_ timeout: TimeInterval) -> Self {
        var copy = self
        copy.sendTimeout = timeout
        return copy
    }
    
    @discardableResult
    public func withBatchingEnabled(_ enabled: Bool) -> Self {
        var copy = self
        copy.batchingEnabled = enabled
        return copy
    }
    
    @discardableResult
    public func withBatchingMaxMessages(_ max: UInt) -> Self {
        var copy = self
        copy.batchingMaxMessages = max
        return copy
    }
    
    @discardableResult
    public func withBatchingMaxDelay(_ delay: TimeInterval) -> Self {
        var copy = self
        copy.batchingMaxDelay = delay
        return copy
    }
    
    @discardableResult
    public func withBatchingMaxBytes(_ bytes: UInt) -> Self {
        var copy = self
        copy.batchingMaxBytes = bytes
        return copy
    }
    
    @discardableResult
    public func withCompressionType(_ type: CompressionType) -> Self {
        var copy = self
        copy.compressionType = type
        return copy
    }
    
    @discardableResult
    public func withHashingScheme(_ scheme: HashingScheme) -> Self {
        var copy = self
        copy.hashingScheme = scheme
        return copy
    }
    
    @discardableResult
    public func withMessageRouter(_ router: MessageRouter) -> Self {
        var copy = self
        copy.messageRouter = router
        return copy
    }
    
    @discardableResult
    public func withCryptoKeyReader(_ reader: CryptoKeyReader?) -> Self {
        var copy = self
        copy.cryptoKeyReader = reader
        return copy
    }
    
    @discardableResult
    public func withEncryptionKeys(_ keys: [String]) -> Self {
        var copy = self
        copy.encryptionKeys = keys
        return copy
    }
    
    @discardableResult
    public func withProducerAccessMode(_ mode: ProducerAccessMode) -> Self {
        var copy = self
        copy.producerAccessMode = mode
        return copy
    }
    
    @discardableResult
    public func withAttachTraceInfoToMessages(_ enabled: Bool) -> Self {
        var copy = self
        copy.attachTraceInfoToMessages = enabled
        return copy
    }
    
    @discardableResult
    public func withMaxPendingMessages(_ max: UInt) -> Self {
        var copy = self
        copy.maxPendingMessages = max
        return copy
    }
    
    @discardableResult
    public func withProducerProperties(_ properties: [String: String]) -> Self {
        var copy = self
        copy.producerProperties = properties
        return copy
    }
    
    @discardableResult
    public func withStateChangedHandler(_ handler: (@Sendable (ProducerStateChanged<TMessage>) -> Void)?) -> Self {
        var copy = self
        copy.stateChangedHandler = handler
        return copy
    }
}


/// Default round-robin message router implementation.
public struct RoundRobinMessageRouter: MessageRouter {
    private let counter = AtomicCounter()
    
    public init() {}
    
    public func choosePartition(messageMetadata: MessageMetadata, numberOfPartitions: Int) -> Int {
        guard numberOfPartitions > 0 else {
            return 0
        }
        let count = counter.increment()
        return Int(count % UInt64(numberOfPartitions))
    }
}

// Thread-safe atomic counter for round-robin routing
final class AtomicCounter: Sendable {
    private let value = ManagedAtomic<UInt64>(0)

    func increment() -> UInt64 {
        return value.wrappingIncrementThenLoad(ordering: .relaxed)
    }
}

// MARK: - String Hashing Extension

private extension String {
    func hash(using scheme: HashingScheme) -> Int32 {
        switch scheme {
        case .javaStringHash:
            return javaStringHash()
        case .murmur3_32Hash:
            // For now, fallback to Java string hash
            // In production, implement proper Murmur3 hash
            return javaStringHash()
        }
    }
    
    private func javaStringHash() -> Int32 {
        var hash: Int32 = 0
        for char in self.utf8 {
            hash = 31 &* hash &+ Int32(char)
        }
        return hash
    }
}