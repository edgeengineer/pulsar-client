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

#if canImport(RegexBuilder)
  import RegexBuilder
#endif

/// The consumer building options.
public struct ConsumerOptions<TMessage>: Sendable where TMessage: Sendable {    
    // MARK: - Default Values
    
    /// The default initial position.
    public static var defaultInitialPosition: SubscriptionInitialPosition { .latest }
    
    /// The default message prefetch count.
    public static var defaultMessagePrefetchCount: UInt { 1000 }
    
    /// The default priority level.
    public static var defaultPriorityLevel: Int { 0 }
    
    /// The default of whether to read compacted.
    public static var defaultReadCompacted: Bool { false }
    
    /// The default regex subscription mode.
    public static var defaultRegexSubscriptionMode: RegexSubscriptionMode { .persistent }
    
    /// The default of whether to replicate the subscription's state.
    public static var defaultReplicateSubscriptionState: Bool { false }
    
    /// The default subscription type.
    public static var defaultSubscriptionType: SubscriptionType { .exclusive }
    
    /// The default of whether to allow out-of-order delivery on key_shared subscriptions.
    public static var defaultAllowOutOfOrderDelivery: Bool { false }
    
    // MARK: - Required Properties
    
    /// Set the subscription name for this consumer. This is required.
    public let subscriptionName: String
    
    /// Set the schema. This is required.
    public let schema: Schema<TMessage>
    
    /// Set the topic for this consumer. This, or setting multiple topics or a topic pattern, is required.
    public let topic: String
    
    /// Topics for multi-topic subscription
    public let topics: [String]
    
    /// Specify a pattern for topics that this consumer subscribes to.
    public let topicsPattern: String?
    
    // MARK: - Optional Properties
    
    /// Set the consumer name. This is optional.
    public var consumerName: String?
    
    /// Set initial position for the subscription. The default is 'Latest'.
    public var initialPosition: SubscriptionInitialPosition
    
    /// Number of messages that will be prefetched. The default is 1000.
    public var messagePrefetchCount: UInt
    
    /// Set the priority level for the shared subscription consumer. The default is 0.
    public var priorityLevel: Int
    
    /// Whether to read from the compacted topic. The default is 'false'.
    public var readCompacted: Bool
    
    /// Determines which topics this consumer should be subscribed to - Persistent, Non-Persistent, or both. The default is 'Persistent'.
    public var regexSubscriptionMode: RegexSubscriptionMode
    
    /// Whether to replicate the subscription's state across clusters (when using geo-replication). The default is 'false'.
    public var replicateSubscriptionState: Bool
    
    /// Register a state changed handler. This is optional.
    public var stateChangedHandler: (@Sendable (ConsumerStateChanged<TMessage>) -> Void)?
    
    /// Add/Set the subscription's properties. This is optional.
    public var subscriptionProperties: [String: String]
    
    /// Set the subscription type for this consumer. The default is 'Exclusive'.
    public var subscriptionType: SubscriptionType
    
    /// Allow out-of-order delivery on key_shared subscriptions. The default is 'false'.
    /// - Note: https://pulsar.apache.org/docs/3.3.x/concepts-messaging/#preserving-order-of-processing
    public var allowOutOfOrderDelivery: Bool
    
    // MARK: - Additional Swift-specific Properties
    
    /// Acknowledgment timeout in seconds. Default is 30.0.
    public var ackTimeout: TimeInterval
    
    /// Negative acknowledgment redelivery delay in seconds. Default is 60.0.
    public var negativeAckRedeliveryDelay: TimeInterval
    
    /// Maximum total receiver queue size across partitions. Default is 50000.
    public var maxTotalReceiverQueueSizeAcrossPartitions: Int
    
    /// Auto acknowledge oldest chunked message on queue full. Default is false.
    public var autoAcknowledgeOldestChunkedMessageOnQueueFull: Bool
    
    /// Expire time of incomplete chunked message in seconds. Default is 60.0.
    public var expireTimeOfIncompleteChunkedMessage: TimeInterval
    
    /// Crypto key reader for message decryption.
    public var cryptoKeyReader: CryptoKeyReader?
    
    /// Pattern auto discovery period in seconds. Default is 60.0.
    public var patternAutoDiscoveryPeriod: TimeInterval
    
    /// Maximum pending chunked messages. Default is 10.
    public var maxPendingChunkedMessage: Int
    
    /// Receiver queue size. Default is 1000.
    public var receiverQueueSize: Int
    
    /// Dead letter policy for handling failed messages.
    public var deadLetterPolicy: DeadLetterPolicy?
    
    /// Message interceptors for the consumer.
    public var interceptors: [any ConsumerInterceptor<TMessage>]
    
    // MARK: - Initialization
    
    /// Initializes a new instance using the specified subscription name, topic and schema.
    public init(subscriptionName: String, topic: String, schema: Schema<TMessage>) {
        self.subscriptionName = subscriptionName
        self.topic = topic
        self.topics = []
        self.topicsPattern = nil
        self.schema = schema
        self.consumerName = nil
        self.initialPosition = Self.defaultInitialPosition
        self.messagePrefetchCount = Self.defaultMessagePrefetchCount
        self.priorityLevel = Self.defaultPriorityLevel
        self.readCompacted = Self.defaultReadCompacted
        self.regexSubscriptionMode = Self.defaultRegexSubscriptionMode
        self.replicateSubscriptionState = Self.defaultReplicateSubscriptionState
        self.subscriptionType = Self.defaultSubscriptionType
        self.allowOutOfOrderDelivery = Self.defaultAllowOutOfOrderDelivery
        self.subscriptionProperties = [:]
        self.stateChangedHandler = nil
        self.ackTimeout = 30.0
        self.negativeAckRedeliveryDelay = 60.0
        self.maxTotalReceiverQueueSizeAcrossPartitions = 50000
        self.autoAcknowledgeOldestChunkedMessageOnQueueFull = false
        self.expireTimeOfIncompleteChunkedMessage = 60.0
        self.cryptoKeyReader = nil
        self.patternAutoDiscoveryPeriod = 60.0
        self.maxPendingChunkedMessage = 10
        self.receiverQueueSize = 1000
        self.deadLetterPolicy = nil
        self.interceptors = []
    }
    
    /// Initializes a new instance using the specified subscription name, topics and schema.
    public init(subscriptionName: String, topics: [String], schema: Schema<TMessage>) {
        self.subscriptionName = subscriptionName
        self.topic = ""
        self.topics = topics
        self.topicsPattern = nil
        self.schema = schema
        self.consumerName = nil
        self.initialPosition = Self.defaultInitialPosition
        self.messagePrefetchCount = Self.defaultMessagePrefetchCount
        self.priorityLevel = Self.defaultPriorityLevel
        self.readCompacted = Self.defaultReadCompacted
        self.regexSubscriptionMode = Self.defaultRegexSubscriptionMode
        self.replicateSubscriptionState = Self.defaultReplicateSubscriptionState
        self.subscriptionType = Self.defaultSubscriptionType
        self.allowOutOfOrderDelivery = Self.defaultAllowOutOfOrderDelivery
        self.subscriptionProperties = [:]
        self.stateChangedHandler = nil
        self.ackTimeout = 30.0
        self.negativeAckRedeliveryDelay = 60.0
        self.maxTotalReceiverQueueSizeAcrossPartitions = 50000
        self.autoAcknowledgeOldestChunkedMessageOnQueueFull = false
        self.expireTimeOfIncompleteChunkedMessage = 60.0
        self.cryptoKeyReader = nil
        self.patternAutoDiscoveryPeriod = 60.0
        self.maxPendingChunkedMessage = 10
        self.receiverQueueSize = 1000
        self.deadLetterPolicy = nil
        self.interceptors = []
    }
    
    /// Initializes a new instance using the specified subscription name, topics pattern and schema.
    public init(subscriptionName: String, topicsPattern: String, schema: Schema<TMessage>) {
        self.subscriptionName = subscriptionName
        self.topic = ""
        self.topics = []
        self.topicsPattern = topicsPattern
        self.schema = schema
        self.consumerName = nil
        self.initialPosition = Self.defaultInitialPosition
        self.messagePrefetchCount = Self.defaultMessagePrefetchCount
        self.priorityLevel = Self.defaultPriorityLevel
        self.readCompacted = Self.defaultReadCompacted
        self.regexSubscriptionMode = Self.defaultRegexSubscriptionMode
        self.replicateSubscriptionState = Self.defaultReplicateSubscriptionState
        self.subscriptionType = Self.defaultSubscriptionType
        self.allowOutOfOrderDelivery = Self.defaultAllowOutOfOrderDelivery
        self.subscriptionProperties = [:]
        self.stateChangedHandler = nil
        self.ackTimeout = 30.0
        self.negativeAckRedeliveryDelay = 60.0
        self.maxTotalReceiverQueueSizeAcrossPartitions = 50000
        self.autoAcknowledgeOldestChunkedMessageOnQueueFull = false
        self.expireTimeOfIncompleteChunkedMessage = 60.0
        self.cryptoKeyReader = nil
        self.patternAutoDiscoveryPeriod = 60.0
        self.maxPendingChunkedMessage = 10
        self.receiverQueueSize = 1000
        self.deadLetterPolicy = nil
        self.interceptors = []
    }
    
    // MARK: - Validation
    
    /// Validate that the consumer options are properly configured.
    public func validate() throws {
        guard !subscriptionName.isEmpty else {
            throw PulsarClientError.invalidConfiguration("Subscription name cannot be empty")
        }
        
        let hasTopics = !topic.isEmpty || !topics.isEmpty || topicsPattern != nil
        guard hasTopics else {
            throw PulsarClientError.invalidConfiguration("Must specify at least one topic, multiple topics, or a topics pattern")
        }
        
        // Validate that only one topic specification method is used
        let topicMethods = [
            !topic.isEmpty,
            !topics.isEmpty,
            topicsPattern != nil
        ].filter { $0 }.count
        
        guard topicMethods == 1 else {
            throw PulsarClientError.invalidConfiguration("Must specify exactly one of: single topic, multiple topics, or topics pattern")
        }
        
        guard messagePrefetchCount > 0 else {
            throw PulsarClientError.invalidConfiguration("Message prefetch count must be greater than 0")
        }
        
        guard priorityLevel >= 0 else {
            throw PulsarClientError.invalidConfiguration("Priority level must be non-negative")
        }
        
        guard ackTimeout > 0 else {
            throw PulsarClientError.invalidConfiguration("Acknowledgment timeout must be greater than 0")
        }
        
        guard negativeAckRedeliveryDelay > 0 else {
            throw PulsarClientError.invalidConfiguration("Negative acknowledgment redelivery delay must be greater than 0")
        }
        
        guard maxTotalReceiverQueueSizeAcrossPartitions > 0 else {
            throw PulsarClientError.invalidConfiguration("Max total receiver queue size across partitions must be greater than 0")
        }
        
        guard expireTimeOfIncompleteChunkedMessage > 0 else {
            throw PulsarClientError.invalidConfiguration("Expire time of incomplete chunked message must be greater than 0")
        }
        
        guard patternAutoDiscoveryPeriod > 0 else {
            throw PulsarClientError.invalidConfiguration("Pattern auto discovery period must be greater than 0")
        }
        
        guard maxPendingChunkedMessage > 0 else {
            throw PulsarClientError.invalidConfiguration("Max pending chunked message must be greater than 0")
        }
    }
    
    // MARK: - Builder Pattern Methods (Functional Style)
    
    /// Returns a new ConsumerOptions with the consumer name set
    public func withConsumerName(_ name: String?) -> ConsumerOptions {
        var copy = self
        copy.consumerName = name
        return copy
    }
    
    /// Returns a new ConsumerOptions with the initial position set
    public func withInitialPosition(_ position: SubscriptionInitialPosition) -> ConsumerOptions {
        var copy = self
        copy.initialPosition = position
        return copy
    }
    
    /// Returns a new ConsumerOptions with the message prefetch count set
    public func withMessagePrefetchCount(_ count: UInt) -> ConsumerOptions {
        var copy = self
        copy.messagePrefetchCount = count
        return copy
    }
    
    /// Returns a new ConsumerOptions with the priority level set
    public func withPriorityLevel(_ level: Int) -> ConsumerOptions {
        var copy = self
        copy.priorityLevel = level
        return copy
    }
    
    /// Returns a new ConsumerOptions with read compacted setting
    public func withReadCompacted(_ enabled: Bool) -> ConsumerOptions {
        var copy = self
        copy.readCompacted = enabled
        return copy
    }
    
    /// Returns a new ConsumerOptions with regex subscription mode set
    public func withRegexSubscriptionMode(_ mode: RegexSubscriptionMode) -> ConsumerOptions {
        var copy = self
        copy.regexSubscriptionMode = mode
        return copy
    }
    
    /// Returns a new ConsumerOptions with replicate subscription state setting
    public func withReplicateSubscriptionState(_ enabled: Bool) -> ConsumerOptions {
        var copy = self
        copy.replicateSubscriptionState = enabled
        return copy
    }
    
    /// Returns a new ConsumerOptions with state changed handler set
    public func withStateChangedHandler(_ handler: @escaping @Sendable (ConsumerStateChanged<TMessage>) -> Void) -> ConsumerOptions {
        var copy = self
        copy.stateChangedHandler = handler
        return copy
    }
    
    /// Returns a new ConsumerOptions with subscription type set
    public func withSubscriptionType(_ type: SubscriptionType) -> ConsumerOptions {
        var copy = self
        copy.subscriptionType = type
        return copy
    }
    
    /// Returns a new ConsumerOptions with allow out-of-order delivery setting
    public func withAllowOutOfOrderDelivery(_ enabled: Bool) -> ConsumerOptions {
        var copy = self
        copy.allowOutOfOrderDelivery = enabled
        return copy
    }
    
    /// Returns a new ConsumerOptions with ack timeout set
    public func withAckTimeout(_ timeout: TimeInterval) -> ConsumerOptions {
        var copy = self
        copy.ackTimeout = timeout
        return copy
    }
    
    /// Returns a new ConsumerOptions with dead letter policy set
    public func withDeadLetterPolicy(_ policy: DeadLetterPolicy?) -> ConsumerOptions {
        var copy = self
        copy.deadLetterPolicy = policy
        return copy
    }
    
    /// Returns a new ConsumerOptions with interceptors set
    public func withInterceptors(_ interceptors: [any ConsumerInterceptor<TMessage>]) -> ConsumerOptions {
        var copy = self
        copy.interceptors = interceptors
        return copy
    }
    
    /// Returns a new ConsumerOptions with an interceptor added
    public func withInterceptor(_ interceptor: any ConsumerInterceptor<TMessage>) -> ConsumerOptions {
        var copy = self
        copy.interceptors.append(interceptor)
        return copy
    }

  
  /// Returns a new ConsumerOptions with negative ack redelivery delay set
  public func withNegativeAckRedeliveryDelay(_ delay: TimeInterval) -> ConsumerOptions {
    var copy = self
    copy.negativeAckRedeliveryDelay = delay
    return copy
  }

  /// Returns a new ConsumerOptions with crypto key reader set
  public func withCryptoKeyReader(_ reader: CryptoKeyReader?) -> ConsumerOptions {
    var copy = self
    copy.cryptoKeyReader = reader
    return copy
  }

  /// Returns a new ConsumerOptions with a subscription property added
  public func withSubscriptionProperty(_ key: String, _ value: String) -> ConsumerOptions {
    var copy = self
    copy.subscriptionProperties[key] = value
    return copy
  }

  /// Returns a new ConsumerOptions with all subscription properties replaced
  public func withSubscriptionProperties(_ properties: [String: String]) -> ConsumerOptions {
    var copy = self
    copy.subscriptionProperties = properties
    return copy
  }

  /// Returns a new ConsumerOptions with max total receiver queue size set
  public func withMaxTotalReceiverQueueSizeAcrossPartitions(_ size: Int) -> ConsumerOptions {
    var copy = self
    copy.maxTotalReceiverQueueSizeAcrossPartitions = size
    return copy
  }

  /// Returns a new ConsumerOptions with auto acknowledge oldest chunked message setting
  public func withAutoAcknowledgeOldestChunkedMessageOnQueueFull(_ enabled: Bool) -> ConsumerOptions
  {
    var copy = self
    copy.autoAcknowledgeOldestChunkedMessageOnQueueFull = enabled
    return copy
  }

  /// Returns a new ConsumerOptions with expire time of incomplete chunked message set
  public func withExpireTimeOfIncompleteChunkedMessage(_ time: TimeInterval) -> ConsumerOptions {
    var copy = self
    copy.expireTimeOfIncompleteChunkedMessage = time
    return copy
  }

  /// Returns a new ConsumerOptions with pattern auto discovery period set
  public func withPatternAutoDiscoveryPeriod(_ period: TimeInterval) -> ConsumerOptions {
    var copy = self
    copy.patternAutoDiscoveryPeriod = period
    return copy
  }

  /// Returns a new ConsumerOptions with max pending chunked message set
  public func withMaxPendingChunkedMessage(_ count: Int) -> ConsumerOptions {
    var copy = self
    copy.maxPendingChunkedMessage = count
    return copy
  }

  /// Returns a new ConsumerOptions with receiver queue size set
  public func withReceiverQueueSize(_ size: Int) -> ConsumerOptions {
    var copy = self
    copy.receiverQueueSize = size
    return copy
  }
}
