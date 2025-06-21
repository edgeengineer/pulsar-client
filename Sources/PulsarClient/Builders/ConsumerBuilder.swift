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

/// Builder for creating consumers
@dynamicMemberLookup
public final class ConsumerBuilder<T> where T: Sendable {
    internal var options: ConsumerOptions<T>
    
    public init(options: ConsumerOptions<T>) {
        self.options = options
    }
    
    @discardableResult
    public func consumerName(_ name: String?) -> Self {
        options = options.withConsumerName(name)
        return self
    }
    
    @discardableResult
    public func subscriptionName(_ name: String) -> Self {
        // Update the subscription name by creating new options
        options = ConsumerOptions(subscriptionName: name, topic: options.topic, schema: options.schema)
            .withConsumerName(options.consumerName)
            .withInitialPosition(options.initialPosition)
            .withMessagePrefetchCount(options.messagePrefetchCount)
            .withPriorityLevel(options.priorityLevel)
            .withReadCompacted(options.readCompacted)
            .withRegexSubscriptionMode(options.regexSubscriptionMode)
            .withReplicateSubscriptionState(options.replicateSubscriptionState)
            .withSubscriptionType(options.subscriptionType)
            .withAllowOutOfOrderDelivery(options.allowOutOfOrderDelivery)
            .withAckTimeout(options.ackTimeout)
            .withNegativeAckRedeliveryDelay(options.negativeAckRedeliveryDelay)
            .withSubscriptionProperties(options.subscriptionProperties)
        if let handler = options.stateChangedHandler {
            options = options.withStateChangedHandler(handler)
        }
        return self
    }
    
    @discardableResult
    public func initialPosition(_ position: SubscriptionInitialPosition) -> Self {
        options = options.withInitialPosition(position)
        return self
    }
    
    @discardableResult
    public func messagePrefetchCount(_ count: UInt) -> Self {
        options = options.withMessagePrefetchCount(count)
        return self
    }
    
    @discardableResult
    public func priorityLevel(_ level: Int) -> Self {
        options = options.withPriorityLevel(level)
        return self
    }
    
    @discardableResult
    public func readCompacted(_ enabled: Bool) -> Self {
        options = options.withReadCompacted(enabled)
        return self
    }
    
    @discardableResult
    public func regexSubscriptionMode(_ mode: RegexSubscriptionMode) -> Self {
        options = options.withRegexSubscriptionMode(mode)
        return self
    }
    
    @discardableResult
    public func replicateSubscriptionState(_ enabled: Bool) -> Self {
        options = options.withReplicateSubscriptionState(enabled)
        return self
    }
    
    @discardableResult
    public func subscriptionType(_ type: SubscriptionType) -> Self {
        options = options.withSubscriptionType(type)
        return self
    }
    
    @discardableResult
    public func allowOutOfOrderDelivery(_ enabled: Bool) -> Self {
        options = options.withAllowOutOfOrderDelivery(enabled)
        return self
    }
    
    @discardableResult
    public func ackTimeout(_ timeout: TimeInterval) -> Self {
        options = options.withAckTimeout(timeout)
        return self
    }
    
    @discardableResult
    public func negativeAckRedeliveryDelay(_ delay: TimeInterval) -> Self {
        options = options.withNegativeAckRedeliveryDelay(delay)
        return self
    }
    
    @discardableResult
    public func maxTotalReceiverQueueSizeAcrossPartitions(_ size: Int) -> Self {
        options = options.withMaxTotalReceiverQueueSizeAcrossPartitions(size)
        return self
    }
    
    @discardableResult
    public func autoAcknowledgeOldestChunkedMessageOnQueueFull(_ enabled: Bool) -> Self {
        options = options.withAutoAcknowledgeOldestChunkedMessageOnQueueFull(enabled)
        return self
    }
    
    @discardableResult
    public func expireTimeOfIncompleteChunkedMessage(_ time: TimeInterval) -> Self {
        options = options.withExpireTimeOfIncompleteChunkedMessage(time)
        return self
    }
    
    @discardableResult
    public func cryptoKeyReader(_ reader: CryptoKeyReader?) -> Self {
        options = options.withCryptoKeyReader(reader)
        return self
    }
    
    @discardableResult
    public func patternAutoDiscoveryPeriod(_ period: TimeInterval) -> Self {
        options = options.withPatternAutoDiscoveryPeriod(period)
        return self
    }
    
    @discardableResult
    public func maxPendingChunkedMessage(_ count: Int) -> Self {
        options = options.withMaxPendingChunkedMessage(count)
        return self
    }
    
    @discardableResult
    public func subscriptionProperties(_ properties: [String: String]) -> Self {
        options = options.withSubscriptionProperties(properties)
        return self
    }
    
    @discardableResult
    public func stateChangedHandler(_ handler: @escaping @Sendable (ConsumerStateChanged<T>) -> Void) -> Self {
        options = options.withStateChangedHandler(handler)
        return self
    }
    
    // Dynamic member lookup for accessing options properties
    public subscript<Value>(dynamicMember keyPath: KeyPath<ConsumerOptions<T>, Value>) -> Value {
        return options[keyPath: keyPath]
    }
}