import Foundation

/// Configuration for Dead Letter Queue (DLQ) policy
public struct DeadLetterPolicy: Sendable {
    /// Maximum number of times a message can be redelivered before being sent to DLQ
    public let maxRedeliverCount: Int
    
    /// Optional custom topic name for the dead letter queue
    /// If not specified, defaults to "{original-topic}-{subscription}-DLQ"
    public let deadLetterTopic: String?
    
    /// Optional retry topic for messages before they go to DLQ
    public let retryLetterTopic: String?
    
    /// Initial subscription name for the DLQ topic
    public let initialSubscriptionName: String?
    
    public init(
        maxRedeliverCount: Int,
        deadLetterTopic: String? = nil,
        retryLetterTopic: String? = nil,
        initialSubscriptionName: String? = nil
    ) {
        self.maxRedeliverCount = maxRedeliverCount
        self.deadLetterTopic = deadLetterTopic
        self.retryLetterTopic = retryLetterTopic
        self.initialSubscriptionName = initialSubscriptionName
    }
}

/// Builder for DeadLetterPolicy
public actor DeadLetterPolicyBuilder {
    private var maxRedeliverCount: Int = 3
    private var deadLetterTopic: String?
    private var retryLetterTopic: String?
    private var initialSubscriptionName: String?
    
    public init() {}
    
    /// Set the maximum redelivery count
    public func maxRedeliverCount(_ count: Int) -> DeadLetterPolicyBuilder {
        self.maxRedeliverCount = count
        return self
    }
    
    /// Set a custom dead letter topic name
    public func deadLetterTopic(_ topic: String) -> DeadLetterPolicyBuilder {
        self.deadLetterTopic = topic
        return self
    }
    
    /// Set a retry topic name
    public func retryLetterTopic(_ topic: String) -> DeadLetterPolicyBuilder {
        self.retryLetterTopic = topic
        return self
    }
    
    /// Set the initial subscription name for DLQ topic
    public func initialSubscriptionName(_ name: String) -> DeadLetterPolicyBuilder {
        self.initialSubscriptionName = name
        return self
    }
    
    /// Build the DeadLetterPolicy
    public func build() -> DeadLetterPolicy {
        return DeadLetterPolicy(
            maxRedeliverCount: maxRedeliverCount,
            deadLetterTopic: deadLetterTopic,
            retryLetterTopic: retryLetterTopic,
            initialSubscriptionName: initialSubscriptionName
        )
    }
}

/// Extension to create a builder
public extension DeadLetterPolicy {
    static func builder() -> DeadLetterPolicyBuilder {
        return DeadLetterPolicyBuilder()
    }
}