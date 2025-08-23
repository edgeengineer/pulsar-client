import Foundation
import Logging

/// Actor-based handler for Dead Letter Queue operations
actor DeadLetterQueueHandler<T: Sendable> {
    /// Action to take after negative acknowledgment
    enum NegativeAckAction {
        case none
        case retry
        case dlq
    }
    private let logger = Logger(label: "DeadLetterQueueHandler")
    private let policy: DeadLetterPolicy
    private let originalTopic: String
    private let subscriptionName: String
    private let client: PulsarClient
    private let schema: Schema<T>
    
    // Track negative acknowledgment counts for messages
    private var negativeAckCounts: [MessageId: Int] = [:]
    
    // DLQ and Retry producers
    private var dlqProducer: (any ProducerProtocol<T>)?
    private var retryProducer: (any ProducerProtocol<T>)?
    
    init(
        policy: DeadLetterPolicy,
        originalTopic: String,
        subscriptionName: String,
        client: PulsarClient,
        schema: Schema<T>
    ) {
        self.policy = policy
        self.originalTopic = originalTopic
        self.subscriptionName = subscriptionName
        self.client = client
        self.schema = schema
    }
    
    /// Track a negative acknowledgment and determine action
    func trackNegativeAck(message: Message<T>) -> NegativeAckAction {
        // Track how many times we've negatively acknowledged this specific message
        let currentNegAckCount = negativeAckCounts[message.id, default: 0]
        let newNegAckCount = currentNegAckCount + 1
        negativeAckCounts[message.id] = newNegAckCount
        
        // Use the greater of broker's redeliveryCount or our tracked negative acks
        // This handles cases where the broker doesn't increment redeliveryCount for explicit negative acks
        let effectiveRedeliveryCount = max(Int(message.redeliveryCount), currentNegAckCount)
        
        // After this negative ack, the effective count will be one more
        let nextEffectiveCount = effectiveRedeliveryCount + 1
        
        if nextEffectiveCount >= policy.maxRedeliverCount {
            logger.debug("Message will reach max redelivery count after this negative ack", metadata: [
                "messageId": "\(message.id)",
                "brokerRedeliveryCount": "\(message.redeliveryCount)",
                "negativeAckCount": "\(newNegAckCount)",
                "effectiveCount": "\(nextEffectiveCount)",
                "maxRedeliverCount": "\(policy.maxRedeliverCount)"
            ])
            return .dlq
        }
        
        // If we have a retry topic configured and haven't reached max redeliveries, send to retry
        if policy.retryLetterTopic != nil {
            logger.debug("Sending message to retry topic", metadata: [
                "messageId": "\(message.id)",
                "negativeAckCount": "\(newNegAckCount)",
                "effectiveCount": "\(effectiveRedeliveryCount)"
            ])
            return .retry
        }
        
        logger.debug("Tracking negative ack", metadata: [
            "messageId": "\(message.id)",
            "negativeAckCount": "\(newNegAckCount)",
            "effectiveCount": "\(effectiveRedeliveryCount)"
        ])
        
        return .none
    }
    
    /// Send a message to the Dead Letter Queue
    func sendToDLQ(_ message: Message<T>) async throws {
        let dlqProducer = try await getDLQProducer()
        
        logger.debug("Sending message to DLQ", metadata: [
            "originalMessageId": "\(message.id)",
            "dlqTopic": "\(dlqProducer.topic)"
        ])
        
        // Create metadata for DLQ message
        var dlqMetadata = MessageMetadata()
        
        // Copy original message properties
        for (key, value) in message.properties {
            dlqMetadata = dlqMetadata.withProperty(key, value)
        }
        
        // Add DLQ-specific properties
        dlqMetadata = dlqMetadata
            .withProperty("ORIGINAL_TOPIC", originalTopic)
            .withProperty("ORIGINAL_SUBSCRIPTION", subscriptionName)
            .withProperty("ORIGINAL_MESSAGE_ID", message.id.description)
            .withProperty("DLQ_TIMESTAMP", ISO8601DateFormatter().string(from: Date()))
            .withProperty("REDELIVERY_COUNT", String(message.redeliveryCount))
        
        // Copy key if present
        if let key = message.key {
            dlqMetadata = dlqMetadata.withKey(key)
        }
        
        // Send to DLQ
        let dlqMessageId = try await dlqProducer.send(message.value, metadata: dlqMetadata)
        
        logger.debug("Message sent to DLQ successfully", metadata: [
            "originalMessageId": "\(message.id)",
            "dlqMessageId": "\(dlqMessageId)"
        ])
        
        // Clean up tracking
        negativeAckCounts.removeValue(forKey: message.id)
    }
    
    /// Send a message to the Retry Topic (if configured)
    func sendToRetryTopic(_ message: Message<T>) async throws {
        guard policy.retryLetterTopic != nil else {
            // No retry topic configured, send directly to DLQ
            try await sendToDLQ(message)
            return
        }
        
        let retryProducer = try await getRetryProducer()
        
        logger.debug("Sending message to retry topic", metadata: [
            "originalMessageId": "\(message.id)",
            "retryTopic": "\(retryProducer.topic)"
        ])
        
        // Create metadata for retry message
        var retryMetadata = MessageMetadata()
        
        // Copy original message properties
        for (key, value) in message.properties {
            retryMetadata = retryMetadata.withProperty(key, value)
        }
        
        // Add retry-specific properties
        retryMetadata = retryMetadata
            .withProperty("ORIGINAL_TOPIC", originalTopic)
            .withProperty("ORIGINAL_SUBSCRIPTION", subscriptionName)
            .withProperty("ORIGINAL_MESSAGE_ID", message.id.description)
            .withProperty("RETRY_TIMESTAMP", ISO8601DateFormatter().string(from: Date()))
            .withProperty("RETRY_COUNT", String(message.redeliveryCount))
        
        // Copy key if present
        if let key = message.key {
            retryMetadata = retryMetadata.withKey(key)
        }
        
        // Set a delay for retry (exponential backoff)
        let retryCount = Int(message.redeliveryCount)
        let delaySeconds = min(pow(2.0, Double(max(retryCount - 1, 0))), 3600.0) // Max 1 hour
        retryMetadata = retryMetadata.withDeliverAfter(delaySeconds)
        
        // Send to retry topic
        let retryMessageId = try await retryProducer.send(message.value, metadata: retryMetadata)
        
        logger.debug("Message sent to retry topic successfully", metadata: [
            "originalMessageId": "\(message.id)",
            "retryMessageId": "\(retryMessageId)",
            "delaySeconds": "\(delaySeconds)"
        ])
    }
    
    /// Clean up old tracked message entries (to prevent memory leak)
    func cleanupOldEntries() {
        let cutoffCount = 10000
        if negativeAckCounts.count > cutoffCount {
            // Remove oldest entries
            let toRemove = negativeAckCounts.count - cutoffCount
            let sortedIds = negativeAckCounts.keys.sorted { $0.description < $1.description }
            for i in 0..<toRemove {
                negativeAckCounts.removeValue(forKey: sortedIds[i])
            }
            
            logger.debug("Cleaned up old tracked entries", metadata: [
                "removed": "\(toRemove)",
                "remaining": "\(negativeAckCounts.count)"
            ])
        }
    }
    
    /// Reset tracking for a message (e.g., after successful processing)
    func resetRedeliveryCount(for messageId: MessageId) {
        negativeAckCounts.removeValue(forKey: messageId)
    }
    
    private func getDLQProducer() async throws -> any ProducerProtocol<T> {
        if let producer = dlqProducer {
            return producer
        }
        
        let dlqTopic = policy.deadLetterTopic ?? "\(originalTopic)-\(subscriptionName)-DLQ"
        
        let producerName = "dlq-producer-\(subscriptionName)"
        dlqProducer = try await client.newProducer(
            topic: dlqTopic,
            schema: schema
        ) { builder in
            builder.producerName(producerName)
        }
        
        logger.debug("Created DLQ producer", metadata: [
            "topic": "\(dlqTopic)"
        ])
        
        return dlqProducer!
    }
    
    private func getRetryProducer() async throws -> any ProducerProtocol<T> {
        if let producer = retryProducer {
            return producer
        }
        
        guard let retryTopic = policy.retryLetterTopic else {
            throw PulsarClientError.invalidConfiguration("Retry topic not configured")
        }
        
        let producerName = "retry-producer-\(subscriptionName)"
        retryProducer = try await client.newProducer(
            topic: retryTopic,
            schema: schema
        ) { builder in
            builder.producerName(producerName)
        }
        
        logger.debug("Created retry producer", metadata: [
            "topic": "\(retryTopic)"
        ])
        
        return retryProducer!
    }
    
    /// Dispose of DLQ handler resources
    func dispose() async {
        if let producer = dlqProducer {
            await producer.dispose()
        }
        if let producer = retryProducer {
            await producer.dispose()
        }
        negativeAckCounts.removeAll()
    }
}