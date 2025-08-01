import Foundation
import Logging

/// Actor-based handler for Dead Letter Queue operations
actor DeadLetterQueueHandler<T: Sendable> {
    private let logger = Logger(label: "DeadLetterQueueHandler")
    private let policy: DeadLetterPolicy
    private let originalTopic: String
    private let subscriptionName: String
    private let client: PulsarClient
    private let schema: Schema<T>
    
    // Track redelivery counts for messages
    private var redeliveryCount: [MessageId: Int] = [:]
    
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
    
    /// Check if a message should be sent to DLQ
    func shouldSendToDLQ(messageId: MessageId) -> Bool {
        let count = redeliveryCount[messageId, default: 0] + 1
        redeliveryCount[messageId] = count
        
        if count >= policy.maxRedeliverCount {
            logger.info("Message exceeded max redelivery count", metadata: [
                "messageId": "\(messageId)",
                "redeliveryCount": "\(count)",
                "maxRedeliverCount": "\(policy.maxRedeliverCount)"
            ])
            return true
        }
        
        return false
    }
    
    /// Send a message to the Dead Letter Queue
    func sendToDLQ(_ message: Message<T>) async throws {
        let dlqProducer = try await getDLQProducer()
        
        logger.info("Sending message to DLQ", metadata: [
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
            .withProperty("REDELIVERY_COUNT", String(redeliveryCount[message.id] ?? 0))
        
        // Copy key if present
        if let key = message.key {
            dlqMetadata = dlqMetadata.withKey(key)
        }
        
        // Send to DLQ
        let dlqMessageId = try await dlqProducer.send(message.value, metadata: dlqMetadata)
        
        logger.info("Message sent to DLQ successfully", metadata: [
            "originalMessageId": "\(message.id)",
            "dlqMessageId": "\(dlqMessageId)"
        ])
        
        // Clean up redelivery tracking
        redeliveryCount.removeValue(forKey: message.id)
    }
    
    /// Send a message to the Retry Topic (if configured)
    func sendToRetryTopic(_ message: Message<T>) async throws {
        guard policy.retryLetterTopic != nil else {
            // No retry topic configured, send directly to DLQ
            try await sendToDLQ(message)
            return
        }
        
        let retryProducer = try await getRetryProducer()
        
        logger.info("Sending message to retry topic", metadata: [
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
            .withProperty("RETRY_COUNT", String(redeliveryCount[message.id] ?? 0))
        
        // Copy key if present
        if let key = message.key {
            retryMetadata = retryMetadata.withKey(key)
        }
        
        // Set a delay for retry (exponential backoff)
        let retryCount = redeliveryCount[message.id] ?? 0
        let delaySeconds = min(pow(2.0, Double(retryCount - 1)), 3600.0) // Max 1 hour
        retryMetadata = retryMetadata.withDeliverAfter(delaySeconds)
        
        // Send to retry topic
        let retryMessageId = try await retryProducer.send(message.value, metadata: retryMetadata)
        
        logger.info("Message sent to retry topic successfully", metadata: [
            "originalMessageId": "\(message.id)",
            "retryMessageId": "\(retryMessageId)",
            "delaySeconds": "\(delaySeconds)"
        ])
    }
    
    /// Clean up old redelivery count entries (to prevent memory leak)
    func cleanupOldEntries() {
        let cutoffCount = 10000
        if redeliveryCount.count > cutoffCount {
            // Remove oldest entries
            let toRemove = redeliveryCount.count - cutoffCount
            let sortedKeys = redeliveryCount.keys.sorted { $0.description < $1.description }
            for i in 0..<toRemove {
                redeliveryCount.removeValue(forKey: sortedKeys[i])
            }
            
            logger.debug("Cleaned up old redelivery count entries", metadata: [
                "removed": "\(toRemove)",
                "remaining": "\(redeliveryCount.count)"
            ])
        }
    }
    
    /// Reset redelivery count for a message (e.g., after successful processing)
    func resetRedeliveryCount(for messageId: MessageId) {
        redeliveryCount.removeValue(forKey: messageId)
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
        
        logger.info("Created DLQ producer", metadata: [
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
        
        logger.info("Created retry producer", metadata: [
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
        redeliveryCount.removeAll()
    }
}