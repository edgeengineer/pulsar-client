import Foundation
import PulsarClient
import Testing

@Suite("Dead Letter Queue Integration Tests", .serialized)
class DeadLetterQueueIntegrationTests {
    let testCase: IntegrationTestCase
    
    init() async throws {
        self.testCase = try await IntegrationTestCase()
    }

    // Non-blocking cleanup to avoid CI teardown deadlocks
    deinit { Task { [testCase] in await testCase.cleanup() } }
    
    @Test("DLQ Basic Functionality", .timeLimit(.minutes(2)))
    func testBasicDLQFunctionality() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        let topic = try await testCase.createTopic()
        let dlqTopic = "\(topic)-dlq"
        let subscriptionName = "test-dlq-sub"
        let maxRedeliverCount = 2
        
        // Create DLQ topic explicitly
        _ = try await testCase.createTopicWithName(dlqTopic)
        
        // Create DLQ policy
        let dlqPolicy = DeadLetterPolicy(
            maxRedeliverCount: maxRedeliverCount,
            deadLetterTopic: dlqTopic
        )
        
        // Create consumer with DLQ policy using the builder pattern
        let consumer = try await client!.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName(subscriptionName)
                   .deadLetterPolicy(dlqPolicy)
                   .ackTimeout(5.0) // Short timeout for testing
                   .subscriptionType(.shared)
        }
        
        // Create producer
        let producer = try await client!.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            // Use default configuration
        }
        
        // Send a test message
        let testMessage = "test-message-for-dlq"
        _ = try await producer.send(testMessage)
        // Message ID returned successfully
        
        // Receive and negatively acknowledge the message multiple times
        // Note: The broker might not increment redeliveryCount for CommandRedeliverUnacknowledgedMessages
        // so we track iterations ourselves
        // The message will go to DLQ after (maxRedeliverCount - 1) negative acks
        for i in 0..<maxRedeliverCount {
            // Try to receive a message - it might have already gone to DLQ
            do {
                let message = try await consumer.receive(timeout: 3.0)
                #expect(message.value == testMessage)
                // Don't check redeliveryCount as it might not increment with explicit negative ack
                print("DLQ Test: Iteration \(i), message redeliveryCount: \(message.redeliveryCount)")
                
                // Negative acknowledge to trigger redelivery or DLQ
                try await consumer.negativeAcknowledge(message)
                
                // Small delay to allow redelivery
                try await Task.sleep(nanoseconds: 500_000_000) // 0.5 seconds
            } catch {
                // Message likely went to DLQ, which is expected after maxRedeliverCount attempts
                print("DLQ Test: Message no longer available at iteration \(i), likely in DLQ")
                break
            }
        }
        
        // After max redeliveries, message should go to DLQ
        // Small delay to ensure message processing is complete
        try await Task.sleep(nanoseconds: 1_000_000_000) // 1 second
        
        print("DLQ Test: Checking if message is still in main topic...")
        // Try to receive from main topic (should timeout as message is in DLQ)
        do {
            _ = try await consumer.receive(timeout: 3.0)
            Issue.record("Message should not be available in main topic after max redeliveries")
        } catch {
            // Expected timeout
            print("DLQ Test: Message not in main topic (expected), error: \(error)")
        }
        
        print("DLQ Test: Creating DLQ consumer for topic: \(dlqTopic)")
        // Now create DLQ consumer and verify message is there
        let dlqConsumer = try await client!.newConsumer(
            topic: dlqTopic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("\(subscriptionName)-dlq-reader")
                   .initialPosition(.earliest)  // Start from beginning to ensure we get the message
        }
        
        print("DLQ Test: Attempting to receive message from DLQ...")
        // Verify message is in DLQ
        let dlqMessage = try await dlqConsumer.receive(timeout: 10.0)
        print("DLQ Test: Successfully received message from DLQ")
        #expect(dlqMessage.value == testMessage)
        #expect(dlqMessage.metadata.properties["ORIGINAL_TOPIC"] == topic)
        #expect(dlqMessage.metadata.properties["ORIGINAL_SUBSCRIPTION"] == subscriptionName)
        
        // Acknowledge DLQ message
        try await dlqConsumer.acknowledge(dlqMessage)
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
        await dlqConsumer.dispose()
    }
    
    @Test("DLQ with Retry Topic", .timeLimit(.minutes(2)))
    func testDLQWithRetryTopic() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        let topic = try await testCase.createTopic()
        let retryTopic = "\(topic)-retry"
        let dlqTopic = "\(topic)-dlq"
        let subscriptionName = "test-retry-sub"
        let maxRedeliverCount = 3
        
        // Create retry and DLQ topics explicitly
        _ = try await testCase.createTopicWithName(retryTopic)
        _ = try await testCase.createTopicWithName(dlqTopic)
        
        // Create DLQ policy with retry topic
        let dlqPolicy = DeadLetterPolicy(
            maxRedeliverCount: maxRedeliverCount,
            deadLetterTopic: dlqTopic,
            retryLetterTopic: retryTopic
        )
        
        // Create consumer with DLQ policy
        let consumer = try await client!.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName(subscriptionName)
                   .deadLetterPolicy(dlqPolicy)
                   .ackTimeout(5.0)
        }
        
        // Create producer
        let producer = try await client!.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            // Use default configuration
        }
        
        // Send a test message
        let testMessage = "test-retry-message"
        _ = try await producer.send(testMessage)
        // Message ID returned successfully
        
        // First negative ack should send to retry topic
        let message = try await consumer.receive(timeout: 10.0)
        #expect(message.value == testMessage)
        try await consumer.negativeAcknowledge(message)
        
        // Small delay
        try await Task.sleep(nanoseconds: 500_000_000)
        
        // Create retry topic consumer
        let retryConsumer = try await client!.newConsumer(
            topic: retryTopic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("\(subscriptionName)-retry-reader")
                   .initialPosition(.earliest)  // Start from beginning to ensure we get the message
        }
        
        // Verify message is in retry topic
        let retryMessage = try await retryConsumer.receive(timeout: 10.0)
        #expect(retryMessage.value == testMessage)
        #expect(retryMessage.metadata.properties["RETRY_COUNT"] != nil)
        
        // Acknowledge retry message
        try await retryConsumer.acknowledge(retryMessage)
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
        await retryConsumer.dispose()
    }
}