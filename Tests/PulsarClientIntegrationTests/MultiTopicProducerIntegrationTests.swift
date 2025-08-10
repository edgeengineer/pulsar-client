import Foundation
import PulsarClient
import Testing

@Suite("Multi-Topic Producer Integration Tests", .serialized)
struct MultiTopicProducerIntegrationTests {
    let testCase: IntegrationTestCase
    
    init() async throws {
        self.testCase = try await IntegrationTestCase()
    }
    
    @Test("Multi-Topic Producer Basic Functionality", .timeLimit(.minutes(2)))
    func testMultiTopicProducerBasic() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        // Create multiple topics
        let topic1 = try await testCase.createTopic()
        let topic2 = try await testCase.createTopic()
        let topic3 = try await testCase.createTopic()
        let topics = [topic1, topic2, topic3]
        
        // Create multi-topic producer with round-robin router
        // Using the newMultiTopicProducer API
        let producer = try await client!.newMultiTopicProducer(
            topics: topics,
            schema: Schema<String>.string
        ) { builder in
            builder.topicRouter(RoundRobinTopicRouter<String>())
        }
        
        // Create consumers for each topic
        let consumer1 = try await client!.newConsumer(
            topic: topic1,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("multi-topic-sub-1")
        }
        
        let consumer2 = try await client!.newConsumer(
            topic: topic2,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("multi-topic-sub-2")
        }
        
        let consumer3 = try await client!.newConsumer(
            topic: topic3,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("multi-topic-sub-3")
        }
        
        // Send messages - should distribute across topics
        let messages = ["message-1", "message-2", "message-3", "message-4", "message-5", "message-6"]
        
        for message in messages {
            _ = try await producer.send(message)
            // Message ID returned successfully
        }
        
        // Flush to ensure all messages are sent
        try await producer.flush()
        
        // Each topic should receive 2 messages (6 messages / 3 topics with round-robin)
        let receivedFromTopic1 = try await receiveMessages(from: consumer1, count: 2, timeout: 10.0)
        let receivedFromTopic2 = try await receiveMessages(from: consumer2, count: 2, timeout: 10.0)
        let receivedFromTopic3 = try await receiveMessages(from: consumer3, count: 2, timeout: 10.0)
        
        #expect(receivedFromTopic1.count == 2)
        #expect(receivedFromTopic2.count == 2)
        #expect(receivedFromTopic3.count == 2)
        
        // Verify all messages were received
        let allReceived = receivedFromTopic1 + receivedFromTopic2 + receivedFromTopic3
        let allReceivedValues = allReceived.map { $0.value }.sorted()
        #expect(allReceivedValues == messages.sorted())
        
        // Cleanup
        await producer.dispose()
        await consumer1.dispose()
        await consumer2.dispose()
        await consumer3.dispose()
    }
    
    @Test("Multi-Topic Producer with Partition Key Routing", .timeLimit(.minutes(2)))
    func testMultiTopicProducerWithPartitionKey() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        // Create multiple topics
        let topic1 = try await testCase.createTopic()
        let topic2 = try await testCase.createTopic()
        let topics = [topic1, topic2]
        
        // Create multi-topic producer (default router uses partition key if available)
        let producer = try await client!.newMultiTopicProducer(
            topics: topics,
            schema: Schema<String>.string
        ) { _ in
            // Use default configuration
        }
        
        // Create consumers
        let consumer1 = try await client!.newConsumer(
            topic: topic1,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("partition-sub-1")
        }
        
        let consumer2 = try await client!.newConsumer(
            topic: topic2,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("partition-sub-2")
        }
        
        // Send messages with partition keys
        // Messages with same key should go to same topic
        let messagesWithKey1 = ["key1-msg1", "key1-msg2", "key1-msg3"]
        let messagesWithKey2 = ["key2-msg1", "key2-msg2", "key2-msg3"]
        
        for message in messagesWithKey1 {
            var metadata = MessageMetadata()
            metadata.properties["__partition_key__"] = "key1"
            _ = try await producer.send(message, metadata: metadata)
            // Message ID returned successfully
        }
        
        for message in messagesWithKey2 {
            var metadata = MessageMetadata()
            metadata.properties["__partition_key__"] = "key2"
            _ = try await producer.send(message, metadata: metadata)
            // Message ID returned successfully
        }
        
        try await producer.flush()
        
        // Collect all messages from both topics
        var topic1Messages: [String] = []
        var topic2Messages: [String] = []
        
        // Try to receive up to 3 messages from each topic
        for _ in 0..<3 {
            if let msg = try? await consumer1.receive(timeout: 2.0) {
                topic1Messages.append(msg.value)
                try await consumer1.acknowledge(msg)
            }
            
            if let msg = try? await consumer2.receive(timeout: 2.0) {
                topic2Messages.append(msg.value)
                try await consumer2.acknowledge(msg)
            }
        }
        
        // Verify all messages were sent
        let totalReceived = topic1Messages.count + topic2Messages.count
        #expect(totalReceived == 6)
        
        // Cleanup
        await producer.dispose()
        await consumer1.dispose()
        await consumer2.dispose()
    }
    
    // Helper function to receive multiple messages
    private func receiveMessages(
        from consumer: any ConsumerProtocol<String>,
        count: Int,
        timeout: TimeInterval
    ) async throws -> [Message<String>] {
        var messages: [Message<String>] = []
        
        for _ in 0..<count {
            if let message = try? await consumer.receive(timeout: timeout) {
                messages.append(message)
                try await consumer.acknowledge(message)
            }
        }
        
        return messages
    }
}