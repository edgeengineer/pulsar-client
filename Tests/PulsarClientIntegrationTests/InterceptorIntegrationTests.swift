import Foundation
import PulsarClient
import Testing

@Suite("Interceptor Integration Tests", .serialized)
class InterceptorIntegrationTests {
    let testCase: IntegrationTestCase
    
    init() async throws {
        self.testCase = try await IntegrationTestCase()
    }

    // Non-blocking cleanup to avoid CI teardown deadlocks
    deinit { Task { [testCase] in await testCase.cleanup() } }
    
    // Test Producer Interceptor
    actor TestProducerInterceptor: ProducerInterceptor {
        typealias MessageType = String
        
        var beforeSendCount = 0
        var onAckCount = 0
        var lastAckedMessageId: MessageId?
        
        func beforeSend(producer: any ProducerProtocol<String>, message: Message<String>) async throws -> Message<String> {
            beforeSendCount += 1
            
            // Create new metadata with modified properties
            var modifiedMetadata = message.metadata
            modifiedMetadata.properties["intercepted"] = "true"
            modifiedMetadata.properties["interceptor-timestamp"] = String(Date().timeIntervalSince1970)
            modifiedMetadata.properties["before-send-count"] = String(beforeSendCount)
            
            // Create new message with modified metadata
            let modifiedMessage = Message(
                id: message.id,
                value: message.value,
                metadata: modifiedMetadata,
                publishTime: message.publishTime,
                producerName: message.producerName,
                replicatedFrom: message.replicatedFrom,
                topicName: message.topicName,
                redeliveryCount: message.redeliveryCount,
                data: message.data
            )
            
            return modifiedMessage
        }
        
        func onSendAcknowledgement(
            producer: any ProducerProtocol<String>,
            message: Message<String>,
            messageId: MessageId?,
            error: Error?
        ) async {
            if error == nil {
                onAckCount += 1
                lastAckedMessageId = messageId
            }
        }
    }
    
    // Test Consumer Interceptor
    actor TestConsumerInterceptor: ConsumerInterceptor {
        typealias MessageType = String
        
        var beforeConsumeCount = 0
        var onAckCount = 0
        var onNackCount = 0
        
        func beforeConsume(consumer: any ConsumerProtocol<String>, message: Message<String>) async -> Message<String> {
            beforeConsumeCount += 1
            
            // Create new metadata with modified properties
            var modifiedMetadata = message.metadata
            modifiedMetadata.properties["consumed"] = "true"
            modifiedMetadata.properties["consume-timestamp"] = String(Date().timeIntervalSince1970)
            modifiedMetadata.properties["before-consume-count"] = String(beforeConsumeCount)
            
            // Create new message with modified metadata
            let modifiedMessage = Message(
                id: message.id,
                value: message.value,
                metadata: modifiedMetadata,
                publishTime: message.publishTime,
                producerName: message.producerName,
                replicatedFrom: message.replicatedFrom,
                topicName: message.topicName,
                redeliveryCount: message.redeliveryCount,
                data: message.data
            )
            
            return modifiedMessage
        }
        
        func onAcknowledge(consumer: any ConsumerProtocol<String>, messageId: MessageId, error: Error?) async {
            onAckCount += 1
        }
        
        func onAcknowledgeCumulative(consumer: any ConsumerProtocol<String>, messageId: MessageId, error: Error?) async {
            // Not counting cumulative acks separately in this test
        }
        
        func onNegativeAcksSend(consumer: any ConsumerProtocol<String>, messageIds: Set<MessageId>) async {
            onNackCount += 1
        }
        
        func close() async {
            // No cleanup needed for test
        }
    }
    
    @Test("Producer Interceptor Message Modification", .timeLimit(.minutes(1)))
    func testProducerInterceptor() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        let topic = try await testCase.createTopic()
        let interceptor = TestProducerInterceptor()
        
        // Create producer with interceptor
        let producer = try await client!.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.intercept([interceptor])
        }
        
        // Create consumer to verify messages
        let consumer = try await client!.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("interceptor-test-sub")
        }
        
        // Send messages
        let testMessages = ["message-1", "message-2", "message-3"]
        var sentMessageIds: [MessageId] = []
        
        for message in testMessages {
            let messageId = try await producer.send(message)
            sentMessageIds.append(messageId)
        }
        
        // Verify interceptor was called
        #expect(await interceptor.beforeSendCount == 3)
        #expect(await interceptor.onAckCount == 3)
        #expect(await interceptor.lastAckedMessageId != nil)
        
        // Verify messages contain interceptor properties
        for i in 0..<testMessages.count {
            let receivedMessage = try await consumer.receive(timeout: 10.0)
            
            #expect(receivedMessage.value == testMessages[i])
            #expect(receivedMessage.metadata.properties["intercepted"] == "true")
            #expect(receivedMessage.metadata.properties["interceptor-timestamp"] != nil)
            #expect(receivedMessage.metadata.properties["before-send-count"] == String(i + 1))
            
            try await consumer.acknowledge(receivedMessage)
        }
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
    }
    
    @Test("Consumer Interceptor Message Processing", .timeLimit(.minutes(1)))
    func testConsumerInterceptor() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        let topic = try await testCase.createTopic()
        let interceptor = TestConsumerInterceptor()
        
        // Create producer
        let producer = try await client!.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            // Use default configuration
        }
        
        // Create consumer with interceptor
        let consumer = try await client!.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("consumer-interceptor-sub")
                   .intercept([interceptor])
        }
        
        // Send messages
        let testMessages = ["test-1", "test-2", "test-3"]
        for message in testMessages {
            _ = try await producer.send(message)
        }
        
        // Receive and acknowledge messages
        for i in 0..<testMessages.count {
            let message = try await consumer.receive(timeout: 10.0)
            
            // Verify interceptor modified the message
            #expect(message.metadata.properties["consumed"] == "true")
            #expect(message.metadata.properties["consume-timestamp"] != nil)
            #expect(message.metadata.properties["before-consume-count"] == String(i + 1))
            
            try await consumer.acknowledge(message)
        }
        
        // Verify interceptor counts
        #expect(await interceptor.beforeConsumeCount == 3)
        #expect(await interceptor.onAckCount == 3)
        
        // Test negative acknowledgment
        _ = try await producer.send("nack-test")
        let nackMessage = try await consumer.receive(timeout: 10.0)
        try await consumer.negativeAcknowledge(nackMessage)
        
        #expect(await interceptor.onNackCount == 1)
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
    }
    
    @Test("Multiple Interceptors Chain", .timeLimit(.minutes(1)))
    func testMultipleInterceptors() async throws {
        let client = await testCase.client
        #expect(client != nil)
        
        let topic = try await testCase.createTopic()
        
        // Create multiple interceptors
        let interceptor1 = TestProducerInterceptor()
        let interceptor2 = TestProducerInterceptor()
        
        // Create producer with multiple interceptors
        let producer = try await client!.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.intercept([interceptor1, interceptor2])
        }
        
        // Create consumer
        let consumer = try await client!.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("multi-interceptor-sub")
        }
        
        // Send a message
        let testMessage = "multi-interceptor-test"
        _ = try await producer.send(testMessage)
        
        // Both interceptors should be called
        #expect(await interceptor1.beforeSendCount == 1)
        #expect(await interceptor2.beforeSendCount == 1)
        #expect(await interceptor1.onAckCount == 1)
        #expect(await interceptor2.onAckCount == 1)
        
        // Receive and verify message
        let receivedMessage = try await consumer.receive(timeout: 10.0)
        #expect(receivedMessage.value == testMessage)
        
        // Both interceptors should have modified the message
        #expect(receivedMessage.metadata.properties["intercepted"] == "true")
        
        try await consumer.acknowledge(receivedMessage)
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
    }
}