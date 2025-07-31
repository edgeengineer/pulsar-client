import Testing
import Foundation
@testable import PulsarClient

@Suite("Producer Integration Tests")
class ProducerIntegrationTests {
    let testCase: IntegrationTestCase
    
    init() async throws {
        self.testCase = try await IntegrationTestCase()
    }
    
    // deinit returns before cleanup is complete, causing hanging tests
    // so we use a semaphore to wait for the cleanup to complete
    // replace with "isolated deinit" in Swift 6.2
    deinit {
        let semaphore = DispatchSemaphore(value: 0)
        Task { [testCase] in
            await testCase.cleanup()
            semaphore.signal()
        }
        semaphore.wait()
    }
    
    @Test("Basic Message Send and Receive")
    func testBasicSendReceive() async throws {
        let topic = try await testCase.createTopic()
        guard let client = await testCase.client else {
            throw IntegrationTestError.clientNotInitialized
        }
        
        // Create producer
        let producer = try await client.newStringProducer(topic: topic)
        
        // Create consumer
        let testId = UUID().uuidString.prefix(8)
        let consumer = try await client.newStringConsumer(topic: topic, subscription: "test-sub-\(testId)")
        
        // Send message
        let messageContent = "Hello, Pulsar!"
        try await producer.send(messageContent)
        
        // Receive message
        let receivedMessage = try await consumer.receive(timeout: 5.0)
        
        #expect(receivedMessage.value == messageContent)
        // MessageId is non-optional so test for a valid ledgerId instead
        #expect(receivedMessage.id.ledgerId > 0)
        
        // Acknowledge
        try await consumer.acknowledge(receivedMessage)
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
    }
    
    @Test("Batch Message Send")
    func testBatchSend() async throws {
        let topic = try await testCase.createTopic()
        guard let client = await testCase.client else {
            throw IntegrationTestError.clientNotInitialized
        }
        
        let producer = try await client.newProducer(topic: topic, schema: Schema<String>.string) { builder in
            _ = builder
                .batchingEnabled(true)
                .batchingMaxMessages(10)
                .batchingMaxDelay(0.1)
        }
        
        let testId = UUID().uuidString.prefix(8)
        let consumer = try await client.newStringConsumer(topic: topic, subscription: "batch-sub-\(testId)")
        
        // Send batch of messages
        let messages = (0..<10).map { "Message \($0)" }
        let sendTasks = messages.map { msg in
            Task { try await producer.send(msg) }
        }
        
        let messageIds = try await withThrowingTaskGroup(of: MessageId.self) { group in
            for task in sendTasks {
                group.addTask { try await task.value }
            }
            
            var ids: [MessageId] = []
            for try await id in group {
                ids.append(id)
            }
            return ids
        }
        
        #expect(messageIds.count == 10)
        
        // Receive all messages
        var receivedMessages: [String] = []
        for _ in 0..<10 {
            let msg = try await consumer.receive(timeout: 5.0)
            receivedMessages.append(msg.value)
            try await consumer.acknowledge(msg)
        }
        
        #expect(Set(receivedMessages) == Set(messages))
        
        await producer.dispose()
        await consumer.dispose()
    }
}