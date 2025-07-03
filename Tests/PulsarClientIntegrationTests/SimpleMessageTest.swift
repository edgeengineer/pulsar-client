import Testing
import Foundation
@testable import PulsarClient

@Suite("Simple Message Test")
struct SimpleMessageTest {
    
    @Test("Simple Send and Receive")
    func testSimpleSendReceive() async throws {
        let client = PulsarClient.builder { _ in }
        
        let topic = "persistent://public/default/simple-test-\(UUID().uuidString)"
        
        // Create producer
        let producer = try await client.newStringProducer(topic: topic)
        
        // Create consumer FIRST, before sending messages
        let consumer = try await client.newConsumer(topic: topic, schema: Schema<String>.string) { builder in
            _ = builder
                .subscriptionName("simple-sub")
                .subscriptionType(SubscriptionType.exclusive)
                .initialPosition(SubscriptionInitialPosition.earliest)  // Start from earliest
        }
        
        // Wait a bit for consumer to fully initialize
        try await Task.sleep(nanoseconds: 500_000_000) // 0.5 seconds
        
        // Now send a message
        print("=== Sending message ===")
        let messageId = try await producer.send("Hello World")
        print("Message sent with ID: \(messageId)")
        
        // Try to receive with a reasonable timeout
        print("=== Waiting for message ===")
        do {
            let message = try await consumer.receive()
            print("SUCCESS: Received message: '\(message.value)'")
            try await consumer.acknowledge(message)
        } catch {
            print("FAILED to receive message: \(error)")
            throw error
        }
        
        await producer.dispose()
        await consumer.dispose()
        await client.dispose()
    }
}