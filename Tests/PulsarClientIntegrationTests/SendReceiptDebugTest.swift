import Testing
import Foundation
@testable import PulsarClient

@Test("Send Receipt Debug Test", .timeLimit(.minutes(1)))
func testSendReceiptDebug() async throws {
    print("\n\n=== SEND RECEIPT DEBUG TEST STARTED ===\n")
    
    let client = PulsarClientBuilder()
        .withServiceUrl("pulsar://localhost:6650")
        .build()
    
    let topic = "persistent://public/default/test-send-receipt-debug-\(UUID().uuidString)"
    
    print("Creating producer for topic: \(topic)")
    let producer = try await client.newStringProducer(topic: topic)
    print("Producer created successfully")
    
    print("\nAttempting to send message...")
    do {
        let messageId = try await withTimeout(seconds: 10) {
            try await producer.send("Test message for debugging")
        }
        print("✅ SUCCESS! Message sent with ID: \(messageId)")
    } catch {
        print("❌ FAILED! Error: \(error)")
        if error is PulsarClientError {
            print("PulsarClientError details: \(error)")
        }
    }
    
    print("\nCleaning up...")
    await producer.dispose()
    await client.dispose()
    
    print("\n=== TEST COMPLETED SUCCESSFULLY ===\n")
}