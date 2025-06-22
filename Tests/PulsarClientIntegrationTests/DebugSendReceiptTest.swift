import Testing
import Foundation
@testable import PulsarClient

@Suite("Debug Send Receipt")
struct DebugSendReceiptTest {
    @Test("Debug Send Receipt", .timeLimit(.minutes(1)))
    func testSendReceipt() async throws {
        // Create client
        let client = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .build()
        
        let topic = "persistent://public/default/test-send-receipt-\(UUID().uuidString)"
        
        // Create producer
        print("Creating producer...")
        let producer = try await client.newStringProducer(topic: topic)
        print("Producer created")
        
        // Send a single message
        print("Sending message...")
        do {
            let messageId = try await producer.send("Test message")
            print("Message sent! ID: \(messageId)")
        } catch {
            print("Send failed with error: \(error)")
        }
        
        // Cleanup
        await producer.dispose()
    }
}