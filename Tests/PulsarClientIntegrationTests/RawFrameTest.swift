import Testing
import Foundation
@testable import PulsarClient

@Suite("Raw Frame Test") 
struct RawFrameTest {
    @Test("Check Raw Frame Flow", .timeLimit(.minutes(1)))
    func testRawFrameFlow() async throws {
        // Create client
        let client = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .build()
        
        let topic = "persistent://public/default/test-raw-frame-\(UUID().uuidString)"
        
        // Create producer
        print("\n=== Creating producer ===")
        let producer = try await client.newStringProducer(topic: topic)
        print("Producer created successfully")
        
        // Wait a moment to see if any frames arrive
        print("\n=== Waiting 2 seconds to observe frame traffic ===")
        try await Task.sleep(nanoseconds: 2_000_000_000)
        
        print("\n=== Attempting to send message ===")
        do {
            let messageId = try await producer.send("Test message")
            print("Message sent! ID: \(messageId)")
        } catch {
            print("Send failed: \(error)")
        }
        
        print("\n=== Test complete ===")
        
        // Cleanup
        await producer.dispose()
        await client.dispose()
    }
}