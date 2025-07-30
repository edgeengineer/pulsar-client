import Testing
import Foundation
@testable import PulsarClient

@Suite("Debug Send Receipt")
class DebugSendReceiptTest {    
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
    
    @Test("Debug Send Receipt", .timeLimit(.minutes(1)))
    func testSendReceipt() async throws {
        guard let client = await testCase.client else {
            throw IntegrationTestError.clientNotInitialized
        }
        
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
        await client.dispose()
    }
}