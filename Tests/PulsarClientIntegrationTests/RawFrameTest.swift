import Foundation
import Testing

@testable import PulsarClient

@Suite("Raw Frame Test")
class RawFrameTest {
  let testCase: IntegrationTestCase

  init() async throws {
    self.testCase = try await IntegrationTestCase()
  }

  // Non-blocking cleanup to avoid CI teardown deadlocks
  deinit { Task { [testCase] in await testCase.cleanup() } }

  @Test("Check Raw Frame Flow", .timeLimit(.minutes(1)))
  func testRawFrameFlow() async throws {
    guard let client = await testCase.client else {
      throw IntegrationTestError.clientNotInitialized
    }

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
