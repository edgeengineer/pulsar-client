import Foundation
import Testing

@testable import PulsarClient

@Suite("Simple Message Test")
class SimpleMessageTest {
  let testCase: IntegrationTestCase

  init() async throws {
    self.testCase = try await IntegrationTestCase()
  }

  // Non-blocking cleanup to avoid CI teardown deadlocks
  deinit { Task { [testCase] in await testCase.cleanup() } }

  @Test("Simple Send and Receive")
  func testSimpleSendReceive() async throws {
    guard let client = await testCase.client else {
      throw IntegrationTestError.clientNotInitialized
    }

    let topic = "persistent://public/default/simple-test-\(UUID().uuidString)"

    // Create producer
    let producer = try await client.newStringProducer(topic: topic)

    // Create consumer FIRST, before sending messages
    let consumer = try await client.newConsumer(topic: topic, schema: Schema<String>.string) {
      builder in
      _ =
        builder
        .subscriptionName("simple-sub")
        .subscriptionType(SubscriptionType.exclusive)
        .initialPosition(SubscriptionInitialPosition.earliest)  // Start from earliest
    }

    // Wait a bit for consumer to fully initialize
    try await Task.sleep(nanoseconds: 500_000_000)  // 0.5 seconds

    // Now send a message
    print("=== Sending message ===")
    let messageId = try await producer.send("Hello World")
    print("Message sent with ID: \(messageId)")

    // Try to receive the message
    print("=== Waiting for message ===")
    do {
      var iterator = consumer.makeAsyncIterator()
      if let messageOpt = try await iterator.next() {
        guard let message = messageOpt as? Message<String> else {
          throw PulsarClientError.unknownError("Failed to cast message")
        }
        print("SUCCESS: Received message: '\(message.value)'")
        try await consumer.acknowledge(message)
      } else {
        print("FAILED: No message received")
        throw PulsarClientError.consumerClosed
      }
    } catch {
      print("FAILED to receive message: \(error)")
      throw error
    }

    await producer.dispose()
    await consumer.dispose()
    await client.dispose()
  }
}
