import Foundation
import Testing

@testable import PulsarClient

@Suite("Consumer Integration Tests")
class ConsumerIntegrationTests {
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

  @Test("Subscription Types")
  func testSubscriptionTypes() async throws {
    let topic = try await testCase.createTopic()
    guard let client = await testCase.client else {
      throw IntegrationTestError.clientNotInitialized
    }

    // Test Exclusive subscription
    let exclusiveConsumer = try await client.newConsumer(
      topic: topic, schema: Schema<String>.string
    ) { builder in
      _ =
        builder
        .subscriptionName("exclusive-sub")
        .subscriptionType(.exclusive)
    }

    // Second exclusive consumer should fail
    await #expect(throws: Error.self) {
      try await client.newConsumer(topic: topic, schema: Schema<String>.string) { builder in
        _ =
          builder
          .subscriptionName("exclusive-sub")
          .subscriptionType(.exclusive)
      }
    }

    await exclusiveConsumer.dispose()

    // Test Shared subscription
    let sharedConsumer1 = try await client.newConsumer(topic: topic, schema: Schema<String>.string)
    { builder in
      _ =
        builder
        .subscriptionName("shared-sub")
        .subscriptionType(.shared)
    }

    let sharedConsumer2 = try await client.newConsumer(topic: topic, schema: Schema<String>.string)
    { builder in
      _ =
        builder
        .subscriptionName("shared-sub")
        .subscriptionType(.shared)
    }

    // check that the consumers are connected
    #expect(sharedConsumer1.state == .connected)
    #expect(sharedConsumer2.state == .connected)

    await sharedConsumer1.dispose()
    await sharedConsumer2.dispose()
  }

  @Test("Message Acknowledgment")
  func testAcknowledgment() async throws {
    let topic = try await testCase.createTopic()
    guard let client = await testCase.client else {
      throw IntegrationTestError.clientNotInitialized
    }

    let producer = try await client.newStringProducer(topic: topic)

    let consumer = try await client.newConsumer(topic: topic, schema: Schema<String>.string) {
      builder in
      _ =
        builder
        .subscriptionName("ack-sub")
        .initialPosition(.earliest)
    }

    // Send messages
    for i in 0..<5 {
      try await producer.send("Message \(i)")
    }

    // Receive but don't acknowledge first message
    let firstMessage = try await consumer.receive()
    #expect(firstMessage.value == "Message 0")

    // Close and reopen consumer
    await consumer.dispose()
    try await Task.sleep(nanoseconds: 1_000_000_000)

    let consumer2 = try await client.newConsumer(topic: topic, schema: Schema<String>.string) {
      builder in
      _ =
        builder
        .subscriptionName("ack-sub")
    }

    // Should receive unacknowledged message again
    let redeliveredMessage = try await consumer2.receive()
    #expect(redeliveredMessage.value == "Message 0")

    // Acknowledge this time
    try await consumer2.acknowledge(redeliveredMessage)

    await producer.dispose()
    await consumer2.dispose()
  }
}
