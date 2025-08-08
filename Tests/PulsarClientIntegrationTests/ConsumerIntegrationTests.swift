import Foundation
import Testing

@testable import PulsarClient

@Suite("Consumer Integration Tests")
class ConsumerIntegrationTests {
  let testCase: IntegrationTestCase

  init() async throws {
    self.testCase = try await IntegrationTestCase()
  }

  // Non-blocking cleanup to avoid CI teardown deadlocks
  deinit { Task { [testCase] in await testCase.cleanup() } }

  @Test("Subscription Types", .timeLimit(.minutes(2)))
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

  @Test("Message Acknowledgment", .timeLimit(.minutes(3)))
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
        .subscriptionType(.exclusive)
        .initialPosition(.earliest)
    }

    // Send messages
    for i in 0..<5 {
      try await producer.send("Message \(i)")
    }

    // Receive but don't acknowledge first message
    let firstMessage = try await consumer.receive(timeout: 15.0)
    #expect(firstMessage.value == "Message 0")

    // Close and reopen consumer (wait until broker fully detaches first consumer)
    await consumer.dispose()
    await testCase.waitForNoConsumers(topic: topic, subscription: "ack-sub", timeout: 20)

    // Try to re-open the same exclusive subscription with retries to avoid broker-side teardown races
    let consumer2: any ConsumerProtocol<String> = try await {
      var lastError: Error?
      for i in 0..<60 {
        do {
          return try await client.newConsumer(topic: topic, schema: Schema<String>.string) {
            builder in
            _ =
              builder
              .subscriptionName("ack-sub")
              .subscriptionType(.exclusive)
          }
        } catch {
          lastError = error
          if i == 59 { break }
          try? await Task.sleep(nanoseconds: 500_000_000)
        }
      }
      throw lastError ?? PulsarClientError.unknownError("failed to reopen consumer")
    }()

    // Should receive unacknowledged message again
    let redeliveredMessage = try await consumer2.receive(timeout: 15.0)
    #expect(redeliveredMessage.value == "Message 0")

    // Acknowledge this time
    try await consumer2.acknowledge(redeliveredMessage)

    await producer.dispose()
    await consumer2.dispose()
  }
}
