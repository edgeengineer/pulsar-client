import Foundation
import Testing

@testable import PulsarClient

// on Linux, FoundationNetworking is a seperate library
// so we import it here if we can
#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

@Suite("Failure Scenario Tests")
class FailureScenarioTests {
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

  @Test("Connection Failure Recovery")
  func testConnectionFailure() async throws {
    // Skip if toxiproxy is not available on CI runners
    if let url = URL(string: "http://localhost:8474/version") {
      let ok: Bool = (try? await URLSession.shared.data(from: url).0) != nil
      if !ok {
        print("Toxiproxy not available; skipping testConnectionFailure")
        return
      }
    }
    // This test requires Toxiproxy to be running
    let toxiproxyClient = ToxiproxyClient(baseURL: "http://localhost:8474")
    let proxy = try await toxiproxyClient.getProxy(name: "pulsar")

    let topic = try await testCase.createTopic()
    let client = PulsarClientBuilder()
      .withServiceUrl("pulsar://localhost:16650")  // Toxiproxy port
      .withOperationTimeout(1.0)  // Shorter timeout for testing
      .build()

    let producer = try await client.newStringProducer(topic: topic)

    // Send initial message
    let messageId1 = try await producer.send("Before failure")
    // MessageId is non-optional so test for a valid ledgerId instead
    #expect(messageId1.ledgerId > 0)

    // Disable proxy to simulate network failure
    try await proxy.disable()

    // Wait for the timeout
    try await Task.sleep(nanoseconds: 2_000_000_000)  // 2 seconds

    // Re-enable proxy
    try await proxy.enable()

    // Wait for reconnection
    try await Task.sleep(nanoseconds: 3_000_000_000)  // 3 seconds

    // Should be able to send again
    let messageId2 = try await producer.send("After recovery")
    // MessageId is non-optional so test for a valid ledgerId instead
    #expect(messageId2.ledgerId > 0)

    await producer.dispose()
    await client.dispose()
    proxy.finish()
  }
}

// Simplified Toxiproxy client for testing
struct ToxiproxyClient {
  let baseURL: String

  func getProxy(name: String) async throws -> ToxiproxyProxy {
    return ToxiproxyProxy(client: self, name: name)
  }
}

struct ToxiproxyProxy {
  let client: ToxiproxyClient
  let name: String
  private let urlSession: URLSession

  init(client: ToxiproxyClient, name: String) {
    self.client = client
    self.name = name
    let config = URLSessionConfiguration.ephemeral
    config.timeoutIntervalForRequest = 5
    self.urlSession = URLSession(configuration: config)
  }

  func disable() async throws {
    var request = URLRequest(url: URL(string: "\(client.baseURL)/proxies/\(name)")!)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = try JSONEncoder().encode(["enabled": false])

    _ = try await urlSession.data(for: request)
  }

  func enable() async throws {
    var request = URLRequest(url: URL(string: "\(client.baseURL)/proxies/\(name)")!)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = try JSONEncoder().encode(["enabled": true])

    _ = try await urlSession.data(for: request)
  }

  func finish() {
    urlSession.finishTasksAndInvalidate()
  }
}
