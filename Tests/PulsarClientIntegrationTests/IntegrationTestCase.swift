import Foundation
import PulsarClient
import Testing

// on Linux, FoundationNetworking is a seperate library
// so we import it here if we can
#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

// Base test support class for integration tests
actor IntegrationTestCase {
  static let defaultTimeout: TimeInterval = 60
  static let serviceURL =
    ProcessInfo.processInfo.environment["PULSAR_SERVICE_URL"] ?? "pulsar://localhost:6650"
  static let adminURL =
    ProcessInfo.processInfo.environment["PULSAR_ADMIN_URL"] ?? "http://localhost:8080"

  var client: PulsarClient? {
    get async { _client }
  }

  private var _client: PulsarClient?
  private var createdTopics: [String] = []

  private let urlSession: URLSession

  init() async throws {
    let config = URLSessionConfiguration.ephemeral
    config.timeoutIntervalForRequest = 10
    self.urlSession = URLSession(configuration: config)

    // Setup client with appropriate configuration
    self._client = try await createClient()
  }

  deinit {
    Task.detached { [weak self] in
      guard let self = self else { return }
      await cleanup()
    }
  }

  private func createClient() async throws -> PulsarClient {
    let builder = PulsarClientBuilder()
      .withServiceUrl(Self.serviceURL)

    // Add authentication if token is provided
    if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
      builder.withAuthentication(TokenAuthentication(token: token))
    }

    return builder.build()
  }

  func createTopic() async throws -> String {
    let topicName = "persistent://public/default/test-\(UUID().uuidString)"
    createdTopics.append(topicName)

    // Create topic via admin API
    try await createTopicViaAdmin(topicName)

    return topicName
  }

  func createPartitionedTopic(partitions: Int) async throws -> String {
    let topicName = "persistent://public/default/test-partitioned-\(UUID().uuidString)"
    createdTopics.append(topicName)

    // Create partitioned topic via admin API
    try await createPartitionedTopicViaAdmin(topicName, partitions: partitions)

    return topicName
  }

  private func createTopicViaAdmin(_ topic: String) async throws {
    // Extract just the topic name from the full topic URL
    let topicName =
      topic
      .replacingOccurrences(of: "persistent://", with: "")
      .replacingOccurrences(of: "public/default/", with: "")

    var request = URLRequest(
      url: URL(string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)")!)
    request.httpMethod = "PUT"

    if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
      request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    }

    let (_, response) = try await urlSession.data(for: request)

    guard let httpResponse = response as? HTTPURLResponse else {
      throw IntegrationTestError.topicCreationFailed(topic)
    }

    // Accept 200-299 (success) or 409 (topic already exists)
    guard (200...299).contains(httpResponse.statusCode) || httpResponse.statusCode == 409 else {
      throw IntegrationTestError.topicCreationFailed(topic)
    }
  }

  private func createPartitionedTopicViaAdmin(_ topic: String, partitions: Int) async throws {
    // Extract just the topic name from the full topic URL
    let topicName =
      topic
      .replacingOccurrences(of: "persistent://", with: "")
      .replacingOccurrences(of: "public/default/", with: "")

    var request = URLRequest(
      url: URL(
        string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)/partitions")!)
    request.httpMethod = "PUT"
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = try JSONEncoder().encode(["numPartitions": partitions])

    if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
      request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    }

    let (_, response) = try await urlSession.data(for: request)

    guard let httpResponse = response as? HTTPURLResponse else {
      throw IntegrationTestError.topicCreationFailed(topic)
    }

    // Accept 200-299 (success) or 409 (topic already exists)
    guard (200...299).contains(httpResponse.statusCode) || httpResponse.statusCode == 409 else {
      throw IntegrationTestError.topicCreationFailed(topic)
    }
  }

  func cleanup() async {
    print("[CI-Cleanup] Starting cleanup process.")
    // Close client
    if let client = _client {
      print("[CI-Cleanup] Disposing PulsarClient.")
      await client.dispose()
      print("[CI-Cleanup] PulsarClient disposed.")
    }

    // Clean up topics (optional, for clean test runs)
    for topic in createdTopics {
      try? await deleteTopicViaAdmin(topic)
    }

    // Make sure all outstanding HTTP work is done & the session is closed
    print("[CI-Cleanup] Invalidating URLSession.")
    urlSession.finishTasksAndInvalidate()
    print("[CI-Cleanup] Cleanup process finished.")
  }

  private func deleteTopicViaAdmin(_ topic: String) async throws {
    // Extract just the topic name from the full topic URL
    let topicName =
      topic
      .replacingOccurrences(of: "persistent://", with: "")
      .replacingOccurrences(of: "public/default/", with: "")

    var request = URLRequest(
      url: URL(string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)")!)
    request.httpMethod = "DELETE"

    if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
      request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    }

    _ = try? await urlSession.data(for: request)
  }
}

// MARK: - Admin helpers
extension IntegrationTestCase {
  /// Waits until the given subscription has no connected consumers (or the subscription is gone).
  /// Uses the Admin `stats` endpoint to poll broker state.
  func waitForNoConsumers(
    topic: String,
    subscription: String,
    timeout: TimeInterval = 10,
    pollInterval: TimeInterval = 0.2
  ) async {
    let topicName =
      topic
      .replacingOccurrences(of: "persistent://", with: "")
      .replacingOccurrences(of: "public/default/", with: "")

    guard
      let url = URL(
        string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)/stats"
      )
    else {
      return
    }

    struct TopicStats: Decodable { let subscriptions: [String: SubscriptionStats]? }
    struct SubscriptionStats: Decodable { let consumers: [ConsumerStats]? }
    struct ConsumerStats: Decodable {}

    let deadline = Date().addingTimeInterval(timeout)
    while Date() < deadline {
      var request = URLRequest(url: url)
      if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
        request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
      }

      do {
        let (data, _) = try await urlSession.data(for: request)
        if let stats = try? JSONDecoder().decode(TopicStats.self, from: data) {
          if let sub = stats.subscriptions?[subscription] {
            if (sub.consumers ?? []).isEmpty { return }
          } else {
            // Subscription no longer present
            return
          }
        }
      } catch {
        // ignore transient errors during teardown
      }

      // Wait and retry
      try? await Task.sleep(nanoseconds: UInt64(pollInterval * 1_000_000_000))
    }
  }
}

enum IntegrationTestError: Error {
  case topicCreationFailed(String)
  case clientNotInitialized
}
