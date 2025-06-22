import Testing
import Foundation
import PulsarClient

// Base test support class for integration tests
actor IntegrationTestCase {
    static let defaultTimeout: TimeInterval = 60
    static let serviceURL = ProcessInfo.processInfo.environment["PULSAR_SERVICE_URL"] ?? "pulsar://localhost:6650"
    static let adminURL = ProcessInfo.processInfo.environment["PULSAR_ADMIN_URL"] ?? "http://localhost:8080"
    
    var client: PulsarClient? {
        get async {
            return _client
        }
    }
    
    private var _client: PulsarClient?
    private var createdTopics: [String] = []
    
    init() async throws {
        // Setup client with appropriate configuration
        self._client = try await createClient()
    }
    
    deinit {
        // Cleanup will be handled by test teardown
    }
    
    func tearDown() async {
        await cleanup()
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
        let topicName = topic
            .replacingOccurrences(of: "persistent://", with: "")
            .replacingOccurrences(of: "public/default/", with: "")
        
        var request = URLRequest(url: URL(string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)")!)
        request.httpMethod = "PUT"
        
        if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        
        let (_, response) = try await URLSession.shared.data(for: request)
        
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
        let topicName = topic
            .replacingOccurrences(of: "persistent://", with: "")
            .replacingOccurrences(of: "public/default/", with: "")
        
        var request = URLRequest(url: URL(string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)/partitions")!)
        request.httpMethod = "PUT"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(["numPartitions": partitions])
        
        if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        
        let (_, response) = try await URLSession.shared.data(for: request)
        
        guard let httpResponse = response as? HTTPURLResponse else {
            throw IntegrationTestError.topicCreationFailed(topic)
        }
        
        // Accept 200-299 (success) or 409 (topic already exists)
        guard (200...299).contains(httpResponse.statusCode) || httpResponse.statusCode == 409 else {
            throw IntegrationTestError.topicCreationFailed(topic)
        }
    }
    
    private func cleanup() async {
        // Close client
        if let client = _client {
            await client.dispose()
        }
        
        // Clean up topics (optional, for clean test runs)
        for topic in createdTopics {
            try? await deleteTopicViaAdmin(topic)
        }
    }
    
    private func deleteTopicViaAdmin(_ topic: String) async throws {
        // Extract just the topic name from the full topic URL
        let topicName = topic
            .replacingOccurrences(of: "persistent://", with: "")
            .replacingOccurrences(of: "public/default/", with: "")
        
        var request = URLRequest(url: URL(string: "\(Self.adminURL)/admin/v2/persistent/public/default/\(topicName)")!)
        request.httpMethod = "DELETE"
        
        if let token = ProcessInfo.processInfo.environment["PULSAR_AUTH_TOKEN"] {
            request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        }
        
        _ = try? await URLSession.shared.data(for: request)
    }
}

enum IntegrationTestError: Error {
    case topicCreationFailed(String)
    case clientNotInitialized
}