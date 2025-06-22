import Testing
import Foundation
@testable import PulsarClient

@Suite("Failure Scenario Tests")
struct FailureScenarioTests {
    let testCase: IntegrationTestCase
    
    init() async throws {
        self.testCase = try await IntegrationTestCase()
    }
    
    @Test("Connection Failure Recovery")
    func testConnectionFailure() async throws {
        // This test requires Toxiproxy to be running
        let toxiproxyClient = ToxiproxyClient(baseURL: "http://localhost:8474")
        let proxy = try await toxiproxyClient.getProxy(name: "pulsar")
        
        let topic = try await testCase.createTopic()
        let client = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:16650") // Toxiproxy port
            .build()
        
        let producer = try await client.newStringProducer(topic: topic)
        
        // Send initial message
        let messageId1 = try await producer.send("Before failure")
        #expect(messageId1 != nil)
        
        // Disable proxy to simulate network failure
        try await proxy.disable()
        
        // Try to send - should fail or queue
        await #expect(throws: Error.self) {
            try await producer.send("During failure")
        }
        
        // Re-enable proxy
        try await proxy.enable()
        
        // Wait for reconnection
        try await Task.sleep(nanoseconds: 3_000_000_000) // 3 seconds
        
        // Should be able to send again
        let messageId2 = try await producer.send("After recovery")
        #expect(messageId2 != nil)
        
        await producer.dispose()
        await client.dispose()
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
    
    func disable() async throws {
        var request = URLRequest(url: URL(string: "\(client.baseURL)/proxies/\(name)")!)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(["enabled": false])
        
        _ = try await URLSession.shared.data(for: request)
    }
    
    func enable() async throws {
        var request = URLRequest(url: URL(string: "\(client.baseURL)/proxies/\(name)")!)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.httpBody = try JSONEncoder().encode(["enabled": true])
        
        _ = try await URLSession.shared.data(for: request)
    }
}