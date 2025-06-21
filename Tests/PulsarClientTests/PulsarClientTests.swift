import Testing
@testable import PulsarClient
import Foundation

@Suite("PulsarClient Tests")
struct PulsarClientTests {
    
    @Test("Create PulsarClient with builder")
    func testClientBuilder() async throws {
        let client = PulsarClient.builder { builder in
            builder.serviceUrl("pulsar://localhost:6650")
        }
        
        // Client should be created successfully
        
        await client.dispose()
    }
    
    @Test("Schema types")
    func testSchemas() throws {
        let stringSchema = Schema<String>.string
        #expect(stringSchema.schemaInfo.type == .string)
        
        let bytesSchema = Schema<Data>.bytes
        #expect(bytesSchema.schemaInfo.type == .bytes)
        
        struct TestData: Codable {
            let id: Int
            let name: String
        }
        
        let jsonSchema = Schema<TestData>.json(TestData.self)
        #expect(jsonSchema.schemaInfo.type == .json)
    }
    
    @Test("String schema encoding/decoding")
    func testStringSchema() throws {
        let schema = Schema<String>.string
        let original = "Hello, Pulsar!"
        
        let encoded = try schema.encode(original)
        let decoded = try schema.decode(encoded)
        
        #expect(decoded == original)
    }
    
    @Test("JSON schema encoding/decoding")
    func testJSONSchema() throws {
        struct User: Codable, Equatable {
            let id: Int
            let name: String
            let email: String
        }
        
        let schema = Schema<User>.json(User.self)
        let original = User(id: 1, name: "John Doe", email: "john@example.com")
        
        let encoded = try schema.encode(original)
        let decoded = try schema.decode(encoded)
        
        #expect(decoded == original)
    }
    
    @Test("MessageId comparison")
    func testMessageIdComparison() {
        let id1 = MessageId(ledgerId: 1, entryId: 1)
        let id2 = MessageId(ledgerId: 1, entryId: 2)
        let id3 = MessageId(ledgerId: 2, entryId: 1)
        
        #expect(id1 < id2)
        #expect(id2 < id3)
        #expect(MessageId.earliest < MessageId.latest)
    }
    
    @Test("MessageMetadata initialization")
    func testMessageMetadata() {
        let metadata = MessageMetadata(
            key: "test-key",
            properties: ["env": "test", "version": "1.0"],
            eventTime: Date(),
            sequenceId: 12345
        )
        
        #expect(metadata.key == "test-key")
        #expect(metadata.properties["env"] == "test")
        #expect(metadata.properties["version"] == "1.0")
        #expect(metadata.sequenceId == 12345)
    }
}