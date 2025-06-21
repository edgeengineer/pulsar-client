import Testing
import Foundation
@testable import PulsarClient

@Suite("PulsarClient Final Tests")
struct FinalTests {
    
    @Test("PulsarClient Builder")
    func testPulsarClientBuilder() {
        let client = PulsarClient.builder { builder in
            builder.withServiceUrl("pulsar://localhost:6650")
                   .withAuthentication(TokenAuthentication(token: "test-token"))
                   .withEncryptionPolicy(.preferEncrypted)
        }
        
        // Client should be created successfully
        #expect(type(of: client) == PulsarClient.self)
    }
    
    @Test("MessageId Functionality")
    func testMessageId() {
        let id1 = MessageId(ledgerId: 100, entryId: 200, partition: 0, batchIndex: -1)
        let id2 = MessageId(ledgerId: 100, entryId: 201, partition: 0, batchIndex: -1)
        
        // Test comparison
        #expect(id1 < id2)
        
        // Test description
        #expect(id1.description == "100:200:0:-1")
        
        // Test special values
        #expect(MessageId.earliest.ledgerId == UInt64.max)
        #expect(MessageId.latest.ledgerId == Int64.max.magnitude)
        
        // Test equality
        let id3 = MessageId(ledgerId: 100, entryId: 200, partition: 0, batchIndex: -1)
        #expect(id1 == id3)
    }
    
    @Test("String Schema")
    func testStringSchema() throws {
        let schema = Schemas.string
        let testString = "Hello, Pulsar!"
        let encoded = try schema.encode(testString)
        let decoded = try schema.decode(encoded)
        #expect(decoded == testString)
    }
    
    @Test("Numeric Schemas")
    func testNumericSchemas() throws {
        // Int32
        let int32Schema = Schemas.int32
        let testInt: Int32 = 42
        let intEncoded = try int32Schema.encode(testInt)
        let intDecoded = try int32Schema.decode(intEncoded)
        #expect(intDecoded == testInt)
        
        // Int64
        let int64Schema = Schemas.int64
        let testLong: Int64 = 123456789
        let longEncoded = try int64Schema.encode(testLong)
        let longDecoded = try int64Schema.decode(longEncoded)
        #expect(longDecoded == testLong)
        
        // Double
        let doubleSchema = Schemas.double
        let testDouble: Double = 3.14159
        let doubleEncoded = try doubleSchema.encode(testDouble)
        let doubleDecoded = try doubleSchema.decode(doubleEncoded)
        #expect(doubleDecoded == testDouble)
    }
    
    @Test("Boolean Schema")
    func testBooleanSchema() throws {
        let schema = Schemas.boolean
        let trueEncoded = try schema.encode(true)
        let falseEncoded = try schema.encode(false)
        
        #expect(try schema.decode(trueEncoded) == true)
        #expect(try schema.decode(falseEncoded) == false)
    }
    
    @Test("Bytes Schema")
    func testBytesSchema() throws {
        let schema = Schemas.bytes
        let testData = Data([0x01, 0x02, 0x03, 0x04, 0x05])
        let encoded = try schema.encode(testData)
        let decoded = try schema.decode(encoded)
        
        #expect(decoded == testData)
        #expect(encoded == testData) // Bytes schema passes through
    }
    
    @Test("Message Metadata")
    func testMessageMetadata() {
        var metadata = MessageMetadata()
        
        // Test properties
        metadata.properties["app"] = "test-app"
        metadata.properties["version"] = "1.0.0"
        #expect(metadata.properties.count == 2)
        #expect(metadata.properties["app"] == "test-app")
        
        // Test key
        metadata.key = "partition-key"
        #expect(metadata.key == "partition-key")
        
        // Test event time
        let eventTime = Date()
        metadata.eventTime = eventTime
        #expect(metadata.eventTime == eventTime)
        
        // Test builder pattern
        let builderMetadata = MessageMetadata()
            .withKey("test-key")
            .withProperty("test", "value")
            .withDisableReplication(true)
        
        #expect(builderMetadata.key == "test-key")
        #expect(builderMetadata.properties["test"] == "value")
        #expect(builderMetadata.disableReplication == true)
    }
    
    @Test("Authentication")
    func testAuthentication() async throws {
        // Token authentication
        let tokenAuth = TokenAuthentication(token: "test-token")
        #expect(tokenAuth.authenticationMethodName == "token")
        
        let tokenData = try await tokenAuth.getAuthenticationData()
        #expect(String(data: tokenData, encoding: .utf8) == "test-token")
        
        // No authentication
        let noAuth = NoAuthentication()
        #expect(noAuth.authenticationMethodName == "none")
        
        let noAuthData = try await noAuth.getAuthenticationData()
        #expect(noAuthData.isEmpty)
    }
    
    @Test("Encryption Policy")
    func testEncryptionPolicy() {
        let enforceEncrypted = EncryptionPolicy.enforceEncrypted
        let preferUnencrypted = EncryptionPolicy.preferUnencrypted
        let preferEncrypted = EncryptionPolicy.preferEncrypted
        let enforceUnencrypted = EncryptionPolicy.enforceUnencrypted
        
        // Test encryption requirements
        #expect(enforceEncrypted.isEncryptionRequired == true)
        #expect(preferUnencrypted.isEncryptionRequired == false)
        #expect(preferEncrypted.isEncryptionRequired == false)
        #expect(enforceUnencrypted.isEncryptionRequired == false)
        
        // Test preferences
        #expect(enforceEncrypted.encryptionPreference > preferEncrypted.encryptionPreference)
        #expect(preferEncrypted.encryptionPreference > preferUnencrypted.encryptionPreference)
        #expect(preferUnencrypted.encryptionPreference > enforceUnencrypted.encryptionPreference)
        
        // Test descriptions
        #expect(enforceEncrypted.description == "enforce-encrypted")
        #expect(preferUnencrypted.description == "prefer-unencrypted")
    }
    
    @Test("Producer Access Mode")
    func testProducerAccessMode() {
        let exclusive = ProducerAccessMode.exclusive
        let shared = ProducerAccessMode.shared
        let waitForExclusive = ProducerAccessMode.waitForExclusive
        
        #expect(exclusive.description == "exclusive")
        #expect(shared.description == "shared")
        #expect(waitForExclusive.description == "wait-for-exclusive")
    }
    
    @Test("Subscription Modes")
    func testSubscriptionModes() {
        // Regex subscription modes
        let persistent = RegexSubscriptionMode.persistent
        let nonPersistent = RegexSubscriptionMode.nonPersistent
        let all = RegexSubscriptionMode.all
        
        #expect(persistent.description == "persistent")
        #expect(nonPersistent.description == "non-persistent")
        #expect(all.description == "all")
    }
    
    @Test("Message Routing")
    func testMessageRouting() {
        let roundRobin = RoundRobinMessageRouter()
        let metadata = MessageMetadata()
        
        // Test round-robin behavior (remove async for now)
        var partitions: [Int] = []
        for _ in 0..<5 {
            let partition = roundRobin.choosePartition(messageMetadata: metadata, numberOfPartitions: 3)
            partitions.append(partition)
        }
        
        // Should cycle through partitions (may start at any point)
        #expect(partitions.count == 5)
        #expect(Set(partitions) == Set([0, 1, 2]))
        
        // Test key-based routing
        let keyBased = KeyBasedMessageRouter()
        var keyMetadata = MessageMetadata()
        keyMetadata.key = "test-key"
        
        let partition1 = keyBased.choosePartition(messageMetadata: keyMetadata, numberOfPartitions: 10)
        let partition2 = keyBased.choosePartition(messageMetadata: keyMetadata, numberOfPartitions: 10)
        
        // Same key should go to same partition
        #expect(partition1 == partition2)
        #expect(partition1 >= 0 && partition1 < 10)
    }
    
    @Test("Message Creation")
    func testMessageCreation() {
        let messageId = MessageId(ledgerId: 100, entryId: 200, partition: 0, batchIndex: -1)
        let metadata = MessageMetadata().withKey("test-key")
        
        let message = Message<String>(
            id: messageId,
            value: "Hello, Pulsar!",
            metadata: metadata,
            publishTime: Date(),
            topicName: "test-topic"
        )
        
        #expect(message.id == messageId)
        #expect(message.value == "Hello, Pulsar!")
        #expect(message.key == "test-key")
        #expect(message.topicName == "test-topic")
        #expect(message.hasKey == true)
        #expect(message.hasEventTime == false)
    }
    
    @Test("JSON Schema")
    func testJSONSchema() throws {
        struct TestMessage: Codable, Equatable {
            let id: Int
            let name: String
            let active: Bool
        }
        
        let schema = JSONSchema<TestMessage>()
        let message = TestMessage(id: 42, name: "Test", active: true)
        
        let encoded = try schema.encode(message)
        let decoded = try schema.decode(encoded)
        
        #expect(decoded == message)
    }
    
    @Test("Schema Properties")
    func testSchemaProperties() {
        // Test schema info properties
        let stringSchema = Schemas.string
        #expect(stringSchema.schemaInfo.name == "String")
        
        let boolSchema = Schemas.boolean
        #expect(boolSchema.schemaInfo.name == "Boolean")
        
        let bytesSchema = Schemas.bytes
        #expect(bytesSchema.schemaInfo.name == "Bytes")
    }
}