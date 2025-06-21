import Testing
@testable import PulsarClient
import Foundation

@Suite("Protocol Buffer Tests")
struct ProtocolTests {
    
    @Test("Frame encoding and decoding")
    func testFrameEncodingDecoding() throws {
        let builder = PulsarCommandBuilder()
        let encoder = PulsarFrameEncoder()
        let decoder = PulsarFrameDecoder()
        
        // Create a CONNECT command
        let connectCommand = builder.connect(clientVersion: "TestClient/1.0")
        let frame = PulsarFrame(command: connectCommand)
        
        // Encode the frame
        let encoded = try encoder.encode(frame: frame)
        #expect(encoded.count > 0)
        
        // Decode the frame
        let decoded = try decoder.decode(from: encoded)
        #expect(decoded != nil)
        #expect(decoded?.command.type == .connect)
        #expect(decoded?.command.connect.clientVersion == "TestClient/1.0")
    }
    
    @Test("PING/PONG commands")
    func testPingPongCommands() throws {
        let builder = PulsarCommandBuilder()
        let encoder = PulsarFrameEncoder()
        
        // Create PING command
        let pingCommand = builder.ping()
        let pingFrame = PulsarFrame(command: pingCommand)
        let pingEncoded = try encoder.encode(frame: pingFrame)
        #expect(pingEncoded.count > 0)
        #expect(pingCommand.type == .ping)
        
        // Create PONG command
        let pongCommand = builder.pong()
        let pongFrame = PulsarFrame(command: pongCommand)
        let pongEncoded = try encoder.encode(frame: pongFrame)
        #expect(pongEncoded.count > 0)
        #expect(pongCommand.type == .pong)
    }
    
    @Test("LOOKUP command")
    func testLookupCommand() throws {
        let builder = PulsarCommandBuilder()
        
        let lookupCommand = builder.lookup(topic: "persistent://public/default/test-topic")
        #expect(lookupCommand.type == .lookup)
        #expect(lookupCommand.lookupTopic.topic == "persistent://public/default/test-topic")
        #expect(lookupCommand.lookupTopic.requestID > 0)
    }
    
    @Test("Producer creation command")
    func testProducerCommand() throws {
        let builder = PulsarCommandBuilder()
        
        let schemaInfo = SchemaInfo(name: "TestSchema", type: .json)
        let (command, producerId) = builder.createProducer(
            topic: "persistent://public/default/test-topic",
            producerName: "test-producer",
            schema: schemaInfo
        )
        
        #expect(command.type == .producer)
        #expect(command.producer.topic == "persistent://public/default/test-topic")
        #expect(command.producer.producerName == "test-producer")
        #expect(command.producer.producerID == producerId)
        #expect(command.producer.schema.type == .json)
    }
    
    @Test("Consumer subscription command")
    func testSubscribeCommand() throws {
        let builder = PulsarCommandBuilder()
        
        let (command, consumerId) = builder.subscribe(
            topic: "persistent://public/default/test-topic",
            subscription: "test-subscription",
            subType: .shared,
            consumerName: "test-consumer"
        )
        
        #expect(command.type == .subscribe)
        #expect(command.subscribe.topic == "persistent://public/default/test-topic")
        #expect(command.subscribe.subscription == "test-subscription")
        #expect(command.subscribe.subType == .shared)
        #expect(command.subscribe.consumerName == "test-consumer")
        #expect(command.subscribe.consumerID == consumerId)
    }
    
    @Test("Message send command with metadata")
    func testSendCommand() throws {
        let builder = PulsarCommandBuilder()
        let encoder = PulsarFrameEncoder()
        
        // Create send command
        let sendCommand = builder.send(producerId: 1, sequenceId: 100)
        
        // Create message metadata
        let metadata = builder.createMessageMetadata(
            producerName: "test-producer",
            sequenceId: 100,
            properties: ["key1": "value1", "key2": "value2"]
        )
        
        // Create payload
        let payload = "Hello, Pulsar!".data(using: .utf8)!
        
        // Create frame with command, metadata, and payload
        let frame = PulsarFrame(command: sendCommand, metadata: metadata, payload: payload)
        
        // Encode and check
        let encoded = try encoder.encode(frame: frame)
        #expect(encoded.count > 0)
        
        // Decode and verify
        let decoder = PulsarFrameDecoder()
        let decoded = try decoder.decode(from: encoded)
        
        #expect(decoded != nil)
        #expect(decoded?.command.type == .send)
        #expect(decoded?.command.send.producerID == 1)
        #expect(decoded?.command.send.sequenceID == 100)
        #expect(decoded?.metadata?.producerName == "test-producer")
        #expect(decoded?.payload == payload)
    }
    
    @Test("ACK command")
    func testAckCommand() throws {
        let builder = PulsarCommandBuilder()
        
        let messageId = MessageId(ledgerId: 123, entryId: 456, partition: 0, batchIndex: -1)
        let ackCommand = builder.ack(consumerId: 1, messageId: messageId)
        
        #expect(ackCommand.type == .ack)
        #expect(ackCommand.ack.consumerID == 1)
        #expect(ackCommand.ack.messageID.count == 1)
        #expect(ackCommand.ack.messageID[0].ledgerID == 123)
        #expect(ackCommand.ack.messageID[0].entryID == 456)
    }
    
    @Test("FLOW command")
    func testFlowCommand() throws {
        let builder = PulsarCommandBuilder()
        
        let flowCommand = builder.flow(consumerId: 1, messagePermits: 1000)
        
        #expect(flowCommand.type == .flow)
        #expect(flowCommand.flow.consumerID == 1)
        #expect(flowCommand.flow.messagePermits == 1000)
    }
}