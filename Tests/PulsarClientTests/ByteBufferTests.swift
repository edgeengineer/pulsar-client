import Foundation
import NIOCore
import Testing

@testable import PulsarClient

@Suite("ByteBuffer Tests")
struct ByteBufferTests {
    
    @Test("ByteBuffer to Data conversion")
    func testByteBufferToDataConversion() {
        let testString = "Hello, Pulsar!"
        var buffer = ByteBufferAllocator().buffer(capacity: testString.count)
        buffer.writeString(testString)
        
        let data = buffer.toData()
        let dataString = String(data: data, encoding: .utf8)
        
        #expect(dataString == testString)
    }
    
    @Test("Data to ByteBuffer conversion")
    func testDataToByteBufferConversion() {
        let testString = "Hello, ByteBuffer!"
        let data = Data(testString.utf8)
        
        let buffer = ByteBuffer.from(data)
        var mutableBuffer = buffer
        let bufferString = mutableBuffer.readString(length: mutableBuffer.readableBytes)
        
        #expect(bufferString == testString)
    }
    
    @Test("Frame encoding with ByteBuffer payload")
    func testFrameEncodingWithByteBuffer() throws {
        let commandBuilder = PulsarCommandBuilder()
        let command = commandBuilder.send(
            producerId: 1,
            sequenceId: 1,
            numMessages: 1
        )
        
        let metadata = commandBuilder.createMessageMetadata(
            producerName: "test-producer",
            sequenceId: 1,
            publishTime: Date(),
            properties: ["key": "value"]
        )
        
        // Create payload as ByteBuffer
        let payloadString = "Test payload"
        var payload = ByteBufferAllocator().buffer(capacity: payloadString.count)
        payload.writeString(payloadString)
        
        let frame = PulsarFrame(
            command: command,
            metadata: metadata,
            payload: payload
        )
        
        let encoder = PulsarFrameEncoder()
        var encoded = try encoder.encode(frame: frame)
        
        #expect(encoded.readableBytes > 0)
        
        // Verify we can decode it back
        let decoder = PulsarFrameDecoder()
        let decoded = try decoder.decode(from: &encoded)
        
        #expect(decoded != nil)
        if let decoded = decoded {
            #expect(decoded.command.type == .send)
            #expect(decoded.metadata != nil)
            #expect(decoded.payload != nil)
            
            if var decodedPayload = decoded.payload {
                let decodedString = decodedPayload.readString(length: decodedPayload.readableBytes)
                #expect(decodedString == payloadString)
            }
        }
    }
    
    @Test("ByteBuffer Big Endian operations")
    func testByteBufferBigEndianOperations() {
        var buffer = ByteBufferAllocator().buffer(capacity: 4)
        
        let testValue: UInt32 = 0x12345678
        buffer.writeUInt32BE(testValue)
        
        #expect(buffer.readableBytes == 4)
        
        let readValue = buffer.readUInt32BE()
        #expect(readValue == testValue)
    }
    
    @Test("Schema encode/decode with ByteBuffer conversion")
    func testSchemaWithByteBufferConversion() throws {
        // Test that schemas work with the Data<->ByteBuffer conversion
        let schema = Schemas.string
        let testString = "Schema test message"
        
        // Schema encodes to Data
        let encoded = try schema.encode(testString)
        
        // Convert to ByteBuffer (as done in ProducerImpl)
        var buffer = ByteBufferAllocator().buffer(capacity: encoded.count)
        buffer.writeBytes(encoded)
        
        // Convert back to Data (as done in ConsumerImpl)
        let decodedData = buffer.toData()
        
        // Schema decodes from Data
        let decoded = try schema.decode(decodedData)
        
        #expect(decoded == testString)
    }
    
    @Test("Bytes schema with ByteBuffer conversion")
    func testBytesSchemaWithByteBuffer() throws {
        let schema = Schemas.bytes
        let testData = Data([0x01, 0x02, 0x03, 0x04, 0x05])
        
        // Encode (passthrough for bytes schema)
        let encoded = try schema.encode(testData)
        #expect(encoded == testData)
        
        // Convert to ByteBuffer
        var buffer = ByteBufferAllocator().buffer(capacity: encoded.count)
        buffer.writeBytes(encoded)
        
        // Convert back to Data
        let decodedData = buffer.toData()
        
        // Decode (passthrough for bytes schema)
        let decoded = try schema.decode(decodedData)
        #expect(decoded == testData)
    }
}