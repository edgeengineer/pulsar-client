import Foundation
import NIOCore
import Testing

@testable import PulsarClient

@Suite("ByteBuffer Tests")
struct ByteBufferTests {
    
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
        buffer.writeInteger(testValue, endianness: .big)
        
        #expect(buffer.readableBytes == 4)
        
        let readValue = buffer.readInteger(endianness: .big, as: UInt32.self)
        #expect(readValue == testValue)
    }
}