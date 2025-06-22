import Testing
import Foundation
@testable import PulsarClient

@Suite("Serialization Debug")
struct SerializationDebugTest {
    @Test("Debug Send Command Serialization")
    func testSendCommandSerialization() throws {
        // Create a send command like we do in production
        let commandBuilder = PulsarCommandBuilder()
        let sendCommand = commandBuilder.send(
            producerId: 1,
            sequenceId: 0,
            numMessages: 1
        )
        
        // Create metadata
        let metadata = commandBuilder.createMessageMetadata(
            producerName: "producer-1",
            sequenceId: 0,
            publishTime: Date(),
            properties: [:]
        )
        
        // Create frame
        let frame = PulsarFrame(
            command: sendCommand,
            metadata: metadata,
            payload: Data("Test message".utf8)
        )
        
        // Encode it
        let encoder = PulsarFrameEncoder()
        let encoded = try encoder.encode(frame: frame)
        
        // Print hex dump
        print("Encoded frame (\(encoded.count) bytes):")
        let hex = encoded.map { String(format: "%02x", $0) }.joined(separator: " ")
        print(hex)
        
        // Decode to verify
        let decoder = PulsarFrameDecoder()
        if let decoded = try decoder.decode(from: encoded) {
            print("\nDecoded successfully:")
            print("Command type: \(decoded.command.type)")
            print("Producer ID: \(decoded.command.send.producerID)")
            print("Sequence ID: \(decoded.command.send.sequenceID)")
            print("Num messages: \(decoded.command.send.numMessages)")
            
            if let metadata = decoded.metadata {
                print("\nMetadata:")
                print("Producer name: \(metadata.producerName)")
                print("Sequence ID: \(metadata.sequenceID)")
                print("Publish time: \(metadata.publishTime)")
            }
            
            if let payload = decoded.payload {
                print("\nPayload: \(String(data: payload, encoding: .utf8) ?? "nil")")
            }
        }
    }
}