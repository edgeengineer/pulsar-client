import Foundation
import NIOCore
import Testing

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
    let testMessage = "Test message"
    var payload = ByteBufferAllocator().buffer(capacity: testMessage.count)
    payload.writeString(testMessage)
    
    let frame = PulsarFrame(
      command: sendCommand,
      metadata: metadata,
      payload: payload
    )

    // Encode it
    let encoder = PulsarFrameEncoder()
    var encoded = try encoder.encode(frame: frame)

    // Print hex dump
    print("Encoded frame (\(encoded.readableBytes) bytes):")
    if let bytes = encoded.getBytes(at: encoded.readerIndex, length: encoded.readableBytes) {
      let hex = bytes.map { String(format: "%02x", $0) }.joined(separator: " ")
      print(hex)
    }

    // Decode to verify
    let decoder = PulsarFrameDecoder()
    if let decoded = try decoder.decode(from: &encoded) {
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

      if var payload = decoded.payload {
        let payloadString = payload.readString(length: payload.readableBytes)
        print("\nPayload: \(payloadString ?? "nil")")
      }
    }
  }
}
