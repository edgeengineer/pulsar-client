import CyclicRedundancyCheck
import Foundation
import NIOCore
import SwiftProtobuf

/// Pulsar wire protocol constants
public enum PulsarProtocol {
  /// Magic number for Pulsar protocol
  static let magicNumber: UInt16 = 0x0e01

  /// Current protocol version (matches C# DotPulsar)
  static let protocolVersion: Int32 = 14

  /// Maximum frame size (5MB)
  static let maxFrameSize: Int = 5 * 1024 * 1024

  /// Command size offset in frame
  static let commandSizeOffset = 4
}

/// Pulsar protocol frame
public struct PulsarFrame: Sendable {
  public let totalSize: UInt32
  public let commandSize: UInt32
  public let command: Pulsar_Proto_BaseCommand
  public let metadata: Pulsar_Proto_MessageMetadata?
  public let payload: ByteBuffer?

  public init(
    command: Pulsar_Proto_BaseCommand, metadata: Pulsar_Proto_MessageMetadata? = nil,
    payload: ByteBuffer? = nil
  ) {
    self.command = command
    self.metadata = metadata
    self.payload = payload

    let commandData: Data
    do {
      commandData = try command.serializedData()
    } catch {
      print("ERROR: Failed to serialize command: \(error)")
      print("Command type: \(command.type)")
      print("Command details: \(command)")
      fatalError("Failed to serialize command: \(error)")
    }
    self.commandSize = UInt32(commandData.count)

    var totalSize = 4 + commandSize  // 4 bytes for command size + command data

    if let metadata = metadata {
      do {
        let metadataData = try metadata.serializedData()
        totalSize += UInt32(metadataData.count) + 4  // 4 bytes for metadata size + metadata data
      } catch {
        print("ERROR: Failed to serialize metadata: \(error)")
        print("Metadata details:")
        print(
          "  hasProducerName: \(metadata.hasProducerName), producerName: '\(metadata.producerName)'"
        )
        print("  hasSequenceID: \(metadata.hasSequenceID), sequenceID: \(metadata.sequenceID)")
        print("  hasPublishTime: \(metadata.hasPublishTime), publishTime: \(metadata.publishTime)")
        print("  compression: \(metadata.compression)")
        fatalError("Failed to serialize metadata: \(error)")
      }
    }

    if let payload = payload {
      totalSize += UInt32(payload.readableBytes)
    }

    self.totalSize = totalSize
  }
}

/// Pulsar frame encoder
public struct PulsarFrameEncoder {
  public init() {}

  /// Encode a frame to bytes (matches C# DotPulsar protocol specification)
  public func encode(frame: PulsarFrame) throws -> ByteBuffer {
    // Serialize command
    let commandData = try frame.command.serializedData()
    let commandSize = UInt32(commandData.count)

    // Simple frame (command only) - matches C# Serialize(BaseCommand command)
    if frame.metadata == nil && frame.payload == nil {
      var buffer = ByteBufferAllocator().buffer(capacity: Int(commandSize) + 8)
      let totalSize = commandSize + 4  // 4 bytes for command size

      buffer.writeInteger(totalSize, endianness: .big)
      buffer.writeInteger(commandSize, endianness: .big)
      buffer.writeBytes(commandData)

      return buffer
    }

    // Complex frame (with metadata/payload) - matches C# Serialize(BaseCommand, MessageMetadata, ReadOnlySequence<byte>)
    guard let metadata = frame.metadata else {
      throw PulsarClientError.protocolError("Metadata required for message frames")
    }

    // Serialize metadata
    let metadataData: Data
    do {
      metadataData = try metadata.serializedData()
    } catch {
      print("ERROR: Failed to serialize metadata in encoder: \(error)")
      print("Metadata details:")
      print(
        "  hasProducerName: \(metadata.hasProducerName), producerName: '\(metadata.producerName)'")
      print("  hasSequenceID: \(metadata.hasSequenceID), sequenceID: \(metadata.sequenceID)")
      print("  hasPublishTime: \(metadata.hasPublishTime), publishTime: \(metadata.publishTime)")
      print("  compression: \(metadata.compression)")
      fatalError("Failed to serialize metadata in encoder: \(error)")
    }
    let metadataSize = UInt32(metadataData.count)
    let payload = frame.payload ?? ByteBuffer()

    // Build metadata + payload section for checksum calculation
    var metadataPayloadSection = ByteBufferAllocator().buffer(capacity: Int(metadataSize) + 4 + payload.readableBytes)
    metadataPayloadSection.writeInteger(metadataSize, endianness: .big)
    metadataPayloadSection.writeBytes(metadataData)
    if payload.readableBytes > 0 {
      var payloadCopy = payload
      metadataPayloadSection.writeBuffer(&payloadCopy)
    }

    // Calculate CRC32C checksum
    let checksum = calculateCRC32C(buffer: metadataPayloadSection)

    // Build complete frame: totalSize + commandSize + command + checksum + magicNumber + metadataSize + metadata + payload
    let totalSize = UInt32(4 + commandData.count + 4 + 2 + metadataPayloadSection.readableBytes)  // commandSize + command + checksum + magic + metadata+payload
    var buffer = ByteBufferAllocator().buffer(capacity: Int(totalSize) + 4)

    buffer.writeInteger(totalSize, endianness: .big)
    buffer.writeInteger(commandSize, endianness: .big)
    buffer.writeBytes(commandData)  
    buffer.writeBytes([0x0e, 0x01])  // Magic number
    buffer.writeInteger(checksum, endianness: .big)
    var metadataPayloadCopy = metadataPayloadSection
    buffer.writeBuffer(&metadataPayloadCopy)

    return buffer
  }

  /// Calculate CRC32C checksum (using Castagnoli polynomial)
  private func calculateCRC32C(buffer: ByteBuffer) -> UInt32 {
    var bufferCopy = buffer
    let bytes = bufferCopy.readBytes(length: bufferCopy.readableBytes) ?? []
    return CyclicRedundancyCheck.crc32c(bytes: bytes)
  }
}

/// Pulsar frame decoder
public struct PulsarFrameDecoder {
  public init() {}

  /// Decode a frame from bytes (matches C# DotPulsar protocol specification)
  public func decode(from buffer: inout ByteBuffer) throws -> PulsarFrame? {
    guard buffer.readableBytes >= 8 else { return nil }  // Need at least total size + command size

    // Save the reader index in case we need to reset
    let originalReaderIndex = buffer.readerIndex

    // Read total size
    guard let totalSize = buffer.readInteger(endianness: .big, as: UInt32.self) else {
      buffer.moveReaderIndex(to: originalReaderIndex)
      return nil
    }

    guard buffer.readableBytes >= Int(totalSize) else {
      buffer.moveReaderIndex(to: originalReaderIndex)
      return nil  // Not enough data for complete frame
    }

    // Read command size
    guard let commandSize = buffer.readInteger(endianness: .big, as: UInt32.self) else {
      buffer.moveReaderIndex(to: originalReaderIndex)
      return nil
    }

    // Read command
    guard let commandBytes = buffer.readBytes(length: Int(commandSize)) else {
      buffer.moveReaderIndex(to: originalReaderIndex)
      return nil
    }
    let commandData = Data(commandBytes)
    let command = try Pulsar_Proto_BaseCommand(serializedBytes: commandData)

    var metadata: Pulsar_Proto_MessageMetadata?
    var payload: ByteBuffer?

    // Check if this is a simple frame (command only) or complex frame (with metadata/payload)
    let remainingSize = Int(totalSize) - 4 - Int(commandSize)

    if remainingSize > 6 {  // Need at least magic(2) + checksum(4)
      // Complex frame: magic + checksum + metadata + payload

      // Read magic number (2 bytes)
      guard let magicBytes = buffer.readBytes(length: 2) else {
        buffer.moveReaderIndex(to: originalReaderIndex)
        return nil
      }
      guard magicBytes == [0x0e, 0x01] else {
        throw PulsarClientError.protocolError("Invalid magic number in frame")
      }

      // Read checksum (4 bytes)
      guard let checksum = buffer.readInteger(endianness: .big, as: UInt32.self) else {
        buffer.moveReaderIndex(to: originalReaderIndex)
        return nil
      }

      // Save position for checksum verification
      let checksumStartIndex = buffer.readerIndex

      // Read metadata size and metadata
      guard let metadataSize = buffer.readInteger(endianness: .big, as: UInt32.self) else {
        buffer.moveReaderIndex(to: originalReaderIndex)
        return nil
      }

      if metadataSize > 0 {
        // Read metadata using ByteBuffer directly
        guard let metadataSlice = buffer.readSlice(length: Int(metadataSize)) else {
          buffer.moveReaderIndex(to: originalReaderIndex)
          return nil
        }
        metadata = try Pulsar_Proto_MessageMetadata(serializedBytes: metadataSlice)

        // Read payload if present
        let payloadSize = remainingSize - 6 - 4 - Int(metadataSize)  // minus magic, checksum, metadata size, metadata
        if payloadSize > 0 {
          payload = buffer.readSlice(length: payloadSize)
        }

        // Verify checksum
        let endIndex = buffer.readerIndex
        let checksumLength = endIndex - checksumStartIndex
        let checksumSlice = buffer.getSlice(at: checksumStartIndex, length: checksumLength) ?? ByteBuffer()
        let calculatedChecksum = calculateCRC32C(buffer: checksumSlice)
        guard calculatedChecksum == checksum else {
          throw PulsarClientError.protocolError("Frame checksum mismatch")
        }
      }
    }

    return PulsarFrame(command: command, metadata: metadata, payload: payload)
  }

  /// Calculate CRC32C checksum (using Castagnoli polynomial)
  private func calculateCRC32C(buffer: ByteBuffer) -> UInt32 {
    var bufferCopy = buffer
    let bytes = bufferCopy.readBytes(length: bufferCopy.readableBytes) ?? []
    return CyclicRedundancyCheck.crc32c(bytes: bytes)
  }
}

/// Pulsar command builder
public final class PulsarCommandBuilder: @unchecked Sendable {
  private let lock = NSLock()
  private var requestId: UInt64 = 0
  private var producerId: UInt64 = 0
  private var consumerId: UInt64 = 0

  public init() {}

  private func nextRequestId() -> UInt64 {
    lock.lock()
    defer { lock.unlock() }
    requestId += 1
    return requestId
  }

  private func nextProducerId() -> UInt64 {
    lock.lock()
    defer { lock.unlock() }
    producerId += 1
    return producerId
  }

  public func nextConsumerId() -> UInt64 {
    lock.lock()
    defer { lock.unlock() }
    consumerId += 1
    return consumerId
  }

  /// Create CONNECT command
  public func connect(
    clientVersion: String = "PulsarClient-Swift/1.0.0",
    authMethodName: String? = nil,
    authData: ByteBuffer? = nil
  ) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .connect

    var connect = Pulsar_Proto_CommandConnect()
    connect.clientVersion = clientVersion
    connect.protocolVersion = PulsarProtocol.protocolVersion

    if let authMethodName = authMethodName {
      connect.authMethodName = authMethodName
    }

    if let authData = authData {
      var authDataCopy = authData
      let authBytes = authDataCopy.readBytes(length: authDataCopy.readableBytes) ?? []
      connect.authData = Data(authBytes)
    }

    command.connect = connect
    return command
  }

  /// Create PING command
  public func ping() -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .ping
    command.ping = Pulsar_Proto_CommandPing()
    return command
  }

  /// Create PONG command
  public func pong() -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .pong
    command.pong = Pulsar_Proto_CommandPong()
    return command
  }

  /// Create LOOKUP command
  public func lookup(topic: String, authoritative: Bool = false) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .lookup

    var lookup = Pulsar_Proto_CommandLookupTopic()
    lookup.topic = topic
    lookup.requestID = nextRequestId()
    lookup.authoritative = authoritative

    command.lookupTopic = lookup
    return command
  }

  /// Create PRODUCER command
  public func createProducer(
    topic: String,
    producerName: String? = nil,
    schema: SchemaInfo? = nil,
    initialSequenceId: UInt64? = nil,
    producerAccessMode: ProducerAccessMode = .shared,
    producerProperties: [String: String] = [:]
  ) -> (command: Pulsar_Proto_BaseCommand, producerId: UInt64) {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .producer

    var producer = Pulsar_Proto_CommandProducer()
    producer.topic = topic
    producer.producerID = nextProducerId()
    producer.requestID = nextRequestId()

    if let producerName = producerName {
      producer.producerName = producerName
    }

    if let schema = schema {
      var protoSchema = Pulsar_Proto_Schema()
      protoSchema.name = schema.name
      protoSchema.type = mapSchemaType(schema.type)
      if let schemaData = schema.schema {
        protoSchema.schemaData = schemaData
      }
      protoSchema.properties = schema.properties.map { key, value in
        var prop = Pulsar_Proto_KeyValue()
        prop.key = key
        prop.value = value
        return prop
      }
      producer.schema = protoSchema
    }

    if initialSequenceId != nil {
      // Note: initialSequenceID might be named differently in the proto
      // producer.initialSequenceID = initialSequenceId
    }

    // Set producer access mode
    producer.producerAccessMode = producerAccessMode.toProto()

    // Set producer properties
    if !producerProperties.isEmpty {
      producer.metadata = producerProperties.map { key, value in
        var kv = Pulsar_Proto_KeyValue()
        kv.key = key
        kv.value = value
        return kv
      }
    }

    command.producer = producer
    return (command, producer.producerID)
  }

  /// Create SUBSCRIBE command
  public func subscribe(
    topic: String,
    subscription: String,
    subType: SubscriptionType,
    consumerName: String? = nil,
    initialPosition: SubscriptionInitialPosition = .latest,
    schema: SchemaInfo? = nil,
    preAssignedConsumerId: UInt64? = nil
  ) -> (command: Pulsar_Proto_BaseCommand, consumerId: UInt64) {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .subscribe

    var subscribe = Pulsar_Proto_CommandSubscribe()
    subscribe.topic = topic
    subscribe.subscription = subscription
    subscribe.subType = mapSubscriptionType(subType)
    subscribe.consumerID = preAssignedConsumerId ?? nextConsumerId()
    subscribe.requestID = nextRequestId()

    if let consumerName = consumerName {
      subscribe.consumerName = consumerName
    }

    subscribe.initialPosition = mapInitialPosition(initialPosition)

    // Add missing fields that C# client includes (critical for message delivery)
    subscribe.priorityLevel = 0  // Default priority level
    subscribe.readCompacted = false  // Default to not reading compacted messages
    subscribe.replicateSubscriptionState = false  // Default replication setting

    if let schema = schema {
      var protoSchema = Pulsar_Proto_Schema()
      protoSchema.name = schema.name
      protoSchema.type = mapSchemaType(schema.type)
      if let schemaData = schema.schema {
        protoSchema.schemaData = schemaData
      }
      protoSchema.properties = schema.properties.map { key, value in
        var prop = Pulsar_Proto_KeyValue()
        prop.key = key
        prop.value = value
        return prop
      }
      subscribe.schema = protoSchema
    }

    command.subscribe = subscribe
    return (command, subscribe.consumerID)
  }

  /// Create FLOW command
  public func flow(consumerId: UInt64, messagePermits: UInt32) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .flow

    var flow = Pulsar_Proto_CommandFlow()
    flow.consumerID = consumerId
    flow.messagePermits = messagePermits

    command.flow = flow
    return command
  }

  /// Create ACK command
  public func ack(consumerId: UInt64, messageId: MessageId) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .ack

    var ack = Pulsar_Proto_CommandAck()
    ack.consumerID = consumerId
    ack.ackType = .individual

    let msgId = messageId.toProto()
    ack.messageID = [msgId]

    command.ack = ack
    return command
  }

  /// Create SEND command
  public func send(
    producerId: UInt64,
    sequenceId: UInt64,
    numMessages: Int32 = 1
  ) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .send

    var send = Pulsar_Proto_CommandSend()
    send.producerID = producerId
    send.sequenceID = sequenceId
    send.numMessages = numMessages

    command.send = send
    return command
  }

  /// Create SEEK command
  public func seek(consumerId: UInt64, messageId: MessageId? = nil, timestamp: Date? = nil)
    -> Pulsar_Proto_BaseCommand
  {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .seek

    var seek = Pulsar_Proto_CommandSeek()
    seek.consumerID = consumerId
    seek.requestID = nextRequestId()

    if let messageId = messageId {
      seek.messageID = messageId.toProto()
    } else if let timestamp = timestamp {
      seek.messagePublishTime = UInt64(timestamp.timeIntervalSince1970 * 1000)
    }

    command.seek = seek
    return command
  }

  /// Create UNSUBSCRIBE command
  public func unsubscribe(consumerId: UInt64) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .unsubscribe

    var unsub = Pulsar_Proto_CommandUnsubscribe()
    unsub.consumerID = consumerId
    unsub.requestID = nextRequestId()

    command.unsubscribe = unsub
    return command
  }

  /// Create CLOSE_CONSUMER command
  public func closeConsumer(consumerId: UInt64) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .closeConsumer

    var close = Pulsar_Proto_CommandCloseConsumer()
    close.consumerID = consumerId
    close.requestID = nextRequestId()

    command.closeConsumer = close
    return command
  }

  /// Create GET_LAST_MESSAGE_ID command
  public func getLastMessageId(consumerId: UInt64) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .getLastMessageID

    var getLastMsg = Pulsar_Proto_CommandGetLastMessageId()
    getLastMsg.consumerID = consumerId
    getLastMsg.requestID = nextRequestId()

    command.getLastMessageID = getLastMsg
    return command
  }

  /// Create CLOSE_PRODUCER command
  public func closeProducer(producerId: UInt64) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .closeProducer

    var close = Pulsar_Proto_CommandCloseProducer()
    close.producerID = producerId
    close.requestID = nextRequestId()

    command.closeProducer = close
    return command
  }

  /// Create AUTH_RESPONSE command
  public func authResponse(
    clientVersion: String = "PulsarClient-Swift/1.0.0",
    response: Pulsar_Proto_AuthData
  ) -> Pulsar_Proto_BaseCommand {
    var command = Pulsar_Proto_BaseCommand()
    command.type = .authResponse

    var authResponse = Pulsar_Proto_CommandAuthResponse()
    authResponse.clientVersion = clientVersion
    authResponse.response = response
    authResponse.protocolVersion = PulsarProtocol.protocolVersion

    command.authResponse = authResponse
    return command
  }

  /// Create message metadata
  public func createMessageMetadata(
    producerName: String,
    sequenceId: UInt64,
    publishTime: Date = Date(),
    properties: [String: String] = [:],
    compressionType: CompressionType = .none
  ) -> Pulsar_Proto_MessageMetadata {
    var metadata = Pulsar_Proto_MessageMetadata()
    metadata.producerName = producerName
    metadata.sequenceID = sequenceId
    metadata.publishTime = UInt64(publishTime.timeIntervalSince1970 * 1000)

    if !properties.isEmpty {
      metadata.properties = properties.map { key, value in
        var prop = Pulsar_Proto_KeyValue()
        prop.key = key
        prop.value = value
        return prop
      }
    }

    // Always set compression (required field)
    metadata.compression = mapCompressionType(compressionType)

    return metadata
  }

  /// Create message metadata from MessageMetadata struct
  public func createMessageMetadata(
    from messageMetadata: MessageMetadata,
    producerName: String,
    publishTime: Date = Date()
  ) -> Pulsar_Proto_MessageMetadata {
    var proto = messageMetadata.toProto()
    proto.producerName = producerName
    proto.publishTime = UInt64(publishTime.timeIntervalSince1970 * 1000)

    // Override sequence ID if not set
    if !proto.hasSequenceID, let sequenceId = messageMetadata.sequenceId {
      proto.sequenceID = sequenceId
    }

    return proto
  }

  // MARK: - Type Mapping

  private func mapSchemaType(_ type: SchemaType) -> Pulsar_Proto_Schema.TypeEnum {
    switch type {
    case .none: return .none
    case .string: return .string
    case .json: return .json
    case .protobuf: return .protobuf
    case .avro: return .avro
    case .boolean: return .bool
    case .int8: return .int8
    case .int16: return .int16
    case .int32: return .int32
    case .int64: return .int64
    case .float: return .float
    case .double: return .double
    case .date: return .date
    case .time: return .time
    case .timestamp: return .timestamp
    case .keyValue: return .keyValue
    case .bytes: return .none  // bytes type may not exist in proto
    case .auto: return .autoConsume
    case .autoConsume: return .autoConsume
    case .autoPublish: return .autoConsume  // autoPublish may not exist in proto
    }
  }

  private func mapSubscriptionType(_ type: SubscriptionType)
    -> Pulsar_Proto_CommandSubscribe.SubType
  {
    switch type {
    case .exclusive: return .exclusive
    case .shared: return .shared
    case .keyShared: return .keyShared
    case .failover: return .failover
    }
  }

  private func mapInitialPosition(_ position: SubscriptionInitialPosition)
    -> Pulsar_Proto_CommandSubscribe.InitialPosition
  {
    switch position {
    case .latest: return .latest
    case .earliest: return .earliest
    case .messageId, .timestamp:
      // For specific positions, we'd need to handle this differently
      return .latest
    }
  }

  private func mapCompressionType(_ type: CompressionType) -> Pulsar_Proto_CompressionType {
    switch type {
    case .none: return .none
    case .lz4: return .lz4
    case .zlib: return .zlib
    case .zstd: return .zstd
    case .snappy: return .snappy
    }
  }
}
