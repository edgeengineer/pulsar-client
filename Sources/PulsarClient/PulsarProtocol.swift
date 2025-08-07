
import Foundation
import NIOCore
import SwiftProtobuf
import NIOCore

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
  public let payload: Data?

  public init(
    command: Pulsar_Proto_BaseCommand, metadata: Pulsar_Proto_MessageMetadata? = nil,
    payload: Data? = nil
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
      totalSize += UInt32(payload.count)
    }

    self.totalSize = totalSize
  }
}

/// Pulsar frame encoder
public struct PulsarFrameEncoder {
  public init() {}

  /// Encode a frame to bytes (matches C# DotPulsar protocol specification)
  public func encode(frame: PulsarFrame) throws -> Data {
    let commandData = try frame.command.serializedData()
    let commandSize = UInt32(commandData.count)

    // Simple frame (command only) - matches C# Serialize(BaseCommand command)
    if frame.metadata == nil && frame.payload == nil {
      var buffer = Data()
      let totalSize = commandSize + 4  // 4 bytes for command size

      buffer.append(contentsOf: withUnsafeBytes(of: totalSize.bigEndian) { Array($0) })
      buffer.append(contentsOf: withUnsafeBytes(of: commandSize.bigEndian) { Array($0) })
      buffer.append(commandData)

      return buffer
    }

    // Complex frame (with metadata/payload) - matches C# Serialize(BaseCommand, MessageMetadata, ReadOnlySequence<byte>)
    guard let metadata = frame.metadata else {
      throw PulsarClientError.protocolError("Metadata required for message frames")
    }

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
    let payload = frame.payload ?? Data()

    // Build metadata + payload section for checksum calculation
    var metadataPayloadSection = Data()
    metadataPayloadSection.append(
      contentsOf: withUnsafeBytes(of: metadataSize.bigEndian) { Array($0) })
    metadataPayloadSection.append(metadataData)
    metadataPayloadSection.append(payload)

    // Calculate CRC32C checksum
    let checksum = calculateCRC32C(data: metadataPayloadSection)

    // Build complete frame: totalSize + commandSize + command + checksum + magicNumber + metadataSize + metadata + payload
    var buffer = Data()
    let totalSize = UInt32(4 + commandData.count + 4 + 2 + metadataPayloadSection.count)  // commandSize + command + checksum + magic + metadata+payload

    buffer.append(contentsOf: withUnsafeBytes(of: totalSize.bigEndian) { Array($0) })
    buffer.append(contentsOf: withUnsafeBytes(of: commandSize.bigEndian) { Array($0) })
    buffer.append(commandData)
    buffer.append(contentsOf: [0x0e, 0x01])  // Magic number
    buffer.append(contentsOf: withUnsafeBytes(of: checksum.bigEndian) { Array($0) })
    buffer.append(metadataPayloadSection)

    return buffer
  }

  /// Calculate CRC32C checksum (using Castagnoli polynomial)
  private func calculateCRC32C(data: Data) -> UInt32 {
    return CyclicRedundancyCheck.crc32c(bytes: data)
  }
}

/// Pulsar frame decoder
public struct PulsarFrameDecoder {
  public init() {}

  /// Decode a frame from bytes (matches C# DotPulsar protocol specification)
  public func decode(from data: Data) throws -> PulsarFrame? {
    guard data.count >= 8 else { return nil }  // Need at least total size + command size

    // Read total size
    let totalSize = data.subdata(in: 0..<4).withUnsafeBytes { bytes in
      UInt32(bigEndian: bytes.load(as: UInt32.self))
    }

    guard data.count >= totalSize + 4 else { return nil }  // Not enough data for complete frame

    // Read command size
    let commandSize = data.subdata(in: 4..<8).withUnsafeBytes { bytes in
      UInt32(bigEndian: bytes.load(as: UInt32.self))
    }

    // Read command
    let commandData = data.subdata(in: 8..<(8 + Int(commandSize)))
    let command = try Pulsar_Proto_BaseCommand(serializedBytes: commandData)

    var offset = 8 + Int(commandSize)
    var metadata: Pulsar_Proto_MessageMetadata?
    var payload: Data?

    // Check if this is a simple frame (command only) or complex frame (with metadata/payload)
    let remainingSize = Int(totalSize) + 4 - offset

    if remainingSize > 6 {  // Need at least checksum(4) + magic(2)
      // Complex frame: checksum + magic + metadata + payload

      // Read magic number (2 bytes)
      guard offset + 2 <= data.count else { return nil }
      let magic = data.subdata(in: offset..<(offset + 2))
      guard magic == Data([0x0e, 0x01]) else {
        throw PulsarClientError.protocolError("Invalid magic number in frame")
      }
      offset += 2

      // Read checksum (4 bytes)
      guard offset + 4 <= data.count else { return nil }
      let checksum = data.subdata(in: offset..<(offset + 4)).withUnsafeBytes { bytes in
        UInt32(bigEndian: bytes.load(as: UInt32.self))
      }
      offset += 4

      // Read metadata size and metadata
      guard offset + 4 <= data.count else { return nil }
      let metadataSize = data.subdata(in: offset..<(offset + 4)).withUnsafeBytes { bytes in
        UInt32(bigEndian: bytes.load(as: UInt32.self))
      }
      offset += 4

      if metadataSize > 0 && offset + Int(metadataSize) <= data.count {
        // Read metadata
        let metadataData = data.subdata(in: offset..<(offset + Int(metadataSize)))
        metadata = try Pulsar_Proto_MessageMetadata(serializedBytes: metadataData)
        offset += Int(metadataSize)

        // Read payload if present
        let payloadSize = Int(totalSize) + 4 - offset
        if payloadSize > 0 {
          payload = data.subdata(in: offset..<(offset + payloadSize))
        }

        // Verify checksum
        let metadataPayloadSection = data.subdata(
          in: (8 + Int(commandSize) + 6)..<(Int(totalSize) + 4))
        let calculatedChecksum = calculateCRC32C(data: metadataPayloadSection)
        guard calculatedChecksum == checksum else {
          throw PulsarClientError.protocolError("Frame checksum mismatch")
        }
      }
    }

    return PulsarFrame(command: command, metadata: metadata, payload: payload)
  }

  /// Calculate CRC32C checksum (using Castagnoli polynomial)
  private func calculateCRC32C(data: Data) -> UInt32 {
    return CyclicRedundancyCheck.crc32c(bytes: data)
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
    authData: Data? = nil
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
      connect.authData = authData
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
