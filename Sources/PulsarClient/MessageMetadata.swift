/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation

/// The message metadata.
public struct MessageMetadata: Sendable {

  // MARK: - Properties

  /// The key of the message as a string.
  public var key: String?

  /// The key of the message as bytes.
  public var keyBytes: Data?

  /// The ordering key of the message.
  public var orderingKey: Data?

  /// The properties of the message.
  public var properties: [String: String]

  /// The event time of the message.
  public var eventTime: Date?

  /// The sequence id of the message.
  public var sequenceId: UInt64?

  /// The schema version of the message.
  public var schemaVersion: Data?

  /// The replication clusters for this message.
  public var replicationClusters: [String]?

  /// Whether to disable replication for this message.
  public var disableReplication: Bool

  /// The deliver after delay for this message.
  public var deliverAfter: TimeInterval?

  /// The deliver at time for this message.
  public var deliverAt: Date?

  /// The compression type and uncompressed size.
  public var compressionInfo: CompressionInfo?

  /// Partition key hash for routing (internal use).
  public var partitionKeyHash: UInt32?

  /// Whether this message is part of a batch.
  public var isPartOfBatch: Bool

  // MARK: - Initialization

  public init(
    key: String? = nil,
    keyBytes: Data? = nil,
    orderingKey: Data? = nil,
    properties: [String: String] = [:],
    eventTime: Date? = nil,
    sequenceId: UInt64? = nil,
    schemaVersion: Data? = nil,
    replicationClusters: [String]? = nil,
    disableReplication: Bool = false,
    deliverAfter: TimeInterval? = nil,
    deliverAt: Date? = nil,
    compressionInfo: CompressionInfo? = nil,
    partitionKeyHash: UInt32? = nil,
    isPartOfBatch: Bool = false
  ) {
    self.key = key
    self.keyBytes = keyBytes
    self.orderingKey = orderingKey
    self.properties = properties
    self.eventTime = eventTime
    self.sequenceId = sequenceId
    self.schemaVersion = schemaVersion
    self.replicationClusters = replicationClusters
    self.disableReplication = disableReplication
    self.deliverAfter = deliverAfter
    self.deliverAt = deliverAt
    self.compressionInfo = compressionInfo
    self.partitionKeyHash = partitionKeyHash
    self.isPartOfBatch = isPartOfBatch
  }

  // MARK: - Computed Properties

  /// The event time as unix time in milliseconds.
  public var eventTimeMillis: UInt64? {
    get {
      guard let eventTime = eventTime else { return nil }
      return UInt64(eventTime.timeIntervalSince1970 * 1000)
    }
    set {
      if let millis = newValue {
        eventTime = Date(timeIntervalSince1970: TimeInterval(millis) / 1000)
      } else {
        eventTime = nil
      }
    }
  }

  /// The deliver at time as unix time in milliseconds.
  public var deliverAtTimeMillis: Int64? {
    get {
      guard let deliverAt = deliverAt else { return nil }
      return Int64(deliverAt.timeIntervalSince1970 * 1000)
    }
    set {
      if let millis = newValue {
        deliverAt = Date(timeIntervalSince1970: TimeInterval(millis) / 1000)
      } else {
        deliverAt = nil
      }
    }
  }

  /// The deliver after delay in milliseconds.
  public var deliverAfterMillis: Int64? {
    get {
      guard let deliverAfter = deliverAfter else { return nil }
      return Int64(deliverAfter * 1000)
    }
    set {
      if let millis = newValue {
        deliverAfter = TimeInterval(millis) / 1000
      } else {
        deliverAfter = nil
      }
    }
  }

  // MARK: - Property Access

  /// Get or set a property value.
  public subscript(key: String) -> String? {
    get {
      return properties[key]
    }
    set {
      if let value = newValue {
        properties[key] = value
      } else {
        properties.removeValue(forKey: key)
      }
    }
  }

  // MARK: - Builder Pattern Methods (Functional Style)

  /// Returns a new MessageMetadata with the key set.
  public func withKey(_ key: String?) -> MessageMetadata {
    var copy = self
    copy.key = key
    return copy
  }

  /// Returns a new MessageMetadata with the key bytes set.
  public func withKeyBytes(_ keyBytes: Data?) -> MessageMetadata {
    var copy = self
    copy.keyBytes = keyBytes
    return copy
  }

  /// Returns a new MessageMetadata with the ordering key set.
  public func withOrderingKey(_ orderingKey: Data?) -> MessageMetadata {
    var copy = self
    copy.orderingKey = orderingKey
    return copy
  }

  /// Returns a new MessageMetadata with a property set.
  public func withProperty(_ key: String, _ value: String) -> MessageMetadata {
    var copy = self
    copy.properties[key] = value
    return copy
  }

  /// Returns a new MessageMetadata with properties set.
  public func withProperties(_ properties: [String: String]) -> MessageMetadata {
    var copy = self
    copy.properties = properties
    return copy
  }

  /// Returns a new MessageMetadata with the event time set.
  public func withEventTime(_ eventTime: Date?) -> MessageMetadata {
    var copy = self
    copy.eventTime = eventTime
    return copy
  }

  /// Returns a new MessageMetadata with the sequence ID set.
  public func withSequenceId(_ sequenceId: UInt64?) -> MessageMetadata {
    var copy = self
    copy.sequenceId = sequenceId
    return copy
  }

  /// Returns a new MessageMetadata with the schema version set.
  public func withSchemaVersion(_ schemaVersion: Data?) -> MessageMetadata {
    var copy = self
    copy.schemaVersion = schemaVersion
    return copy
  }

  /// Returns a new MessageMetadata with replication clusters set.
  public func withReplicationClusters(_ clusters: [String]?) -> MessageMetadata {
    var copy = self
    copy.replicationClusters = clusters
    return copy
  }

  /// Returns a new MessageMetadata with disable replication set.
  public func withDisableReplication(_ disable: Bool) -> MessageMetadata {
    var copy = self
    copy.disableReplication = disable
    return copy
  }

  /// Returns a new MessageMetadata with deliver after set.
  public func withDeliverAfter(_ delay: TimeInterval?) -> MessageMetadata {
    var copy = self
    copy.deliverAfter = delay
    return copy
  }

  /// Returns a new MessageMetadata with deliver at set.
  public func withDeliverAt(_ time: Date?) -> MessageMetadata {
    var copy = self
    copy.deliverAt = time
    return copy
  }

  /// Returns a new MessageMetadata with compression info set.
  public func withCompressionInfo(_ info: CompressionInfo?) -> MessageMetadata {
    var copy = self
    copy.compressionInfo = info
    return copy
  }

  /// Returns a new MessageMetadata with compression type and uncompressed size.
  public func withCompression(type: Pulsar_Proto_CompressionType, uncompressedSize: UInt32)
    -> MessageMetadata
  {
    var copy = self
    copy.compressionInfo = CompressionInfo(type: type, uncompressedSize: uncompressedSize)
    return copy
  }
}

// MARK: - Compression Info

/// Information about message compression.
public struct CompressionInfo: Sendable {
  /// The compression type.
  public let type: Pulsar_Proto_CompressionType

  /// The uncompressed size in bytes.
  public let uncompressedSize: UInt32

  public init(type: Pulsar_Proto_CompressionType, uncompressedSize: UInt32) {
    self.type = type
    self.uncompressedSize = uncompressedSize
  }
}

// MARK: - Extensions

extension MessageMetadata {
  /// Convert to Protocol Buffer metadata.
  internal func toProto() -> Pulsar_Proto_MessageMetadata {
    var proto = Pulsar_Proto_MessageMetadata()

    if let key = key {
      proto.partitionKey = key
      proto.partitionKeyB64Encoded = false
    } else if let keyBytes = keyBytes {
      proto.partitionKey = keyBytes.base64EncodedString()
      proto.partitionKeyB64Encoded = true
    }

    if let orderingKey = orderingKey {
      proto.orderingKey = orderingKey
    }

    if !properties.isEmpty {
      proto.properties = properties.map { key, value in
        var kv = Pulsar_Proto_KeyValue()
        kv.key = key
        kv.value = value
        return kv
      }
    }

    if let eventTimeMillis = eventTimeMillis {
      proto.eventTime = eventTimeMillis
    }

    if let sequenceId = sequenceId {
      proto.sequenceID = sequenceId
    }

    if let schemaVersion = schemaVersion {
      proto.schemaVersion = schemaVersion
    }

    if let replicationClusters = replicationClusters {
      proto.replicateTo = replicationClusters
    }

    if disableReplication {
      proto.replicatedFrom = ""  // Empty string indicates disabled replication
    }

    if let deliverAtTimeMillis = deliverAtTimeMillis {
      proto.deliverAtTime = deliverAtTimeMillis
    }

    if let compressionInfo = compressionInfo {
      switch compressionInfo.type {
      case .none:
        proto.compression = .none
      case .lz4:
        proto.compression = .lz4
      case .zlib:
        proto.compression = .zlib
      case .zstd:
        proto.compression = .zstd
      case .snappy:
        proto.compression = .snappy
      }
      proto.uncompressedSize = compressionInfo.uncompressedSize
    } else {
      // Always set compression to avoid issues (default to none if not specified)
      proto.compression = .none
    }

    // Note: producer_name, sequence_id, and publish_time are required fields
    // but they should be set by the caller since MessageMetadata doesn't have these values

    return proto
  }

  /// Create from Protocol Buffer metadata.
  internal init(from proto: Pulsar_Proto_MessageMetadata) {
    if proto.hasPartitionKey {
      if proto.partitionKeyB64Encoded {
        self.keyBytes = Data(base64Encoded: proto.partitionKey)
        self.key = nil
      } else {
        self.key = proto.partitionKey
        self.keyBytes = nil
      }
    } else {
      self.key = nil
      self.keyBytes = nil
    }

    if proto.hasOrderingKey {
      self.orderingKey = proto.orderingKey
    } else {
      self.orderingKey = nil
    }

    self.properties = Dictionary(uniqueKeysWithValues: proto.properties.map { ($0.key, $0.value) })

    if proto.hasEventTime {
      self.eventTime = Date(timeIntervalSince1970: TimeInterval(proto.eventTime) / 1000)
    } else {
      self.eventTime = nil
    }

    if proto.hasSequenceID {
      self.sequenceId = proto.sequenceID
    } else {
      self.sequenceId = nil
    }

    if proto.hasSchemaVersion {
      self.schemaVersion = proto.schemaVersion
    } else {
      self.schemaVersion = nil
    }

    if !proto.replicateTo.isEmpty {
      self.replicationClusters = proto.replicateTo
    } else {
      self.replicationClusters = nil
    }

    // Check if replication is disabled
    self.disableReplication = proto.hasReplicatedFrom && proto.replicatedFrom.isEmpty

    if proto.hasDeliverAtTime {
      self.deliverAt = Date(timeIntervalSince1970: TimeInterval(proto.deliverAtTime) / 1000)
      self.deliverAfter = nil
    } else {
      self.deliverAt = nil
      self.deliverAfter = nil
    }

    if proto.hasCompression && proto.compression != .none {
      let compressionType: CompressionType
      switch proto.compression {
      case .lz4:
        compressionType = .lz4
      case .zlib:
        compressionType = .zlib
      case .zstd:
        compressionType = .zstd
      case .snappy:
        compressionType = .snappy
      default:
        compressionType = .none
      }
      self.compressionInfo = CompressionInfo(
        type: compressionType,
        uncompressedSize: proto.hasUncompressedSize ? proto.uncompressedSize : 0
      )
    } else {
      self.compressionInfo = nil
    }

    self.partitionKeyHash = nil
    self.isPartOfBatch = false
  }
}
