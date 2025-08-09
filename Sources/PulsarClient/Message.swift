import Foundation

/// Message received from Pulsar
public struct Message<T> {
  public let id: MessageId
  public let value: T
  public let metadata: MessageMetadata
  public let publishTime: Date
  public let producerName: String?
  public let replicatedFrom: String?
  public let topicName: String
  public let redeliveryCount: UInt32
  public let data: Data?

  // MARK: - Convenience Properties

  /// The key of the message.
  public var key: String? {
    return metadata.key
  }

  /// The properties of the message.
  public var properties: [String: String] {
    return metadata.properties
  }

  /// The event time of the message.
  public var eventTime: Date? {
    return metadata.eventTime
  }

  /// The sequence ID of the message.
  public var sequenceId: UInt64? {
    return metadata.sequenceId
  }

  /// The partition key of the message.
  public var partitionKey: String? {
    return metadata.key
  }

  /// The schema version of the message.
  public var schemaVersion: Data? {
    return metadata.schemaVersion
  }

  /// The ordering key of the message.
  public var orderingKey: Data? {
    return metadata.orderingKey
  }

  /// Check if the message has an event time.
  public var hasEventTime: Bool {
    return metadata.eventTime != nil
  }

  /// Check if the message has a key.
  public var hasKey: Bool {
    return metadata.key != nil
  }

  /// Check if the message has an ordering key.
  public var hasOrderingKey: Bool {
    return metadata.orderingKey != nil
  }

  /// Check if the message has a base64 encoded key.
  public var hasBase64EncodedKey: Bool {
    return metadata.keyBytes != nil
  }

  /// Get the key as bytes.
  public var keyBytes: Data? {
    return metadata.keyBytes
  }

  // MARK: - Initialization

  public init(
    id: MessageId,
    value: T,
    metadata: MessageMetadata,
    publishTime: Date,
    producerName: String? = nil,
    replicatedFrom: String? = nil,
    topicName: String,
    redeliveryCount: UInt32 = 0,
    data: Data? = nil
  ) {
    self.id = id
    self.value = value
    self.metadata = metadata
    self.publishTime = publishTime
    self.producerName = producerName
    self.replicatedFrom = replicatedFrom
    self.topicName = topicName
    self.redeliveryCount = redeliveryCount
    self.data = data
  }

  /// Legacy initializer for backward compatibility
  public init(
    id: MessageId,
    value: T,
    key: String? = nil,
    properties: [String: String] = [:],
    eventTime: Date? = nil,
    publishTime: Date,
    producerName: String? = nil,
    sequenceId: UInt64? = nil,
    replicatedFrom: String? = nil,
    partitionKey: String? = nil,
    schemaVersion: Data? = nil,
    topicName: String,
    redeliveryCount: UInt32 = 0,
    data: Data? = nil
  ) {
    self.id = id
    self.value = value
    self.metadata = MessageMetadata(
      key: key ?? partitionKey,
      properties: properties,
      eventTime: eventTime,
      sequenceId: sequenceId,
      schemaVersion: schemaVersion
    )
    self.publishTime = publishTime
    self.producerName = producerName
    self.replicatedFrom = replicatedFrom
    self.topicName = topicName
    self.redeliveryCount = redeliveryCount
    self.data = data
  }
}

extension Message: Sendable where T: Sendable {}
