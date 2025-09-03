import Foundation
import Logging
import NIOCore

/// Channel state
enum ChannelState: Sendable {
  case inactive
  case active
  case closing
  case closed
}

/// Base channel protocol
protocol PulsarChannel: Actor {
  var id: UInt64 { get }
  var state: ChannelState { get }
  func close() async
}

/// Producer channel
actor ProducerChannel: PulsarChannel {
  let id: UInt64
  let topic: String
  let producerName: String?
  private(set) var state: ChannelState = .inactive
  internal weak var connection: Connection?

  // Send receipt handling - store a type-erased wrapper
  private var pendingSends: [UInt64: SendOperationWrapper] = [:]

  // Store schema info for reconnection
  private var schemaInfo: SchemaInfo?

  init(
    id: UInt64, topic: String, producerName: String?, connection: Connection,
    schemaInfo: SchemaInfo? = nil
  ) {
    self.id = id
    self.topic = topic
    self.producerName = producerName
    self.connection = connection
    self.schemaInfo = schemaInfo
  }

  func activate() {
    state = .active
  }

  func updateState(_ newState: ChannelState) {
    state = newState
  }

  /// Register a send operation (non-blocking, like C#)
  func registerSendOperation<T>(_ operation: SendOperation<T>) {
    logger.debug(
      "Registering send operation for sequence \(operation.sequenceId), total pending: \(pendingSends.count + 1)"
    )
    let wrapper = AnySendOperationWrapper(operation)
    pendingSends[operation.sequenceId] = wrapper
  }

  /// Handle incoming send receipt
  func handleSendReceipt(_ receipt: Pulsar_Proto_CommandSendReceipt) {
    if let wrapper = pendingSends.removeValue(forKey: receipt.sequenceID) {
      let messageId = MessageId(
        ledgerId: receipt.messageID.ledgerID,
        entryId: receipt.messageID.entryID,
        partition: receipt.messageID.hasPartition ? receipt.messageID.partition : -1,
        batchIndex: receipt.messageID.hasBatchIndex ? receipt.messageID.batchIndex : -1
      )
      logger.debug(
        "Completed send operation for sequence \(receipt.sequenceID), messageId: \(messageId), remaining: \(pendingSends.count)"
      )

      // Complete the operation
      wrapper.complete(with: messageId)
    } else {
      logger.debug(
        "Received send receipt for unknown sequence ID", metadata: ["sequenceId": "\(receipt.sequenceID)", "pendingSequences": "\(pendingSends.keys.sorted())"]
      )
    }
  }

  func close() async {
    guard state == .active else { return }
    state = .closing

    // Cancel all pending sends
    for (_, wrapper) in pendingSends {
      wrapper.fail(with: PulsarClientError.producerBusy("Producer closing"))
    }
    pendingSends.removeAll()

    state = .closed
  }

  /// Set schema info for reconnection
  func setSchemaInfo(_ schemaInfo: SchemaInfo?) {
    self.schemaInfo = schemaInfo
  }
  
  /// Handle connection failure
  func handleConnectionFailure(_ error: Error) async {
    logger.error("Producer handling connection failure", metadata: [
      "producerId": "\(id)",
      "topic": "\(topic)",
      "error": "\(error)"
    ])
    
    // Fail all pending sends
    for (sequenceId, wrapper) in pendingSends {
      logger.debug("Failing pending send", metadata: ["sequenceId": "\(sequenceId)"])
      wrapper.fail(with: error)
    }
    pendingSends.removeAll()
    
    // Update state
    state = .inactive
  }

  /// Get schema info
  func getSchemaInfo() -> SchemaInfo? {
    return schemaInfo
  }
}

/// Consumer channel
actor ConsumerChannel: PulsarChannel {
  let id: UInt64
  let topic: String
  let subscription: String
  let consumerName: String?
  private(set) var state: ChannelState = .inactive
  internal weak var connection: Connection?
  private let logger = Logger(label: "ConsumerChannel")

  // Message delivery callback
  internal var messageHandler:
    ((Pulsar_Proto_CommandMessage, ByteBuffer, Pulsar_Proto_MessageMetadata) async -> Void)?

  // Per-consumer inbound FIFO dispatcher state
  private typealias InboundMessage = (
    msg: Pulsar_Proto_CommandMessage,
    payload: ByteBuffer,
    metadata: Pulsar_Proto_MessageMetadata
  )
  private var inboundBuffer: [InboundMessage] = []
  private var inboundWaiters: [CheckedContinuation<InboundMessage?, Never>] = []
  private var dispatchTask: Task<Void, Never>?
  private var inboundClosed = false

  // Store subscription configuration for reconnection
  private var subscriptionType: SubscriptionType = .exclusive
  private var initialPosition: SubscriptionInitialPosition = .latest
  private var schemaInfo: SchemaInfo?

  init(
    id: UInt64, topic: String, subscription: String, consumerName: String?, connection: Connection,
    subscriptionType: SubscriptionType = .exclusive,
    initialPosition: SubscriptionInitialPosition = .latest,
    schemaInfo: SchemaInfo? = nil
  ) {
    self.id = id
    self.topic = topic
    self.subscription = subscription
    self.consumerName = consumerName
    self.connection = connection
    self.subscriptionType = subscriptionType
    self.initialPosition = initialPosition
    self.schemaInfo = schemaInfo
  }

  func activate() {
    state = .active
    startDispatcherIfNeeded()
  }

  func updateState(_ newState: ChannelState) {
    state = newState
  }

  /// Set the message handler callback
  func setMessageHandler(
    _ handler: @escaping (Pulsar_Proto_CommandMessage, ByteBuffer, Pulsar_Proto_MessageMetadata) async ->
      Void
  ) {
    self.messageHandler = handler
    startDispatcherIfNeeded()
  }

  func close() async {
    guard state == .active else { return }
    state = .closing

    // Clear message handler
    messageHandler = nil

    // Close inbound queue and cancel dispatcher
    inboundClosed = true
    for waiter in inboundWaiters { waiter.resume(returning: nil) }
    inboundWaiters.removeAll()
    dispatchTask?.cancel()
    dispatchTask = nil

    state = .closed
  }
  
  /// Handle connection failure
  func handleConnectionFailure(_ error: Error) async {
    logger.error("Consumer handling connection failure", metadata: [
      "consumerId": "\(id)",
      "topic": "\(topic)",
      "subscription": "\(subscription)",
      "error": "\(error)"
    ])
    
    // Clear message handler
    messageHandler = nil
    
    // Close the inbound stream
    inboundClosed = true
    
    // Resume all waiters with nil to signal closure
    for waiter in inboundWaiters {
      waiter.resume(returning: nil)
    }
    inboundWaiters.removeAll()
    
    // Clear buffer
    inboundBuffer.removeAll()
    
    // Cancel dispatcher
    dispatchTask?.cancel()
    dispatchTask = nil
    
    // Update state
    state = .inactive
  }

  /// Set subscription configuration
  func setSubscriptionConfig(type: SubscriptionType, initialPosition: SubscriptionInitialPosition) {
    self.subscriptionType = type
    self.initialPosition = initialPosition
  }

  /// Set schema info for reconnection
  func setSchemaInfo(_ schemaInfo: SchemaInfo?) {
    self.schemaInfo = schemaInfo
  }

  /// Get subscription configuration
  func getSubscriptionConfig() -> (
    type: SubscriptionType, initialPosition: SubscriptionInitialPosition, schemaInfo: SchemaInfo?
  ) {
    return (subscriptionType, initialPosition, schemaInfo)
  }

  func enqueueInbound(
    message: Pulsar_Proto_CommandMessage,
    payload: ByteBuffer,
    metadata: Pulsar_Proto_MessageMetadata
  ) async {
    guard state == .active else { return }
    let item: InboundMessage = (message, payload, metadata)
    if let waiter = inboundWaiters.first {
      inboundWaiters.removeFirst()
      waiter.resume(returning: item)
    } else {
      inboundBuffer.append(item)
    }
    startDispatcherIfNeeded()
  }

  private func nextInbound() async -> InboundMessage? {
    if inboundClosed { return nil }
    if let item = inboundBuffer.first {
      inboundBuffer.removeFirst()
      return item
    }
    if inboundClosed { return nil }
    return await withCheckedContinuation { (c: CheckedContinuation<InboundMessage?, Never>) in
      inboundWaiters.append(c)
    }
  }

  private func startDispatcherIfNeeded() {
    guard dispatchTask == nil, state == .active else { return }
    guard messageHandler != nil else { return }
    dispatchTask = Task { [weak self] in
      while let self = self, !Task.isCancelled {
        guard let item = await self.nextInbound() else { break }
        await self.dispatchItem(item)
      }
    }
  }

  private func dispatchItem(_ item: InboundMessage) async {
    if let handler = self.messageHandler {
      await handler(item.msg, item.payload, item.metadata)
    }
  }
}

// Add logger
private let logger = Logger(label: "ProducerChannel")

/// Channel manager for a connection
actor ChannelManager {
  private let logger: Logger
  internal var producers: [UInt64: ProducerChannel] = [:]
  internal var consumers: [UInt64: ConsumerChannel] = [:]

  init(logger: Logger = Logger(label: "ChannelManager")) {
    self.logger = logger
  }

  /// Get count of active producers
  func getProducerCount() -> Int {
    return producers.count
  }

  /// Get count of active consumers
  func getConsumerCount() -> Int {
    return consumers.count
  }

  /// Register a producer channel
  func registerProducer(_ channel: ProducerChannel) {
    producers[channel.id] = channel
    logger.debug("Registered producer", metadata: ["producerId": "\(channel.id)", "topic": "\(channel.topic)"])
  }

  /// Register a consumer channel
  func registerConsumer(_ channel: ConsumerChannel) {
    consumers[channel.id] = channel
    logger.debug("Registered consumer", metadata: ["consumerId": "\(channel.id)", "topic": "\(channel.topic)"])
  }

  /// Get producer by ID
  func getProducer(id: UInt64) -> ProducerChannel? {
    producers[id]
  }

  /// Get consumer by ID
  func getConsumer(id: UInt64) -> ConsumerChannel? {
    consumers[id]
  }

  /// Remove producer
  func removeProducer(id: UInt64) {
    if let producer = producers.removeValue(forKey: id) {
      logger.debug("Removed producer", metadata: ["producerId": "\(id)", "topic": "\(producer.topic)"])
    }
  }

  /// Remove consumer
  func removeConsumer(id: UInt64) {
    if let consumer = consumers.removeValue(forKey: id) {
      logger.debug("Removed consumer", metadata: ["consumerId": "\(id)", "topic": "\(consumer.topic)"])
    }
  }

  /// Close all channels
  func closeAll() async {
    // Close all producers
    await withTaskGroup(of: Void.self) { group in
      for (_, producer) in producers {
        group.addTask {
          await producer.close()
        }
      }
    }
    producers.removeAll()

    // Close all consumers
    await withTaskGroup(of: Void.self) { group in
      for (_, consumer) in consumers {
        group.addTask {
          await consumer.close()
        }
      }
    }
    consumers.removeAll()
  }
  
  /// Handle connection failure by notifying all channels
  func handleConnectionFailure(error: Error) async {
    logger.error("Handling connection failure for all channels", metadata: ["error": "\(error)"])
    
    // Notify all producers
    await withTaskGroup(of: Void.self) { group in
      for (id, producer) in producers {
        group.addTask {
          self.logger.debug("Notifying producer of connection failure", metadata: ["producerId": "\(id)"])
          await producer.handleConnectionFailure(error)
        }
      }
    }
    
    // Notify all consumers
    await withTaskGroup(of: Void.self) { group in
      for (id, consumer) in consumers {
        group.addTask {
          self.logger.debug("Notifying consumer of connection failure", metadata: ["consumerId": "\(id)"])
          await consumer.handleConnectionFailure(error)
        }
      }
    }
  }
}
