import Foundation
import Logging
import NIOCore

/// Producer implementation with fault tolerance
actor ProducerImpl<T>: ProducerProtocol where T: Sendable {
  typealias MessageType = T
  private let id: UInt64
  private let producerName: String
  private let connection: Connection
  private let schema: Schema<T>
  private let configuration: ProducerOptions<T>
  private let logger: Logger
  private weak var tracker: ClientTracker?

  private var sequenceId: UInt64
  private var _state: ClientState = .connected
  internal let stateStream: AsyncStream<ClientState>
  private let stateContinuation: AsyncStream<ClientState>.Continuation

  private let batchBuilder: MessageBatchBuilder<T>?
  private var sendTask: Task<Void, Never>?

  // Queue-based sending (like C# implementation)
  private let sendQueue: SendQueue<T>
  private var dispatcherTask: Task<Void, Never>?

  // Serial dispatch queue for maintaining FIFO message ordering
  private let sendOrderQueue = DispatchQueue(label: "sendOrder", qos: .userInitiated)

  // Fault tolerance components
  private let retryExecutor: RetryExecutor
  private let stateManager: ProducerStateManager
  private let exceptionHandler: ExceptionHandler
  
  // Interceptors
  private let interceptors: ProducerInterceptors<T>?

  public let topic: String
  public nonisolated var state: ClientState {
    ClientState.connected  // Simple fallback for nonisolated access
  }
  public nonisolated var stateChanges: AsyncStream<ClientState> { stateStream }

  init(
    id: UInt64,
    topic: String,
    producerName: String?,
    connection: Connection,
    schema: Schema<T>,
    configuration: ProducerOptions<T>,
    logger: Logger,
    channelManager: ChannelManager,
    tracker: ClientTracker?
  ) {
    self.id = id
    self.topic = topic
    self.producerName = producerName ?? "producer-\(id)"
    self.connection = connection
    self.schema = schema
    self.configuration = configuration
    self.logger = logger
    self.tracker = tracker
    self.sequenceId = configuration.initialSequenceId

    (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()

    // Initialize send queue
    // If maxPendingMessages is 0, use unlimited (default to 1000)
    let queueSize =
      configuration.maxPendingMessages == 0 ? 1000 : Int(configuration.maxPendingMessages)
    self.sendQueue = SendQueue(maxSize: queueSize)

    // Initialize fault tolerance components
    self.retryExecutor = RetryExecutor(logger: logger)
    self.stateManager = StateManagerFactory.createProducerStateManager(
      initialState: .connected,
      componentName: "Producer-\(String(describing: producerName))"
    )
    self.exceptionHandler = DefaultExceptionHandler(logger: logger)

    // Initialize interceptors if configured
    if !configuration.interceptors.isEmpty {
      self.interceptors = ProducerInterceptors(interceptors: configuration.interceptors)
    } else {
      self.interceptors = nil
    }
    
    // Initialize batch builder if batching is enabled
    if configuration.batchingEnabled {
      self.batchBuilder = MessageBatchBuilder(
        maxMessages: Int(configuration.batchingMaxMessages),
        maxBytes: Int(configuration.batchingMaxBytes),
        maxDelay: configuration.batchingMaxDelay
      )
      // Producer reference will be set after initialization
    } else {
      self.batchBuilder = nil
    }
  }

  func startBatchSender() {
    // Start the message dispatcher (like C# MessageDispatcher)
    dispatcherTask = Task { [weak self] in
      await self?.runMessageDispatcher()
    }

    if configuration.batchingEnabled {
      // Set producer reference in batch builder
      Task { [weak self] in
        guard let self = self else { return }
        await self.batchBuilder?.setProducer(self)
      }

      self.sendTask = Task { [weak self] in
        await self?.runBatchSender()
      }
    }
  }

  deinit {
    stateContinuation.finish()
    sendTask?.cancel()
    dispatcherTask?.cancel()
  }

  // MARK: - StateHolder

  public nonisolated func onStateChange(_ handler: @escaping @Sendable (ClientState) -> Void) {
    Task { [weak self] in
      guard let self = self else { return }
      for await state in self.stateStream {
        handler(state)
      }
    }
  }

  public nonisolated func isFinal() -> Bool {
    false  // Conservative approach for nonisolated access
  }

  public nonisolated func handleException(_ error: any Error) {
    Task { [weak self] in
      await self?.handleError(error)
    }
  }

  public func stateChangedTo(_ state: ClientState, timeout: TimeInterval) async throws
    -> ClientState
  {
    if _state == state {
      return _state
    }

    return try await withThrowingTaskGroup(of: ClientState.self) { group in
      // Add timeout task
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        throw PulsarClientError.timeout("Timeout waiting for state change to \(state)")
      }

      // Add state monitoring task
      group.addTask { [weak self] in
        guard let self = self else { throw PulsarClientError.clientClosed }

        for await currentState in self.stateStream {
          if currentState == state {
            return currentState
          }
        }
        throw PulsarClientError.clientClosed
      }

      let result = try await group.next()!
      group.cancelAll()
      return result
    }
  }

  public func stateChangedFrom(_ state: ClientState, timeout: TimeInterval) async throws
    -> ClientState
  {
    if _state != state {
      return _state
    }

    return try await withThrowingTaskGroup(of: ClientState.self) { group in
      // Add timeout task
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        throw PulsarClientError.timeout("Timeout waiting for state change from \(state)")
      }

      // Add state monitoring task
      group.addTask { [weak self] in
        guard let self = self else { throw PulsarClientError.clientClosed }

        for await currentState in self.stateStream {
          if currentState != state {
            return currentState
          }
        }
        throw PulsarClientError.clientClosed
      }

      let result = try await group.next()!
      group.cancelAll()
      return result
    }
  }

  // MARK: - ProducerProtocol

  public nonisolated var isConnected: Bool {
    true  // Conservative approach for nonisolated access
  }

  public func send(_ message: T) async throws -> MessageId {
    try await send(message, metadata: MessageMetadata())
  }

   public func send(_ message: T, metadata: MessageMetadata) async throws -> MessageId {
      guard _state == .connected else {
        throw PulsarClientError.producerBusy("Producer not connected")
      }
     
     // Create a Message object for interceptors
      var msg = Message(
            id: MessageId(ledgerId: 0, entryId: 0),  // Will be set after send
            value: message,
            metadata: metadata,
            publishTime: Date(),
            producerName: producerName,
            replicatedFrom: nil,
            topicName: topic,
            redeliveryCount: 0,
            data: nil
        )
        
        // Process through interceptors if configured
      if let interceptors = interceptors {
          msg = try await interceptors.beforeSend(producer: self, message: msg)
      }
     
      // Store the message for interceptor notification
      let interceptorMessage = msg

      // Use the potentially modified metadata from the interceptor
      var metadata = msg.metadata
      if metadata.sequenceId == nil {
        metadata.sequenceId = nextSequenceId()
      }
      
      guard let sequenceId = metadata.sequenceId else {
        throw PulsarClientError.invalidConfiguration("Failed to generate sequence ID")
      }

      // Encode message synchronously to ensure no race conditions
      let payload = try schema.encode(message)

      // Create metadata synchronously
      let protoMetadata = await connection.commandBuilder.createMessageMetadata(
        producerName: producerName,
        sequenceId: sequenceId,
        publishTime: Date(),
        properties: metadata.properties,
        compressionType: configuration.compressionType
      )

      // Use the serial dispatch queue to ensure FIFO ordering
      return try await enqueueAndWait(
        metadata: protoMetadata, 
        payload: payload,
        interceptorMessage: interceptorMessage
      )
  }

  public func sendBatch(_ messages: [T]) async throws -> [MessageId] {
    // Capture state before entering the closure
    let currentState = _state

    return try await retryExecutor.executeWithProducerRetry(operation: "sendBatch") { [weak self] in
      guard let self = self else {
        throw PulsarClientError.producerBusy("Producer deallocated")
      }

      guard currentState == .connected else {
        throw PulsarClientError.producerBusy("Producer not connected")
      }

      var messageIds: [MessageId] = []

      for message in messages {
        let messageId = try await self.send(message)
        messageIds.append(messageId)
      }

      return messageIds
    }
  }

  public func flush() async throws {
    try await retryExecutor.executeWithProducerRetry(operation: "flush") { [weak self] in
      if let batchBuilder = self?.batchBuilder {
        try await batchBuilder.flush()
      }
    }
  }

  public func dispose() async {
    updateState(.closing)

    // IMPORTANT: Cancel pending operations FIRST to prevent hanging
    await sendQueue.cancelAll()

    // Then cancel tasks
    sendTask?.cancel()
    dispatcherTask?.cancel()

    // Send close producer command
    let closeCommand = await connection.commandBuilder.closeProducer(producerId: id)
    let frame = PulsarFrame(command: closeCommand)

    // Close producer expects a success response
    let _ = try? await connection.sendRequest(frame, responseType: SuccessResponse.self)

    // Remove from channel manager
    let channelManager = await connection.getChannelManager()
    await channelManager.removeProducer(id: id)

    // Unregister from tracker
    if let tracker = tracker {
      await tracker.unregisterProducer(self)
    }

    updateState(.closed)
    logger.debug("Producer closed", metadata: ["producerName": "\(producerName)"])
  }

  // MARK: - Private Methods

  /// Enqueue operation and wait for completion with FIFO ordering
  private func enqueueAndWait(
    metadata: Pulsar_Proto_MessageMetadata, 
    payload: Data,
    interceptorMessage: Message<T>? = nil
  ) async throws -> MessageId {
    return try await withCheckedThrowingContinuation { continuation in
      let sendOp = SendOperation<T>(
        metadata: metadata,
        payload: payload,
        continuation: continuation,
        interceptorMessage: interceptorMessage,
        interceptors: interceptors,
        producer: self
      )

      // Use serial dispatch queue to maintain FIFO ordering
      sendOrderQueue.async { [weak self] in
        guard let self = self else {
          continuation.resume(throwing: PulsarClientError.producerBusy("Producer deallocated"))
          return
        }

        Task {
          do {
            try await self.sendQueue.enqueue(sendOp)
          } catch {
            if Task.isCancelled {
              continuation.resume(throwing: CancellationError())
            } else {
              continuation.resume(throwing: error)
            }
          }
        }
      }
    }
  }

  private func nextSequenceId() -> UInt64 {
    let id = sequenceId
    sequenceId += 1
    return id
  }

  private func updateState(_ newState: ClientState) {
    _state = newState
    stateContinuation.yield(newState)

    // Update state manager
    Task {
      await stateManager.transitionTo(newState)
    }
  }

  /// Message dispatcher that processes the send queue (like C# MessageDispatcher)
  private func runMessageDispatcher() async {
    logger.debug("Starting message dispatcher", metadata: ["producerName": "\(producerName)"])

    while !Task.isCancelled && _state == .connected {
      do {
        // Get next operation from queue
        let sendOp = try await sendQueue.dequeue()

        // Get the producer channel
        let channelManager = await connection.getChannelManager()
        guard let producerChannel = await channelManager.getProducer(id: id) else {
          sendOp.fail(with: PulsarClientError.producerBusy("Producer channel not found"))
          continue
        }

        // Process the send operation
        await processSendOperation(sendOp, channel: producerChannel)

      } catch {
        if !Task.isCancelled {
      logger.error("Message dispatcher error", metadata: ["error": "\(error)"])
        }
      }
    }

    logger.debug("Message dispatcher stopped", metadata: ["producerName": "\(producerName)"])
  }

  /// Process a single send operation (like C# channel.Send)
  private func processSendOperation(_ sendOp: SendOperation<T>, channel: ProducerChannel) async {
    do {
      // Validate required fields before sending
      guard sendOp.metadata.hasProducerName,
        sendOp.metadata.hasSequenceID,
        sendOp.metadata.hasPublishTime
      else {
        let missingFields = [
          sendOp.metadata.hasProducerName ? nil : "producer_name",
          sendOp.metadata.hasSequenceID ? nil : "sequence_id",
          sendOp.metadata.hasPublishTime ? nil : "publish_time",
        ].compactMap { $0 }.joined(separator: ", ")

        sendOp.fail(
          with: PulsarClientError.invalidConfiguration(
            "Missing required metadata fields: \(missingFields)"
          ))
        return
      }

      // Create send command
      let sendCommand = await connection.commandBuilder.send(
        producerId: id,
        sequenceId: sendOp.sequenceId,
        numMessages: 1
      )

      // Create frame with metadata and payload
      let frame = PulsarFrame(
        command: sendCommand,
        metadata: sendOp.metadata,
        payload: sendOp.payload
      )

      // Register the send operation with the channel
      // This is key - we register BEFORE sending, but don't block
      await channel.registerSendOperation(sendOp)

      // Send the frame
      logger.trace("Sending frame for sequence", metadata: ["sequenceId": "\(sendOp.sequenceId)"])
      try await connection.sendCommand(frame)

      // The receipt will be handled by the channel when it arrives

    } catch {
      logger.error("Failed to send message", metadata: ["error": "\(error)"])
      sendOp.fail(with: error)
    }
  }

  private func runBatchSender() async {
    while !Task.isCancelled && _state == .connected {
      do {
        // Wait for batch timeout
        try await Task.sleep(nanoseconds: UInt64(configuration.batchingMaxDelay * 1_000_000_000))

        // Send any pending batch
        if let batch = await batchBuilder?.takeBatch() {
          try await sendBatch(batch)
        }
      } catch {
        if !Task.isCancelled {
      logger.debug("Batch sender error", metadata: ["error": "\(error)"])
        }
      }
    }
  }

  private func sendBatch(_ batch: MessageBatch<T>) async throws {
    guard !batch.messages.isEmpty else { return }

    // If only one message, send it as a single message
    if batch.messages.count == 1 {
      _ = try await send(batch.messages[0].value, metadata: batch.messages[0].metadata)
      return
    }

    // Create batched payload
    var batchedPayload = Data()
    var singleMessageMetadatas: [Pulsar_Proto_SingleMessageMetadata] = []

    for message in batch.messages {
      // Encode individual message
      let messagePayload = try schema.encode(message.value)

      // Create single message metadata
      var singleMeta = Pulsar_Proto_SingleMessageMetadata()
      singleMeta.payloadSize = Int32(messagePayload.count)
      singleMeta.sequenceID = message.sequenceId

      // Add properties if any
      if !message.metadata.properties.isEmpty {
        singleMeta.properties = message.metadata.properties.map { key, value in
          var kv = Pulsar_Proto_KeyValue()
          kv.key = key
          kv.value = value
          return kv
        }
      }

      // Add partition key if present
      if let key = message.metadata.key {
        singleMeta.partitionKey = key
      }

      // Add event time if present
      if let eventTime = message.metadata.eventTime {
        singleMeta.eventTime = UInt64(eventTime.timeIntervalSince1970 * 1000)
      }

      singleMessageMetadatas.append(singleMeta)

      // Serialize single message metadata
      let metadataData = try singleMeta.serializedData()

      // Write metadata size (4 bytes)
      var metadataSize = UInt32(metadataData.count).bigEndian
      batchedPayload.append(Data(bytes: &metadataSize, count: 4))

      // Write metadata
      batchedPayload.append(metadataData)

      // Write payload
      batchedPayload.append(messagePayload)
    }

    // Create batch metadata
    guard let firstSequenceId = batch.messages.first?.sequenceId else {
      throw PulsarClientError.invalidConfiguration("Batch has no messages")
    }
    
    var metadata = await connection.commandBuilder.createMessageMetadata(
      producerName: producerName,
      sequenceId: firstSequenceId,
      publishTime: Date(),
      properties: [:],  // Batch level properties
      compressionType: configuration.compressionType
    )

    // Set batch information
    metadata.numMessagesInBatch = Int32(batch.messages.count)
    metadata.uncompressedSize = UInt32(batchedPayload.count)

    // Create send command with the highest sequence ID
    guard let highestSequenceId = batch.messages.last?.sequenceId else {
      throw PulsarClientError.invalidConfiguration("Batch has no messages")
    }
    let sendCommand = await connection.commandBuilder.send(
      producerId: id,
      sequenceId: highestSequenceId,
      numMessages: Int32(batch.messages.count)
    )

    // Apply compression if needed
    let finalPayload: Data
    if configuration.compressionType != .none {
      // Compress the payload
      do {
        finalPayload = try compressData(batchedPayload, type: configuration.compressionType)
        metadata.compression = configuration.compressionType.toProto()
      } catch {
        logger.debug("Failed to compress batch, sending uncompressed", metadata: ["error": "\(error)"])
        finalPayload = batchedPayload
      }
    } else {
      finalPayload = batchedPayload
    }

    // Create frame with metadata and batched payload
    let frame = PulsarFrame(
      command: sendCommand,
      metadata: metadata,
      payload: finalPayload
    )

    // Get the producer channel
    let channelManager = await connection.getChannelManager()
    guard await channelManager.getProducer(id: id) != nil else {
      throw PulsarClientError.producerBusy("Producer channel not found")
    }

    // Validate required fields before sending batch
    guard metadata.hasProducerName,
      metadata.hasSequenceID,
      metadata.hasPublishTime
    else {
      let missingFields = [
        metadata.hasProducerName ? nil : "producer_name",
        metadata.hasSequenceID ? nil : "sequence_id",
        metadata.hasPublishTime ? nil : "publish_time",
      ].compactMap { $0 }.joined(separator: ", ")

      throw PulsarClientError.invalidConfiguration(
        "Missing required metadata fields in batch: \(missingFields)"
      )
    }

    // For batch sends, we need to handle all messages differently
    // The C# client uses a different approach for batches
    // For now, just send the batch without waiting for individual receipts
    try await connection.sendCommand(frame)

    // The broker will assign message IDs for individual messages in the batch
    // For now, we'll return the base message ID
    // In a full implementation, we'd need to handle batch acknowledgments
    logger.debug("Sent batch of \(batch.messages.count) messages")
  }

  // MARK: - Fault Tolerance Methods

  /// Handle producer error with fault tolerance
  func handleError(_ error: Error) async {
    logger.debug("Producer error", metadata: ["producerName": "\(producerName)", "error": "\(error)"])

    var exceptionContext = ExceptionContext(
      exception: error,
      operationType: "producer",
      componentType: "Producer"
    )

    await exceptionHandler.handleException(&exceptionContext)

    switch exceptionContext.result {
    case .retry, .retryAfter:
      // Try to recover producer state
      await attemptRecovery()

    case .fail:
      logger.error("Producer \(producerName) permanently failed: \(error)")
      updateState(.faulted(error))

    case .rethrow:
      // Keep current state, might recover later
      logger.debug(
        "Producer keeping current state after error",
        metadata: ["producerName": "\(producerName)", "error": "\(error)"]
      )
    }
  }

  /// Attempt to recover the producer
  private func attemptRecovery() async {
    do {
      updateState(.reconnecting)

      // Wait for connection to be ready
      let connectionState = await connection.state
      if connectionState != .connected {
        try await connection.connectWithRetry()
      }

      // Re-establish producer if needed
      // The channel manager should handle producer re-creation
      updateState(.connected)
      logger.debug("Producer recovered successfully", metadata: ["producerName": "\(producerName)"])

    } catch {
      logger.error(
        "Producer recovery failed",
        metadata: ["producerName": "\(producerName)", "error": "\(error)"]
      )
      updateState(.faulted(error))
    }
  }

  /// Get producer statistics for monitoring
  func getProducerStats() async -> ProducerStats {
    return ProducerStats(
      id: id,
      name: producerName,
      topic: topic,
      state: _state,
      totalMessages: sequenceId,
      isConnected: _state == .connected,
      isBatchingEnabled: configuration.batchingEnabled,
      pendingBatchSize: await batchBuilder?.getCurrentBatchSize() ?? 0
    )
  }
}

// MARK: - Producer Statistics

public struct ProducerStats: Sendable {
  public let id: UInt64
  public let name: String
  public let topic: String
  public let state: ClientState
  public let totalMessages: UInt64
  public let isConnected: Bool
  public let isBatchingEnabled: Bool
  public let pendingBatchSize: Int

  public init(
    id: UInt64,
    name: String,
    topic: String,
    state: ClientState,
    totalMessages: UInt64,
    isConnected: Bool,
    isBatchingEnabled: Bool,
    pendingBatchSize: Int
  ) {
    self.id = id
    self.name = name
    self.topic = topic
    self.state = state
    self.totalMessages = totalMessages
    self.isConnected = isConnected
    self.isBatchingEnabled = isBatchingEnabled
    self.pendingBatchSize = pendingBatchSize
  }
}

// MARK: - Supporting Types

private struct ProducerMessage<T: Sendable>: Sendable {
  let value: T
  let metadata: MessageMetadata
  let sequenceId: UInt64
}

private struct MessageBatch<T: Sendable>: Sendable {
  let messages: [ProducerMessage<T>]
  let totalSize: Int
}

/// Message batch builder
private actor MessageBatchBuilder<T: Sendable> {
  private let maxMessages: Int
  private let maxBytes: Int
  private let maxDelay: TimeInterval
  private weak var producer: ProducerImpl<T>?

  private var pendingMessages: [ProducerMessage<T>] = []
  private var currentBatchSize: Int = 0
  private var continuations: [CheckedContinuation<MessageId, Error>] = []

  init(maxMessages: Int, maxBytes: Int, maxDelay: TimeInterval, producer: ProducerImpl<T>? = nil) {
    self.maxMessages = maxMessages
    self.maxBytes = maxBytes
    self.maxDelay = maxDelay
    self.producer = producer
  }

  func setProducer(_ producer: ProducerImpl<T>) {
    self.producer = producer
  }

  func add(_ message: ProducerMessage<T>) async throws -> MessageId {
    return try await withCheckedThrowingContinuation { continuation in
      pendingMessages.append(message)
      continuations.append(continuation)

      // Estimate size with metadata overhead
      let messageSize = estimateMessageSize(message)
      currentBatchSize += messageSize

      // Check if batch is full
      if pendingMessages.count >= maxMessages || currentBatchSize >= maxBytes {
        Task { [weak self] in
          await self?.sendPendingBatch()
        }
      }
    }
  }

  private func estimateMessageSize(_ message: ProducerMessage<T>) -> Int {
    var size = 0

    // Try to estimate payload size (this is rough since we don't encode here)
    // In a real implementation, we might cache encoded sizes
    size += 100  // Base estimate for payload

    // Properties overhead
    for (key, value) in message.metadata.properties {
      size += key.utf8.count + value.utf8.count + 10  // Extra for protobuf encoding
    }

    // Metadata overhead
    size += 50  // Base metadata size
    if let key = message.metadata.key {
      size += key.utf8.count + 5
    }

    return size
  }

  func takeBatch() -> MessageBatch<T>? {
    guard !pendingMessages.isEmpty else { return nil }

    let messages = pendingMessages
    let size = currentBatchSize

    pendingMessages.removeAll()
    currentBatchSize = 0

    return MessageBatch(messages: messages, totalSize: size)
  }

  func flush() async throws {
    await sendPendingBatch()
  }

  func getCurrentBatchSize() -> Int {
    return pendingMessages.count
  }

  private func sendPendingBatch() async {
    guard let batch = takeBatch() else { return }
    guard let producer = producer else {
      // No producer set, fail all continuations
      for continuation in continuations {
        continuation.resume(throwing: PulsarClientError.producerBusy("Producer not available"))
      }
      continuations.removeAll()
      return
    }

    let savedContinuations = continuations
    continuations.removeAll()

    do {
      // Send the batch using internal method
      try await producer.sendBatchInternal(batch)

      // All messages in the batch succeeded - resolve continuations
      // In a real implementation, we'd need to map individual message IDs
      for (index, continuation) in savedContinuations.enumerated() {
        let messageId = MessageId(
          ledgerId: 0,
          entryId: UInt64(index),
          partition: -1,
          batchIndex: Int32(index)
        )
        continuation.resume(returning: messageId)
      }
    } catch {
      // Batch send failed - fail all continuations
      for continuation in savedContinuations {
        continuation.resume(throwing: error)
      }
    }
  }
}

// MARK: - Tracker Support

extension ProducerImpl {
  /// Set the tracker for this producer
  func setTracker(_ tracker: ClientTracker) {
    self.tracker = tracker
  }

  /// Internal method to send batch (used by MessageBatchBuilder)
  fileprivate func sendBatchInternal(_ batch: MessageBatch<T>) async throws {
    try await sendBatch(batch)
  }
}

// MARK: - Compression

private func compressData(_ data: Data, type: CompressionType) throws -> Data {
  switch type {
  case .none:
    return data
  case .lz4:
    // LZ4 compression would require external library
    throw PulsarClientError.notImplemented
  case .zlib:
    // For cross-platform compatibility, we need to use compression libraries
    // that work on both Apple and Linux platforms
    throw PulsarClientError.notImplemented
  case .zstd:
    // ZSTD compression would require external library
    throw PulsarClientError.notImplemented
  case .snappy:
    // Snappy compression would require external library
    throw PulsarClientError.notImplemented
  }
}

// MARK: - Compression Type Conversion

extension CompressionType {
  func toProto() -> Pulsar_Proto_CompressionType {
    switch self {
    case .none: return .none
    case .lz4: return .lz4
    case .zlib: return .zlib
    case .zstd: return .zstd
    case .snappy: return .snappy
    }
  }
}
