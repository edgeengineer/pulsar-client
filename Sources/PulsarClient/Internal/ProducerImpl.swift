import Foundation
import Logging
import NIOCore

/// Producer implementation with fault tolerance
actor ProducerImpl<T>: ProducerProtocol where T: Sendable {
    private let id: UInt64
    private let producerName: String
    private let connection: Connection
    private let schema: Schema<T>
    private let configuration: ProducerConfigBuilder<T>
    private let logger: Logger
    private weak var tracker: ClientTracker?
    
    private var sequenceId: UInt64
    private var _state: ClientState = .connected
    private let stateStream: AsyncStream<ClientState>
    private let stateContinuation: AsyncStream<ClientState>.Continuation
    
    private let batchBuilder: MessageBatchBuilder<T>?
    private var sendTask: Task<Void, Never>?
    
    // Fault tolerance components
    private let retryExecutor: RetryExecutor
    private let stateManager: ProducerStateManager
    private let exceptionHandler: ExceptionHandler
    
    public let topic: String
    public var state: ClientState { _state }
    public nonisolated var stateChanges: AsyncStream<ClientState> { stateStream }
    
    init(
        client: PulsarClient,
        connection: Connection,
        options: ProducerOptions<T>
    ) async throws {
        self.id = id
        self.topic = topic
        self.producerName = producerName
        self.connection = connection
        self.schema = schema
        self.configuration = configuration
        self.logger = logger
        self.tracker = tracker
        self.sequenceId = configuration.initialSequenceId ?? 0
        
        (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()
        
        // Initialize fault tolerance components
        self.retryExecutor = RetryExecutor(logger: logger)
        self.stateManager = StateManagerFactory.createProducerStateManager(
            initialState: .connected,
            componentName: "Producer-\(producerName)"
        )
        self.exceptionHandler = DefaultExceptionHandler(logger: logger)
        
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
    }
    
    // MARK: - ProducerProtocol
    
    public var isConnected: Bool {
        get async { _state == .connected }
    }
    
    public func send(_ message: T) async throws -> MessageId {
        try await send(message, metadata: MessageMetadata())
    }
    
    public func send(_ message: T, metadata: MessageMetadata) async throws -> MessageId {
        // Capture state and other properties before entering the closure
        let currentState = _state
        let sequenceId = nextSequenceId()
        let batchBuilderCopy = batchBuilder
        
        return try await retryExecutor.executeWithProducerRetry(operation: "send") { [weak self] in
            guard let self = self else {
                throw PulsarClientError.producerBusy("Producer deallocated")
            }
            
            guard currentState == .connected else {
                throw PulsarClientError.producerBusy("Producer not connected")
            }
            
            // Create producer message
            let producerMessage = ProducerMessage(
                value: message,
                metadata: metadata,
                sequenceId: sequenceId
            )
            
            if let batchBuilder = batchBuilderCopy {
                // Add to batch
                return try await batchBuilder.add(producerMessage)
            } else {
                // Send immediately
                return try await self.sendSingle(producerMessage)
            }
        }
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
        
        // Cancel batch sender
        sendTask?.cancel()
        
        // Flush any pending messages
        try? await flush()
        
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
        logger.info("Producer \(producerName) closed")
    }
    
    // MARK: - Private Methods
    
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
    
    private func sendSingle(_ message: ProducerMessage<T>) async throws -> MessageId {
        // Encode message
        let payload = try schema.encode(message.value)
        
        // Create metadata
        let metadata = await connection.commandBuilder.createMessageMetadata(
            from: message.metadata,
            producerName: producerName,
            publishTime: Date()
        )
        
        // Create send command
        let sendCommand = await connection.commandBuilder.send(
            producerId: id,
            sequenceId: message.sequenceId,
            numMessages: 1
        )
        
        // Create frame with metadata and payload
        let frame = PulsarFrame(
            command: sendCommand,
            metadata: metadata,
            payload: payload
        )
        
        // Get the producer channel
        let channelManager = await connection.getChannelManager()
        guard let producerChannel = await channelManager.getProducer(id: id) else {
            throw PulsarClientError.producerBusy("Producer channel not found")
        }
        
        // Register for receipt and send
        async let messageIdFuture = producerChannel.registerPendingSend(sequenceId: message.sequenceId)
        try await connection.sendCommand(frame)
        
        // Wait for receipt
        return try await messageIdFuture
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
                    logger.warning("Batch sender error: \(error)")
                }
            }
        }
    }
    
    /// Internal method for sending a batch from the batch builder
    fileprivate func sendBatchInternal(_ batch: MessageBatch<T>) async throws {
        try await sendBatch(batch)
    }
    
    private func sendBatch(_ batch: MessageBatch<T>) async throws {
        guard !batch.messages.isEmpty else { return }
        
        // If only one message, send it as a single message
        if batch.messages.count == 1 {
            _ = try await sendSingle(batch.messages[0])
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
        var metadata = await connection.commandBuilder.createMessageMetadata(
            producerName: producerName,
            sequenceId: batch.messages.first!.sequenceId,
            publishTime: Date(),
            properties: [:], // Batch level properties
            compressionType: configuration.compressionType
        )
        
        // Set batch information
        metadata.numMessagesInBatch = Int32(batch.messages.count)
        metadata.uncompressedSize = UInt32(batchedPayload.count)
        
        // Create send command with the highest sequence ID
        let highestSequenceId = batch.messages.last!.sequenceId
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
                logger.warning("Failed to compress batch, sending uncompressed: \(error)")
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
        guard let producerChannel = await channelManager.getProducer(id: id) else {
            throw PulsarClientError.producerBusy("Producer channel not found")
        }
        
        // Register for receipt with highest sequence ID
        async let messageIdFuture = producerChannel.registerPendingSend(sequenceId: highestSequenceId)
        try await connection.sendCommand(frame)
        
        // Wait for receipt
        _ = try await messageIdFuture
        
        // The broker will assign message IDs for individual messages in the batch
        // For now, we'll return the base message ID
        // In a full implementation, we'd need to handle batch acknowledgments
        logger.debug("Sent batch of \(batch.messages.count) messages")
    }
    
    // MARK: - Fault Tolerance Methods
    
    /// Handle producer error with fault tolerance
    func handleError(_ error: Error) async {
        logger.warning("Producer \(producerName) error: \(error)")
        
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
            logger.warning("Producer \(producerName) keeping current state after error: \(error)")
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
            logger.info("Producer \(producerName) recovered successfully")
            
        } catch {
            logger.error("Producer \(producerName) recovery failed: \(error)")
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
        size += 100 // Base estimate for payload
        
        // Properties overhead
        for (key, value) in message.metadata.properties {
            size += key.utf8.count + value.utf8.count + 10 // Extra for protobuf encoding
        }
        
        // Metadata overhead
        size += 50 // Base metadata size
        if message.metadata.key != nil {
            size += message.metadata.key!.utf8.count + 5
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
        // Use built-in zlib compression
        return try (data as NSData).compressed(using: .zlib) as Data
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