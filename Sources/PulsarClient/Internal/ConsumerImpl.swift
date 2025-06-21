import Foundation
import Logging

/// Consumer implementation
actor ConsumerImpl<T>: ConsumerProtocol where T: Sendable {
    typealias MessageType = T
    private let id: UInt64
    private let connection: Connection
    private let schema: Schema<T>
    private let configuration: ConsumerOptions<T>
    private let logger: Logger
    private let channelManager: ChannelManager
    private weak var tracker: ClientTracker?
    
    private var _state: ClientState = .connected
    internal let stateStream: AsyncStream<ClientState>
    private let stateContinuation: AsyncStream<ClientState>.Continuation
    
    private let messageQueue: AsyncChannel<Message<T>>
    private var receiveTask: Task<Void, Never>?
    private var permits: Int
    private var lastReceivedMessageId: MessageId?
    
    public let topics: [String]
    public let subscription: String
    public nonisolated var state: ClientState { 
        ClientState.connected  // Simple fallback for nonisolated access
    }
    public nonisolated var stateChanges: AsyncStream<ClientState> { stateStream }
    
    init(
        id: UInt64,
        topics: [String],
        subscription: String,
        connection: Connection,
        schema: Schema<T>,
        configuration: ConsumerOptions<T>,
        logger: Logger,
        channelManager: ChannelManager,
        tracker: ClientTracker? = nil
    ) {
        self.id = id
        self.topics = topics
        self.subscription = subscription
        self.connection = connection
        self.schema = schema
        self.configuration = configuration
        self.logger = logger
        self.channelManager = channelManager
        self.tracker = tracker
        self.permits = configuration.receiverQueueSize
        
        (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()
        self.messageQueue = AsyncChannel(capacity: configuration.receiverQueueSize)
        
        // Task will be started after initialization
    }
    
    func startReceiver() {
        self.receiveTask = Task { [weak self] in
            await self?.runReceiver()
        }
    }
    
    deinit {
        stateContinuation.finish()
        receiveTask?.cancel()
        // messageQueue will be cleaned up automatically
    }
    
    // MARK: - StateHolder
    
    public nonisolated func onStateChange(_ handler: @escaping @Sendable (ClientState) -> Void) {
        Task { [weak self] in
            guard let self = self else { return }
            for await state in await self.stateStream {
                handler(state)
            }
        }
    }
    
    public nonisolated func isFinal() -> Bool {
        false  // Conservative approach for nonisolated access
    }
    
    public nonisolated func handleException(_ error: any Error) {
        Task { [weak self] in
            await self?.processException(error)
        }
    }
    
    public func stateChangedTo(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState {
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
                
                for await currentState in await self.stateStream {
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
    
    public func stateChangedFrom(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState {
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
                
                for await currentState in await self.stateStream {
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
    
    // MARK: - ConsumerProtocol
    
    public nonisolated var isConnected: Bool {
        true  // Conservative approach for nonisolated access
    }
    
    public func receive() async throws -> Message<T> {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        // Get message from queue
        guard let message = await messageQueue.receive() else {
            throw PulsarClientError.consumerBusy("Consumer closed")
        }
        
        // Flow control: request more messages if needed
        permits -= 1
        if permits <= configuration.receiverQueueSize / 2 {
            await requestMoreMessages()
        }
        
        return message
    }
    
    public func receiveBatch(maxMessages: Int) async throws -> [Message<T>] {
        var messages: [Message<T>] = []
        
        for _ in 0..<maxMessages {
            if let message = try? await receive() {
                messages.append(message)
            } else {
                break
            }
        }
        
        return messages
    }
    
    public func acknowledge(_ message: Message<T>) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let ackCommand = await connection.commandBuilder.ack(consumerId: id, messageId: message.id)
        let frame = PulsarFrame(command: ackCommand)
        
        try await connection.sendCommand(frame)
        logger.trace("Acknowledged message \(message.id)")
    }
    
    public func acknowledgeCumulative(_ message: Message<T>) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        var command = Pulsar_Proto_BaseCommand()
        command.type = .ack
        
        var ack = Pulsar_Proto_CommandAck()
        ack.consumerID = id
        ack.ackType = .cumulative
        
        let msgId = message.id.toProto()
        ack.messageID = [msgId]
        
        command.ack = ack
        
        let frame = PulsarFrame(command: command)
        try await connection.sendCommand(frame)
        logger.trace("Cumulatively acknowledged up to message \(message.id)")
    }
    
    public func acknowledgeBatch(_ messages: [Message<T>]) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        // Acknowledge each message
        for message in messages {
            try await acknowledge(message)
        }
    }
    
    public func negativeAcknowledge(_ message: Message<T>) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        var command = Pulsar_Proto_BaseCommand()
        command.type = .redeliverUnacknowledgedMessages
        
        var redeliver = Pulsar_Proto_CommandRedeliverUnacknowledgedMessages()
        redeliver.consumerID = id
        
        let msgId = message.id.toProto()
        redeliver.messageIds = [msgId]
        
        command.redeliverUnacknowledgedMessages = redeliver
        
        let frame = PulsarFrame(command: command)
        try await connection.sendCommand(frame)
        logger.trace("Negatively acknowledged message \(message.id)")
    }
    
    public func seek(to messageId: MessageId) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let command = await connection.commandBuilder.seek(consumerId: id, messageId: messageId)
        let frame = PulsarFrame(command: command)
        
        // Seek expects a success response
        let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
        logger.info("Seeked to message \(messageId)")
    }
    
    public func seek(to timestamp: Date) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let command = await connection.commandBuilder.seek(consumerId: id, timestamp: timestamp)
        let frame = PulsarFrame(command: command)
        
        // Seek expects a success response
        let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
        logger.info("Seeked to timestamp \(timestamp)")
    }
    
    public func unsubscribe() async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let command = await connection.commandBuilder.unsubscribe(consumerId: id)
        let frame = PulsarFrame(command: command)
        
        // Unsubscribe expects a success response
        let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
        logger.info("Unsubscribed from \(subscription)")
    }
    
    public func getBufferedMessageCount() async -> Int {
        return await messageQueue.getBufferedCount()
    }
    
    public func getLastMessageId() async throws -> GetLastMessageIdResponse {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        return try await connection.getLastMessageId(consumerId: id)
    }
    
    public func getCurrentPosition() async -> MessageId? {
        return lastReceivedMessageId
    }
    
    public func dispose() async {
        updateState(.closing)
        
        // Cancel receive task
        receiveTask?.cancel()
        
        // Close message queue
        await messageQueue.finish()
        
        // Send close consumer command
        let command = await connection.commandBuilder.closeConsumer(consumerId: id)
        let frame = PulsarFrame(command: command)
        
        // Close consumer expects a success response
        let _ = try? await connection.sendRequest(frame, responseType: SuccessResponse.self)
        
        // Remove from channel manager
        await channelManager.removeConsumer(id: id)
        
        // Unregister from tracker
        if let tracker = tracker {
            await tracker.unregisterConsumer(self)
        }
        
        updateState(.closed)
        logger.info("Consumer closed for subscription \(subscription)")
    }
    
    // MARK: - Internal Methods
    
    /// Handle incoming message from broker
    /// Note: In Pulsar protocol, the actual message payload comes separately from the CommandMessage
    func handleIncomingMessage(_ commandMessage: Pulsar_Proto_CommandMessage, payload: Data, metadata: Pulsar_Proto_MessageMetadata) async {
        do {
            // Decode message payload
            let value = try schema.decode(payload)
            
            // Create MessageMetadata from protocol buffer
            let messageMetadata = MessageMetadata(from: metadata)
            
            // Create message
            let topicName = determineTopicName(from: commandMessage, metadata: metadata)
            let message = Message(
                id: MessageId(
                    ledgerId: commandMessage.messageID.ledgerID,
                    entryId: commandMessage.messageID.entryID,
                    partition: commandMessage.messageID.hasPartition ? commandMessage.messageID.partition : -1,
                    batchIndex: commandMessage.messageID.hasBatchIndex ? commandMessage.messageID.batchIndex : -1,
                    topic: topicName
                ),
                value: value,
                metadata: messageMetadata,
                publishTime: Date(timeIntervalSince1970: Double(metadata.publishTime) / 1000.0),
                producerName: metadata.producerName,
                replicatedFrom: metadata.hasReplicatedFrom ? metadata.replicatedFrom : nil,
                topicName: topicName
            )
            
            // Update last received message ID
            lastReceivedMessageId = message.id
            
            // Add to queue
            await messageQueue.send(message)
            
        } catch {
            logger.error("Failed to process message: \(error)")
        }
    }
    
    // MARK: - Private Methods
    
    private func updateState(_ newState: ClientState) {
        _state = newState
        stateContinuation.yield(newState)
    }
    
    private func processException(_ error: any Error) async {
        logger.error("Consumer exception: \(error)")
        
        // Handle different types of errors
        switch error {
        case let pulsarError as PulsarClientError:
            switch pulsarError {
            case .connectionFailed, .protocolError:
                // Connection issues - attempt to reconnect if configured
                // Note: ConsumerOptions doesn't have retryPolicy yet
                // if configuration.retryPolicy != nil {
                //     updateState(.reconnecting)
                //     // The connection manager will handle reconnection
                // } else {
                    updateState(.faulted(error))
                // }
            case .timeout:
                // Timeout errors are usually transient
                break
            default:
                updateState(.faulted(error))
            }
        default:
            updateState(.faulted(error))
        }
        
        // Note: ConsumerOptions doesn't have exceptionHandler yet
        // Call user-defined exception handler if provided
        // if let handler = configuration.exceptionHandler {
        //     await handler.onException(ExceptionContext(
        //         exception: error,
        //         operationType: "consume",
        //         componentType: "Consumer"
        //     ))
        // }
    }
    
    private func requestMoreMessages() async {
        let permitsToRequest = configuration.receiverQueueSize - permits
        guard permitsToRequest > 0 else { return }
        
        let flowCommand = await connection.commandBuilder.flow(
            consumerId: id,
            messagePermits: UInt32(permitsToRequest)
        )
        let frame = PulsarFrame(command: flowCommand)
        
        do {
            try await connection.sendCommand(frame)
            permits += permitsToRequest
            logger.trace("Requested \(permitsToRequest) more message permits")
        } catch {
            logger.warning("Failed to request more messages: \(error)")
        }
    }
    
    private func runReceiver() async {
        // This task handles connection events and message delivery
        // The actual message receiving is handled by handleIncomingMessage
        // which is called by the connection when messages arrive
    }
    
    private func determineTopicName(from commandMessage: Pulsar_Proto_CommandMessage, metadata: Pulsar_Proto_MessageMetadata) -> String {
        // If message metadata contains topic name, use it
        if metadata.hasPartitionKey && metadata.partitionKey.contains("://") {
            // Sometimes the partition key contains the full topic name
            return metadata.partitionKey
        }
        
        // For multi-topic subscriptions, we would need to maintain a mapping
        // of message IDs to topics. For now, return the first topic.
        // In a full implementation, the broker would send topic information
        // with each message for multi-topic subscriptions.
        return topics.first ?? ""
    }
}

// MARK: - AsyncChannel

/// Simple async channel for message queuing
private actor AsyncChannel<T: Sendable> {
    private var buffer: [T] = []
    private var waiters: [CheckedContinuation<T?, Never>] = []
    private var spaceWaiters: [CheckedContinuation<Void, Never>] = []
    private let capacity: Int
    private var isFinished = false
    
    init(capacity: Int) {
        self.capacity = capacity
    }
    
    func send(_ value: T) async {
        guard !isFinished else { return }
        
        if let waiter = waiters.first {
            waiters.removeFirst()
            waiter.resume(returning: value)
        } else if buffer.count < capacity {
            buffer.append(value)
        } else {
            // Buffer is full - implement backpressure by waiting
            await waitForSpace()
            // After space is available, try to add the value
            if buffer.count < capacity && !isFinished {
                buffer.append(value)
            }
        }
    }
    
    func receive() async -> T? {
        if let value = buffer.first {
            buffer.removeFirst()
            
            // Notify any waiting senders that space is available
            if let spaceWaiter = spaceWaiters.first {
                spaceWaiters.removeFirst()
                spaceWaiter.resume()
            }
            
            return value
        }
        
        if isFinished {
            return nil
        }
        
        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
    
    func finish() {
        isFinished = true
        for waiter in waiters {
            waiter.resume(returning: nil)
        }
        waiters.removeAll()
        
        // Also resume any space waiters
        for spaceWaiter in spaceWaiters {
            spaceWaiter.resume()
        }
        spaceWaiters.removeAll()
    }
    
    func getBufferedCount() -> Int {
        return buffer.count
    }
    
    private func waitForSpace() async {
        await withCheckedContinuation { continuation in
            spaceWaiters.append(continuation)
        }
    }
}

// MARK: - Tracker Support

extension ConsumerImpl {
    /// Set the tracker for this consumer
    func setTracker(_ tracker: ClientTracker) {
        self.tracker = tracker
    }
}