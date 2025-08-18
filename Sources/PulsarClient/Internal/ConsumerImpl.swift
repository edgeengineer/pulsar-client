import Foundation
import Logging

/// Consumer implementation
actor ConsumerImpl<T>: ConsumerProtocol, AsyncSequence where T: Sendable {
    typealias MessageType = T
    typealias Element = Message<T>
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
    
    // Message stream with built-in backpressure via buffering policy
    private let messageStream: AsyncStream<Message<T>>
    private let messageContinuation: AsyncStream<Message<T>>.Continuation
    
    private var receiveTask: Task<Void, Never>?
    private var permits: Int
    private var isFirstFlow = true
    private var lastReceivedMessageId: MessageId?
    private var bufferedMessageCount: Int = 0  // Track buffered messages
    
    // Dead Letter Queue handler
    private let dlqHandler: DeadLetterQueueHandler<T>?
    private weak var client: PulsarClient?
    
    // Interceptors
    private let interceptors: ConsumerInterceptors<T>?
    
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
        tracker: ClientTracker? = nil,
        client: PulsarClient? = nil
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
        self.client = client
        self.permits = 0  // Start with 0 permits, will request them in runReceiver
        
        (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()
        
        // Create async stream with buffering for backpressure
        // The bufferingOldest policy will drop old messages if buffer is full
        // Combined with Pulsar's permit system, this provides effective backpressure
        (self.messageStream, self.messageContinuation) = AsyncStream<Message<T>>.makeStream(
            bufferingPolicy: .bufferingOldest(configuration.receiverQueueSize)
        )
        
        // Initialize DLQ handler if policy is configured
        if let dlqPolicy = configuration.deadLetterPolicy,
           let client = client,
           let firstTopic = topics.first {
            self.dlqHandler = DeadLetterQueueHandler(
                policy: dlqPolicy,
                originalTopic: firstTopic,
                subscriptionName: subscription,
                client: client,
                schema: schema
            )
        } else {
            self.dlqHandler = nil
        }
        
        // Initialize interceptors if configured
        if !configuration.interceptors.isEmpty {
            self.interceptors = ConsumerInterceptors(interceptors: configuration.interceptors)
        } else {
            self.interceptors = nil
        }
        
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
        messageContinuation.finish()
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
    
    // MARK: - ConsumerProtocol
    
    public nonisolated var isConnected: Bool {
        true  // Conservative approach for nonisolated access
    }
    
    /// Receive a message with timeout
    public func receive(timeout: TimeInterval) async throws -> Message<T> {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        // Race between timeout and receive using proper error handling
        return try await withThrowingTaskGroup(of: Message<T>.self) { group in
            // Add timeout task
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                throw PulsarClientError.timeout("Receive operation timed out after \(timeout) seconds")
            }
            
            // Add receive task
            group.addTask { [weak self] in
                guard let self = self else {
                    throw PulsarClientError.consumerBusy("Consumer deallocated")
                }
                
                // Try to receive a message from the stream
                for await message in self.messageStream {
                    return message
                }
                throw PulsarClientError.consumerBusy("Consumer closed")
            }
            
            // Wait for the first task to complete (either timeout or message received)
            guard var message = try await group.next() else {
                throw PulsarClientError.consumerBusy("No message available")
            }
            
            // Cancel the other task
            group.cancelAll()
            
            // Decrement buffered count since we're delivering a message
            self.bufferedMessageCount = Swift.max(0, self.bufferedMessageCount - 1)
            
            // Process through interceptors if configured
            if let interceptors = self.interceptors {
                message = try await interceptors.beforeConsume(consumer: self, message: message)
            }
            
            // Flow control: request more messages if needed
            // Note: permits are already decremented in handleIncomingMessage
            if self.permits <= self.configuration.receiverQueueSize / 4 {
                await self.requestMoreMessages()
            }
            
            return message
        }
    }
    
    public func receive() async throws -> Message<T> {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        // Get message from stream
        for await message in messageStream {
            var unwrappedMessage = message
            
            // Decrement buffered count since we're delivering a message
            bufferedMessageCount = Swift.max(0, bufferedMessageCount - 1)
            
            // Process through interceptors if configured
            if let interceptors = interceptors {
                unwrappedMessage = try await interceptors.beforeConsume(consumer: self, message: unwrappedMessage)
            }
            
            // Flow control: request more messages if needed
            // Note: permits are already decremented in handleIncomingMessage
            if permits <= configuration.receiverQueueSize / 4 {
                await requestMoreMessages()
            }
            
            return unwrappedMessage
        }
        
        // Stream ended
        throw PulsarClientError.consumerBusy("Consumer closed")
    }
    
    public func receiveBatch(maxMessages: Int) async throws -> [Message<T>] {
        var messages: [Message<T>] = []
        
        for _ in 0..<maxMessages {
            // Break early if no messages are buffered
            if bufferedMessageCount == 0 {
                break
            }
            
            if let message = try? await receive() {
                messages.append(message)
                // Note: bufferedMessageCount is already decremented in receive()
            } else {
                break
            }
        }
        
        return messages
    }
    
    // MARK: - AsyncSequence Conformance
    
    public struct AsyncIterator: AsyncIteratorProtocol {
        private let consumer: ConsumerImpl<T>
        
        init(consumer: ConsumerImpl<T>) {
            self.consumer = consumer
        }
        
        public mutating func next() async throws -> Message<T>? {
            guard await consumer._state == .connected else {
                return nil
            }
            
            // Get next message from the stream
            for await message in consumer.messageStream {
                var msg = message
                
                // Process message
                await consumer.processReceivedMessage(&msg)
                return msg
            }
            
            return nil
        }
    }
    
    public nonisolated func makeAsyncIterator() -> AsyncIterator {
        return AsyncIterator(consumer: self)
    }
    
    /// Process a received message (decrement counts, apply interceptors, etc.)
    private func processReceivedMessage(_ message: inout Message<T>) async {
        // Decrement buffered count since we're delivering a message
        bufferedMessageCount = Swift.max(0, bufferedMessageCount - 1)
        
        // Process through interceptors if configured
        if let interceptors = interceptors {
            message = try! await interceptors.beforeConsume(consumer: self, message: message)
        }
        
        // Request more permits if we're running low
        if permits < configuration.receiverQueueSize / 2 {
            await requestMorePermits()
        }
    }
    
    /// Request more permits from the broker
    private func requestMorePermits() async {
        await requestMoreMessages()
    }
    
    public func acknowledge(_ message: Message<T>) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let ackCommand = await connection.commandBuilder.ack(consumerId: id, messageId: message.id)
        let frame = PulsarFrame(command: ackCommand)
        
        do {
            try await connection.sendCommand(frame)
            logger.trace("Acknowledged message \(message.id)")
            
            // Reset DLQ redelivery count on successful acknowledgment
            if let dlqHandler = dlqHandler {
                await dlqHandler.resetRedeliveryCount(for: message.id)
            }
            
            // Notify interceptors of successful acknowledgment
            if let interceptors = interceptors {
                await interceptors.onAcknowledge(consumer: self, messageId: message.id, error: nil)
            }
        } catch {
            // Notify interceptors of failed acknowledgment
            if let interceptors = interceptors {
                await interceptors.onAcknowledge(consumer: self, messageId: message.id, error: error)
            }
            throw error
        }
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
        
        // Check if message should go to DLQ or retry topic after this negative ack
        if let dlqHandler = dlqHandler {
            let action = await dlqHandler.trackNegativeAck(message: message)
            
            switch action {
            case .dlq:
                logger.debug("Message will exceed max redelivery count, sending to DLQ", metadata: [
                    "messageId": "\(message.id)",
                    "currentRedeliveryCount": "\(message.redeliveryCount)"
                ])
                
                do {
                    // Send to DLQ
                    try await dlqHandler.sendToDLQ(message)
                    
                    // Acknowledge the original message after successful DLQ send
                    try await acknowledge(message)
                    
                    // Clean up old entries periodically
                    await dlqHandler.cleanupOldEntries()
                    
                    return
                } catch {
                    logger.error("Failed to send message to DLQ", metadata: [
                        "messageId": "\(message.id)",
                        "error": "\(error)"
                    ])
                    // Fall through to normal negative acknowledgment
                }
                
            case .retry:
                logger.debug("Sending message to retry topic", metadata: [
                    "messageId": "\(message.id)",
                    "currentRedeliveryCount": "\(message.redeliveryCount)"
                ])
                
                do {
                    // Send to retry topic
                    try await dlqHandler.sendToRetryTopic(message)
                    
                    // Acknowledge the original message after successful retry send
                    try await acknowledge(message)
                    
                    // Clean up old entries periodically
                    await dlqHandler.cleanupOldEntries()
                    
                    return
                } catch {
                    logger.error("Failed to send message to retry topic", metadata: [
                        "messageId": "\(message.id)",
                        "error": "\(error)"
                    ])
                    // Fall through to normal negative acknowledgment
                }
                
            case .none:
                // Continue with normal negative acknowledgment
                break
            }
        }
        
        // Normal negative acknowledgment for redelivery
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
        
        // Note: We don't need to restore permits here because the broker will
        // redeliver the message which will use a permit when it arrives
        
        // Notify interceptors of negative acknowledgment
        if let interceptors = interceptors {
            await interceptors.onNegativeAcksSend(consumer: self, messageIds: Set([message.id]))
        }
    }
    
    public func seek(to messageId: MessageId) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let command = await connection.commandBuilder.seek(consumerId: id, messageId: messageId)
        let frame = PulsarFrame(command: command)
        
        // Seek expects a success response
        let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
        logger.debug("Seeked to message", metadata: ["messageId": "\(messageId)"])
    }
    
    public func seek(to timestamp: Date) async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let command = await connection.commandBuilder.seek(consumerId: id, timestamp: timestamp)
        let frame = PulsarFrame(command: command)
        
        // Seek expects a success response
        let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
        logger.debug("Seeked to timestamp", metadata: ["timestamp": "\(timestamp)"])
    }
    
    public func unsubscribe() async throws {
        guard _state == .connected else {
            throw PulsarClientError.consumerBusy("Consumer not connected")
        }
        
        let command = await connection.commandBuilder.unsubscribe(consumerId: id)
        let frame = PulsarFrame(command: command)
        
        // Unsubscribe expects a success response
        let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
        logger.debug("Unsubscribed from subscription", metadata: ["subscription": "\(subscription)"])
    }
    
    public func getBufferedMessageCount() async -> Int {
        return bufferedMessageCount
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
        
        // Close message stream
        messageContinuation.finish()
        
        // Reset buffered message count
        bufferedMessageCount = 0
        
        // Dispose DLQ handler if configured
        if let dlqHandler = dlqHandler {
            await dlqHandler.dispose()
        }
        
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
        logger.debug("Consumer closed for subscription", metadata: ["subscription": "\(subscription)"])
    }
    
    // MARK: - Internal Methods
    
    /// Handle incoming message from broker
    /// Note: In Pulsar protocol, the actual message payload comes separately from the CommandMessage
    func handleIncomingMessage(_ commandMessage: Pulsar_Proto_CommandMessage, payload: Data, metadata: Pulsar_Proto_MessageMetadata) async {
        logger.trace("Consumer received message", metadata: ["consumerId": "\(id)", "ledgerId": "\(commandMessage.messageID.ledgerID)", "entryId": "\(commandMessage.messageID.entryID)"])
        
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
                topicName: topicName,
                redeliveryCount: commandMessage.hasRedeliveryCount ? commandMessage.redeliveryCount : 0
            )
            
            // Update last received message ID
            lastReceivedMessageId = message.id
            
            // Decrement permits since we received a message from broker
            permits -= 1
            logger.trace("Received message, permits remaining: \(permits)")
            
            // Add to message stream
            // The stream's buffering policy handles backpressure
            // Combined with Pulsar's permit system, this prevents unbounded growth
            messageContinuation.yield(message)
            
            // Increment buffered count
            bufferedMessageCount += 1
            
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
        let permitsToRequest: Int
        
        if isFirstFlow {
            // First FLOW command: request full receiver queue size (like C# client)
            permitsToRequest = configuration.receiverQueueSize
            logger.debug("Consumer sending FIRST FLOW command", metadata: ["consumerId": "\(id)", "permits": "\(permitsToRequest)"])
            isFirstFlow = false
        } else {
            // Subsequent FLOW commands: only request what we need (like C# client)
            let needed = configuration.receiverQueueSize - permits
            guard needed > 0 else {
                logger.debug("No permits to request - current: \(permits), queue size: \(configuration.receiverQueueSize)")
                return
            }
            permitsToRequest = needed
            logger.trace("Consumer requesting additional permits", metadata: ["consumerId": "\(id)", "requestedPermits": "\(permitsToRequest)", "currentPermits": "\(permits)"])
        }
        
        let flowCommand = await connection.commandBuilder.flow(
            consumerId: id,
            messagePermits: UInt32(permitsToRequest)
        )
        let frame = PulsarFrame(command: flowCommand)
        
        do {
            try await connection.sendCommand(frame)
            permits += permitsToRequest
            logger.trace("Successfully sent FLOW command", metadata: ["requestedPermits": "\(permitsToRequest)", "totalPermits": "\(permits)"])
        } catch {
            logger.debug("Failed to request more messages", metadata: ["error": "\(error)"])
        }
    }
    
    private func runReceiver() async {
        logger.debug("Consumer receiver task started", metadata: ["consumerId": "\(id)"])
        
        // Send initial FLOW command to start receiving messages
        await requestMoreMessages()
        logger.debug("Consumer sent initial FLOW command", metadata: ["consumerId": "\(id)", "permits": "\(permits)"])
        
        // Keep the receiver task alive to handle ongoing operations
        // The actual message receiving is handled by handleIncomingMessage
        // which is called by the connection when messages arrive
        while !Task.isCancelled && _state == .connected {
            do {
                // Sleep and periodically check state
                try await Task.sleep(nanoseconds: 1_000_000_000) // 1 second
                
                // Check if we need more permits (flow control)
                if permits <= configuration.receiverQueueSize / 4 {
                    await requestMoreMessages()
                }
            } catch {
                if !Task.isCancelled {
                    logger.debug("Consumer receiver error", metadata: ["error": "\(error)"])
                }
                break
            }
        }
        
        logger.debug("Consumer receiver task stopped", metadata: ["consumerId": "\(id)"])
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

// MARK: - Tracker Support

extension ConsumerImpl {
    /// Set the tracker for this consumer
    func setTracker(_ tracker: ClientTracker) {
        self.tracker = tracker
    }
}