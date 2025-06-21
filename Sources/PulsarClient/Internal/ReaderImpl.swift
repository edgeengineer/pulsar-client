import Foundation
import Logging

/// Reader implementation wrapping a consumer
actor ReaderImpl<T>: ReaderProtocol where T: Sendable {
    private let consumer: any ConsumerProtocol<T>
    private let startMessageId: MessageId
    private let logger: Logger
    private let options: ReaderOptions<T>
    
    private var _state: ClientState = .connected
    private let stateStream: AsyncStream<ClientState>
    private let stateContinuation: AsyncStream<ClientState>.Continuation
    
    public let topic: String
    public var state: ClientState { _state }
    public nonisolated var stateChanges: AsyncStream<ClientState> { stateStream }
    
    init(
        consumer: any ConsumerProtocol<T>,
        topic: String,
        startMessageId: MessageId,
        logger: Logger,
        options: ReaderOptions<T>
    ) {
        self.consumer = consumer
        self.topic = topic
        self.startMessageId = startMessageId
        self.logger = logger
        self.options = options
        
        (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()
        
        // Monitor consumer state
        Task { [weak self] in
            await self?.monitorConsumerState()
        }
    }
    
    deinit {
        stateContinuation.finish()
    }
    
    // MARK: - ReaderProtocol
    
    public var isConnected: Bool {
        get async { 
            await consumer.isConnected
        }
    }
    
    public func readNext() async throws -> Message<T> {
        guard _state == ClientState.connected else {
            throw PulsarClientError.consumerBusy("Reader not connected")
        }
        
        let message = try await consumer.receive()
        
        // Automatically acknowledge the message
        try await consumer.acknowledge(message)
        
        return message
    }
    
    public func readBatch(maxMessages: Int) async throws -> [Message<T>] {
        guard _state == ClientState.connected else {
            throw PulsarClientError.consumerBusy("Reader not connected")
        }
        
        let messages = try await consumer.receiveBatch(maxMessages: maxMessages)
        
        // Automatically acknowledge all messages
        if !messages.isEmpty {
            try await consumer.acknowledgeBatch(messages)
        }
        
        return messages
    }
    
    public func hasMessageAvailable() async throws -> Bool {
        guard _state == ClientState.connected else {
            throw PulsarClientError.consumerBusy("Reader not connected")
        }
        
        // First check if we have messages in the local buffer
        if await hasBufferedMessages() {
            return true
        }
        
        // If no buffered messages, check broker-side availability
        return try await hasBrokerSideMessages()
    }
    
    private func hasBufferedMessages() async -> Bool {
        // Check if the consumer has any messages in its local buffer
        let bufferedCount = await consumer.getBufferedMessageCount()
        return bufferedCount > 0
    }
    
    private func hasBrokerSideMessages() async throws -> Bool {
        do {
            // Get the last available message ID from the broker
            let lastMessageResponse = try await consumer.getLastMessageId()
            let lastAvailableMessageId = lastMessageResponse.messageId
            
            // Get our current read position
            let currentPosition = await consumer.getCurrentPosition()
            
            // If we haven't read any messages yet, there are definitely messages available
            // if the last message ID is not the "earliest" marker
            guard let currentPosition = currentPosition else {
                // No messages read yet - check if there are any messages at all
                return lastAvailableMessageId != .earliest
            }
            
            // Compare positions: if last available > current position, messages are available
            let hasMoreMessages = lastAvailableMessageId > currentPosition
            
            logger.debug("Message availability check: last=\(lastAvailableMessageId), current=\(currentPosition), hasMore=\(hasMoreMessages)")
            
            return hasMoreMessages
            
        } catch {
            logger.warning("Failed to check broker-side message availability: \(error)")
            // Fall back to conservative approach - assume no messages available
            return false
        }
    }
    
    public func seek(to messageId: MessageId) async throws {
        guard _state == ClientState.connected else {
            throw PulsarClientError.consumerBusy("Reader not connected")
        }
        
        try await consumer.seek(to: messageId)
        logger.info("Reader seeked to message \(messageId)")
    }
    
    public func seek(to timestamp: Date) async throws {
        guard _state == ClientState.connected else {
            throw PulsarClientError.consumerBusy("Reader not connected")
        }
        
        try await consumer.seek(to: timestamp)
        logger.info("Reader seeked to timestamp \(timestamp)")
    }
    
    public func dispose() async {
        updateState(.closing)
        
        // Unsubscribe and dispose consumer
        try? await consumer.unsubscribe()
        await consumer.dispose()
        
        updateState(.closed)
        logger.info("Reader closed for topic \(topic)")
    }
    
    // MARK: - Private Methods
    
    private func updateState(_ newState: ClientState) {
        let previousState = _state
        _state = newState
        stateContinuation.yield(newState)
        
        // Notify state change handler if configured
        if let handler = options.stateChangedHandler {
            let stateChange = ReaderStateChanged(reader: self, state: newState, previousState: previousState)
            handler(stateChange)
        }
    }
    
    private func monitorConsumerState() async {
        for await state in consumer.stateChanges {
            updateState(state)
        }
    }
}