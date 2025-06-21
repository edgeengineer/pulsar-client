import Foundation
import NIOCore
import NIOPosix
import NIOSSL
import NIO
import Logging

/// Enhanced connection implementation with full request/response correlation
extension Connection {
    
    /// Send a command and wait for response
    func sendRequest<Response>(_ frame: PulsarFrame, responseType: Response.Type) async throws -> Response where Response: ResponseCommand {
        guard await state == .connected else {
            throw PulsarClientError.connectionFailed("Not connected")
        }
        
        guard let requestId = getRequestId(from: frame.command) else {
            throw PulsarClientError.protocolError("Command missing request ID")
        }
        
        // Create continuation for response
        let responseContinuation = AsyncThrowingStream<Pulsar_Proto_BaseCommand, Error>.makeStream()
        pendingRequests[requestId] = responseContinuation.continuation
        
        defer {
            pendingRequests.removeValue(forKey: requestId)
            responseContinuation.continuation.finish()
        }
        
        // Send the request
        try await sendFrame(frame)
        
        // Wait for response with timeout
        let response = try await withTimeout(seconds: 30) {
            for try await command in responseContinuation.stream {
                if let response = try? Response(from: command) {
                    return response
                }
                throw PulsarClientError.protocolError("Unexpected response type")
            }
            throw PulsarClientError.protocolError("No response received")
        }
        
        return response
    }
    
    /// Send command without expecting response
    func sendCommand(_ frame: PulsarFrame) async throws {
        guard await state == .connected else {
            throw PulsarClientError.connectionFailed("Not connected")
        }
        
        try await sendFrame(frame)
    }
    
    /// Handle incoming frame with full data
    func handleIncomingFrame(_ frame: PulsarFrame) {
        handleIncomingCommand(frame.command, frame: frame)
    }
    
    /// Handle incoming commands (enhanced version)
    private func handleIncomingCommand(_ command: Pulsar_Proto_BaseCommand, frame: PulsarFrame) {
        // First check if this is a response to a pending request
        if let requestId = getResponseRequestId(from: command),
           let continuation = pendingRequests.removeValue(forKey: requestId) {
            continuation.yield(command)
            continuation.finish()
            return
        }
        
        // Handle server-initiated commands
        switch command.type {
        case .ping:
            handlePing()
            
        case .message:
            handleMessage(command.message, frame: frame)
            
        case .sendReceipt:
            handleSendReceipt(command.sendReceipt)
            
        case .activeConsumerChange:
            handleActiveConsumerChange(command.activeConsumerChange)
            
        case .closeProducer:
            handleCloseProducer(command.closeProducer)
            
        case .closeConsumer:
            handleCloseConsumer(command.closeConsumer)
            
        case .error:
            handleError(command.error)
            
        default:
            logger.warning("Unhandled command type: \(command.type)")
        }
    }
    
    // MARK: - Private Helpers
    
    private func getRequestId(from command: Pulsar_Proto_BaseCommand) -> UInt64? {
        switch command.type {
        case .producer:
            return command.producer.requestID
        case .subscribe:
            return command.subscribe.requestID
        case .lookup:
            return command.lookupTopic.requestID
        case .partitionedMetadata:
            return command.partitionMetadata.requestID
        case .getLastMessageID:
            return command.getLastMessageID.requestID
        case .getSchema:
            return command.getSchema.requestID
        default:
            return nil
        }
    }
    
    private func getResponseRequestId(from command: Pulsar_Proto_BaseCommand) -> UInt64? {
        switch command.type {
        case .producerSuccess:
            return command.producerSuccess.requestID
        case .success:
            return command.success.requestID
        case .error:
            return command.error.requestID
        case .lookupResponse:
            return command.lookupTopicResponse.requestID
        case .partitionedMetadataResponse:
            return command.partitionMetadataResponse.requestID
        case .getLastMessageIDResponse:
            return command.getLastMessageIDResponse.requestID
        case .getSchemaResponse:
            return command.getSchemaResponse.requestID
        default:
            return nil
        }
    }
    
    private func handlePing() {
        Task {
            let pong = commandBuilder.pong()
            let frame = PulsarFrame(command: pong)
            try? await sendFrame(frame)
            logger.trace("Sent PONG in response to PING")
        }
    }
    
    private func handleMessage(_ message: Pulsar_Proto_CommandMessage, frame: PulsarFrame) {
        Task {
            guard let metadata = frame.metadata, let payload = frame.payload else {
                logger.warning("Received message without metadata or payload")
                return
            }
            
            await channelManager?.handleIncomingMessage(message, payload: payload, metadata: metadata)
        }
    }
    
    private func handleSendReceipt(_ receipt: Pulsar_Proto_CommandSendReceipt) {
        Task {
            // Forward to the producer channel
            if let producerChannel = await channelManager?.getProducer(id: receipt.producerID) {
                await producerChannel.handleSendReceipt(receipt)
            } else {
                logger.warning("Received send receipt for unknown producer \(receipt.producerID)")
            }
        }
    }
    
    private func handleActiveConsumerChange(_ change: Pulsar_Proto_CommandActiveConsumerChange) {
        Task {
            await channelManager?.handleActiveConsumerChange(change)
        }
    }
    
    private func handleCloseProducer(_ close: Pulsar_Proto_CommandCloseProducer) {
        Task {
            await channelManager?.handleCloseProducer(close)
        }
    }
    
    private func handleCloseConsumer(_ close: Pulsar_Proto_CommandCloseConsumer) {
        Task {
            await channelManager?.handleCloseConsumer(close)
        }
    }
    
    private func handleError(_ error: Pulsar_Proto_CommandError) {
        logger.error("Received error from broker: \(error.message) (request: \(error.requestID))")
        
        // If this is for a pending request, fail it
        if let continuation = pendingRequests.removeValue(forKey: error.requestID) {
            continuation.yield(Pulsar_Proto_BaseCommand.with {
                $0.type = .error
                $0.error = error
            })
            continuation.finish()
        }
    }
}

// MARK: - Response Command Protocol

protocol ResponseCommand: Sendable {
    init(from command: Pulsar_Proto_BaseCommand) throws
}

// MARK: - Response Types

struct ConnectedResponse: ResponseCommand {
    let serverVersion: String
    let protocolVersion: Int32
    let maxMessageSize: Int32?
    
    init(from command: Pulsar_Proto_BaseCommand) throws {
        guard command.type == .connected else {
            throw PulsarClientError.protocolError("Expected CONNECTED response")
        }
        
        self.serverVersion = command.connected.serverVersion
        self.protocolVersion = command.connected.protocolVersion
        self.maxMessageSize = command.connected.hasMaxMessageSize ? Int32(command.connected.maxMessageSize) : nil
    }
}

struct ProducerSuccessResponse: ResponseCommand {
    let producerName: String
    let lastSequenceId: Int64
    let schemaVersion: Data?
    let topicEpoch: UInt64?
    
    init(from command: Pulsar_Proto_BaseCommand) throws {
        guard command.type == .producerSuccess else {
            throw PulsarClientError.protocolError("Expected PRODUCER_SUCCESS response")
        }
        
        let success = command.producerSuccess
        self.producerName = success.producerName
        self.lastSequenceId = success.lastSequenceID
        self.schemaVersion = success.hasSchemaVersion ? success.schemaVersion : nil
        self.topicEpoch = success.hasTopicEpoch ? success.topicEpoch : nil
    }
}

struct SuccessResponse: ResponseCommand {
    let requestId: UInt64
    
    init(from command: Pulsar_Proto_BaseCommand) throws {
        guard command.type == .success else {
            throw PulsarClientError.protocolError("Expected SUCCESS response")
        }
        
        self.requestId = command.success.requestID
    }
}

struct LookupResponse: ResponseCommand {
    enum Response {
        case redirect(brokerServiceUrl: String, brokerServiceUrlTls: String?)
        case connect(brokerServiceUrl: String, brokerServiceUrlTls: String?)
        case failed(error: Pulsar_Proto_ServerError)
    }
    
    let response: Response
    let authoritative: Bool
    let proxyThroughServiceUrl: Bool
    
    init(from command: Pulsar_Proto_BaseCommand) throws {
        guard command.type == .lookupResponse else {
            throw PulsarClientError.protocolError("Expected LOOKUP_RESPONSE")
        }
        
        let lookup = command.lookupTopicResponse
        self.authoritative = lookup.authoritative
        self.proxyThroughServiceUrl = lookup.proxyThroughServiceURL
        
        switch lookup.response {
        case .redirect:
            self.response = .redirect(
                brokerServiceUrl: lookup.brokerServiceURL,
                brokerServiceUrlTls: lookup.hasBrokerServiceURLTls ? lookup.brokerServiceURLTls : nil
            )
        case .connect:
            self.response = .connect(
                brokerServiceUrl: lookup.brokerServiceURL,
                brokerServiceUrlTls: lookup.hasBrokerServiceURLTls ? lookup.brokerServiceURLTls : nil
            )
        case .failed:
            self.response = .failed(error: lookup.error)
        default:
            throw PulsarClientError.protocolError("Unknown lookup response type")
        }
    }
}

struct SendReceiptResponse: ResponseCommand {
    let producerId: UInt64
    let sequenceId: UInt64
    let messageId: MessageId
    let highestSequenceId: UInt64?
    
    init(from command: Pulsar_Proto_BaseCommand) throws {
        guard command.type == .sendReceipt else {
            throw PulsarClientError.protocolError("Expected SEND_RECEIPT")
        }
        
        let receipt = command.sendReceipt
        self.producerId = receipt.producerID
        self.sequenceId = receipt.sequenceID
        self.messageId = MessageId(
            ledgerId: receipt.messageID.ledgerID,
            entryId: receipt.messageID.entryID,
            partition: receipt.messageID.hasPartition ? receipt.messageID.partition : -1,
            batchIndex: receipt.messageID.hasBatchIndex ? receipt.messageID.batchIndex : -1
        )
        self.highestSequenceId = receipt.hasHighestSequenceID ? receipt.highestSequenceID : nil
    }
}

public struct GetLastMessageIdResponse: ResponseCommand {
    public let messageId: MessageId
    public let consumerMarkDeletePosition: MessageId?
    
    public init(from command: Pulsar_Proto_BaseCommand) throws {
        guard command.type == .getLastMessageIDResponse else {
            throw PulsarClientError.protocolError("Expected GET_LAST_MESSAGE_ID_RESPONSE")
        }
        
        let response = command.getLastMessageIDResponse
        self.messageId = MessageId(from: response.lastMessageID)
        
        if response.hasConsumerMarkDeletePosition {
            self.consumerMarkDeletePosition = MessageId(from: response.consumerMarkDeletePosition)
        } else {
            self.consumerMarkDeletePosition = nil
        }
    }
}

// MARK: - Connection Extensions

extension Connection {
    /// Perform broker lookup for topic
    func lookup(topic: String) async throws -> LookupResponse {
        let command = commandBuilder.lookup(topic: topic)
        let frame = PulsarFrame(command: command)
        return try await sendRequest(frame, responseType: LookupResponse.self)
    }
    
    /// Create a producer
    func createProducer(
        topic: String,
        producerName: String?,
        schema: SchemaInfo?,
        producerAccessMode: ProducerAccessMode = .shared,
        producerProperties: [String: String] = [:]
    ) async throws -> (UInt64, ProducerSuccessResponse) {
        let (command, producerId) = commandBuilder.createProducer(
            topic: topic,
            producerName: producerName,
            schema: schema,
            initialSequenceId: nil,
            producerAccessMode: producerAccessMode,
            producerProperties: producerProperties
        )
        let frame = PulsarFrame(command: command)
        let response = try await sendRequest(frame, responseType: ProducerSuccessResponse.self)
        return (producerId, response)
    }
    
    /// Get the last message ID for a consumer
    func getLastMessageId(consumerId: UInt64) async throws -> GetLastMessageIdResponse {
        let command = commandBuilder.getLastMessageId(consumerId: consumerId)
        let frame = PulsarFrame(command: command)
        return try await sendRequest(frame, responseType: GetLastMessageIdResponse.self)
    }
}

// MARK: - Fault Tolerance Support

extension Connection {
    
    /// Execute operation with fault tolerance
    func executeWithFaultTolerance<T: Sendable>(
        operation: String,
        _ block: @escaping @Sendable () async throws -> T
    ) async throws -> T {
        let retryExecutor = RetryExecutor(logger: logger)
        return try await retryExecutor.executeWithConnectionRetry(operation: operation, block)
    }
    
    /// Send request with automatic retry on failure
    func sendRequestWithRetry<Response>(_ frame: PulsarFrame, responseType: Response.Type) async throws -> Response where Response: ResponseCommand {
        return try await executeWithFaultTolerance(operation: "sendRequest") { [weak self] in
            guard let self = self else {
                throw PulsarClientError.connectionFailed("Connection deallocated")
            }
            return try await self.sendRequest(frame, responseType: responseType)
        }
    }
    
    /// Connect with automatic retry
    func connectWithRetry() async throws {
        try await executeWithFaultTolerance(operation: "connect") { [weak self] in
            guard let self = self else {
                throw PulsarClientError.connectionFailed("Connection deallocated")
            }
            try await self.connect()
        }
    }
}

// MARK: - Enhanced Reconnection Support

extension Connection {
    
    /// Reconnect to the broker with fault tolerance
    func reconnect() async throws {
        logger.info("Starting connection reconnection")
        
        // Update state to reconnecting
        await updateState(.reconnecting)
        
        try await executeWithFaultTolerance(operation: "reconnect") { [weak self] in
            guard let self = self else {
                throw PulsarClientError.connectionFailed("Connection deallocated")
            }
            
            // Close existing connection
            await self.close()
            
            // Attempt to reconnect
            try await self.connect()
            
            // Re-establish all channels
            await self.channelManager?.reconnectAll()
            
            self.logger.info("Connection reconnection completed successfully")
        }
    }
    
    /// Start automatic reconnection monitoring
    func startReconnectionMonitoring() {
        Task {
            await monitorConnectionHealth()
        }
    }
    
    /// Monitor connection health and handle reconnection
    private func monitorConnectionHealth() async {
        while !Task.isCancelled {
            let currentState = await state
            
            switch currentState {
            case .connected:
                await performHealthCheck()
                
            case .faulted(let error):
                await handleConnectionFault(error)
                
            case .disconnected:
                // Try to reconnect
                await attemptReconnection()
                
            case .reconnecting:
                // Wait for reconnection to complete
                try? await Task.sleep(nanoseconds: 2_000_000_000) // 2 seconds
                
            default:
                // Wait before next check
                try? await Task.sleep(nanoseconds: 5_000_000_000) // 5 seconds
            }
        }
    }
    
    /// Perform health check with ping
    private func performHealthCheck() async {
        // Send ping every 30 seconds
        try? await Task.sleep(nanoseconds: 30_000_000_000)
        
        let currentState = await state
        guard currentState == .connected else { return }
        
        do {
            let ping = commandBuilder.ping()
            let frame = PulsarFrame(command: ping)
            try await sendFrame(frame)
            logger.trace("Health check ping sent successfully")
        } catch {
            logger.warning("Health check ping failed: \(error)")
            // Connection might be dead, trigger reconnection
            await updateState(.faulted(error))
        }
    }
    
    /// Handle connection fault
    private func handleConnectionFault(_ error: Error) async {
        logger.error("Connection faulted: \(error)")
        
        // Create exception context
        var exceptionContext = ExceptionContext(
            exception: error,
            operationType: "connectionHealth",
            componentType: "Connection"
        )
        
        let exceptionHandler = DefaultExceptionHandler(logger: logger)
        await exceptionHandler.handleException(&exceptionContext)
        
        switch exceptionContext.result {
        case .retry, .retryAfter:
            // Attempt reconnection
            await attemptReconnection()
            
        case .fail:
            logger.error("Connection permanently failed: \(error)")
            await updateState(.closed)
            
        case .rethrow:
            // Keep faulted state, might recover later
            logger.warning("Keeping connection in faulted state: \(error)")
        }
    }
    
    /// Attempt automatic reconnection
    private func attemptReconnection() async {
        do {
            try await reconnect()
        } catch {
            logger.error("Automatic reconnection failed: \(error)")
            await updateState(.faulted(error))
            
            // Wait before next attempt
            try? await Task.sleep(nanoseconds: 5_000_000_000) // 5 seconds
        }
    }
    
    /// Get connection statistics for monitoring
    func getConnectionStats() async -> ConnectionStats {
        let currentState = await state
        return ConnectionStats(
            state: currentState,
            connectedAt: connectedAt,
            totalMessages: totalMessagesSent + totalMessagesReceived,
            totalBytesSent: totalBytesSent,
            totalBytesReceived: totalBytesReceived,
            activeProducers: await channelManager?.getProducerCount() ?? 0,
            activeConsumers: await channelManager?.getConsumerCount() ?? 0
        )
    }
}

// MARK: - Connection Statistics

public struct ConnectionStats: Sendable {
    public let state: ConnectionState
    public let connectedAt: Date?
    public let totalMessages: UInt64
    public let totalBytesSent: UInt64
    public let totalBytesReceived: UInt64
    public let activeProducers: Int
    public let activeConsumers: Int
    
    public init(
        state: ConnectionState,
        connectedAt: Date?,
        totalMessages: UInt64,
        totalBytesSent: UInt64,
        totalBytesReceived: UInt64,
        activeProducers: Int,
        activeConsumers: Int
    ) {
        self.state = state
        self.connectedAt = connectedAt
        self.totalMessages = totalMessages
        self.totalBytesSent = totalBytesSent
        self.totalBytesReceived = totalBytesReceived
        self.activeProducers = activeProducers
        self.activeConsumers = activeConsumers
    }
}