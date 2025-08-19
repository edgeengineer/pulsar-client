import Foundation
import Logging
import NIO
import NIOCore
import NIOPosix
import NIOSSL

/// Connection state
public enum ConnectionState: Equatable, Sendable {
  case disconnected
  case connecting
  case connected
  case reconnecting
  case closing
  case closed
  case faulted(Error)

  public static func == (lhs: ConnectionState, rhs: ConnectionState) -> Bool {
    switch (lhs, rhs) {
    case (.disconnected, .disconnected),
      (.connecting, .connecting),
      (.connected, .connected),
      (.reconnecting, .reconnecting),
      (.closing, .closing),
      (.closed, .closed):
      return true
    case (.faulted, .faulted):
      return true  // Consider all faulted states equal
    default:
      return false
    }
  }
}

/// Pulsar connection protocol
protocol PulsarConnection: Actor {
  var state: ConnectionState { get }
  var stateChanges: AsyncStream<ConnectionState> { get }

  func connect() async throws
  func send(frame: PulsarFrame) async throws
  func close() async
}

/// Main connection implementation
actor Connection: PulsarConnection {
  private let url: PulsarURL
  internal let logger: Logger
  private let eventLoopGroup: EventLoopGroup
  private var channel: NIOCore.Channel?
  internal var commandBuilder = PulsarCommandBuilder()
  internal let authentication: Authentication?
  private let encryptionPolicy: EncryptionPolicy
  private let authRefreshInterval: TimeInterval

  private var _state: ConnectionState = .disconnected
  private let stateStream: AsyncStream<ConnectionState>
  private let stateContinuation: AsyncStream<ConnectionState>.Continuation

  internal var pendingRequests:
    [UInt64: AsyncThrowingStream<Pulsar_Proto_BaseCommand, Error>.Continuation] = [:]
  private var asyncChannel: NIOAsyncChannel<PulsarFrame, PulsarFrame>?
  private var outboundWriter: NIOAsyncChannelOutboundWriter<PulsarFrame>?
  internal var channelManager: ChannelManager?
  private var backgroundProcessingTask: Task<Void, Never>?
  internal var healthMonitoringTask: Task<Void, Never>?
  private var frameProcessingStarted = false
  private var authRefreshTask: Task<Void, Never>?

  // Statistics tracking
  internal var connectedAt: Date?
  internal var totalMessagesSent: UInt64 = 0
  internal var totalMessagesReceived: UInt64 = 0
  internal var totalBytesSent: UInt64 = 0
  internal var totalBytesReceived: UInt64 = 0

  var state: ConnectionState { _state }
  var stateChanges: AsyncStream<ConnectionState> { stateStream }

  init(
    url: PulsarURL, eventLoopGroup: EventLoopGroup, logger: Logger,
    authentication: Authentication? = nil, encryptionPolicy: EncryptionPolicy = .preferUnencrypted,
    authRefreshInterval: TimeInterval = 30.0
  ) {
    self.url = url
    self.eventLoopGroup = eventLoopGroup
    self.logger = logger
    self.authentication = authentication
    self.encryptionPolicy = encryptionPolicy
    self.authRefreshInterval = authRefreshInterval
    self.channelManager = ChannelManager(logger: logger)

    (self.stateStream, self.stateContinuation) = AsyncStream<ConnectionState>.makeStream()
  }

  /// Get the channel manager
  func getChannelManager() -> ChannelManager {
    return channelManager!
  }

  deinit {
    // Cancel any running tasks immediately
    backgroundProcessingTask?.cancel()
    healthMonitoringTask?.cancel()
    authRefreshTask?.cancel()
    stateContinuation.finish()
    
    // Clear any remaining pending requests
    for (_, continuation) in pendingRequests {
      continuation.finish(throwing: PulsarClientError.connectionFailed("Connection deallocated"))
    }
    pendingRequests.removeAll()
  }

  func connect() async throws {
    guard _state == .disconnected || _state == .closed else {
      throw PulsarClientError.connectionFailed("Already connected or connecting")
    }

    logger.debug("Starting connection", metadata: ["host": "\(url.host)", "port": "\(url.port)"])

    // Validate encryption policy
    if encryptionPolicy.isEncryptionRequired && !url.isSSL {
      throw PulsarClientError.connectionFailed(
        "Encryption is required by policy but URL is not SSL: \(url)")
    }

    if encryptionPolicy == .enforceUnencrypted && url.isSSL {
      throw PulsarClientError.connectionFailed(
        "Unencrypted connection is required by policy but URL is SSL: \(url)")
    }

    updateState(.connecting)
    logger.debug("State updated to connecting")

    do {
      // Prepare CONNECT command with authentication if provided
      let connectCommand: Pulsar_Proto_BaseCommand
      if let auth = authentication {
        logger.debug("Using authentication")
        let authData = try await auth.getAuthenticationData()
        connectCommand = commandBuilder.connect(
          authMethodName: auth.authenticationMethodName,
          authData: authData
        )
      } else {
        logger.debug("No authentication required")
        connectCommand = commandBuilder.connect()
      }
      
      logger.debug("Protocol version", metadata: ["version": "\(connectCommand.connect.protocolVersion)"])
      logger.debug("Client version", metadata: ["version": "\(connectCommand.connect.clientVersion)"])

      logger.debug("Establishing TCP connection")
      
      // Create channel with traditional bootstrap
      let bootstrap = ClientBootstrap(group: eventLoopGroup)
        .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
        .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
        .channelInitializer { channel in
          self.setupChannelPipeline(channel: channel, connectCommand: connectCommand)
        }
      
      let channel = try await bootstrap.connect(host: url.host, port: url.port).get()
      self.channel = channel
      
      // Wrap channel with NIOAsyncChannel on the event loop
      let asyncChannel = try await channel.eventLoop.submit {
        try NIOAsyncChannel<PulsarFrame, PulsarFrame>(
          wrappingChannelSynchronously: channel,
          configuration: NIOAsyncChannel.Configuration(
            backPressureStrategy: .init(lowWatermark: 2, highWatermark: 100),
            isOutboundHalfClosureEnabled: false,
            inboundType: PulsarFrame.self,
            outboundType: PulsarFrame.self
          )
        )
      }.get()
      
      self.asyncChannel = asyncChannel
      logger.debug("TCP connection established")

      // Start background processing
      // The ConnectedFrameHandler will handle the handshake internally:
      // 1. It sends the CONNECT command when channel becomes active
      // 2. It waits for the CONNECTED response
      // 3. Only after receiving CONNECTED does it propagate channelActive
      // 4. This ensures no frames are processed until handshake completes
      startBackgroundProcessing(asyncChannel)
      
      // The handler will complete the handshake and then fire channelActive
      // When channelActive fires, our background processing will start
      // We just need to wait for our processing to start, which indicates handshake success
      await waitForFrameProcessing()
      
      // At this point, handshake has completed successfully
      updateState(.connected)
      connectedAt = Date()
      logger.debug("Successfully connected to Pulsar", metadata: ["host": "\(url.host)", "port": "\(url.port)"])
      
      // Start authentication refresh task if needed
      startAuthenticationRefreshTask()

    } catch {
      logger.error("Connection failed: \(error)")
      updateState(.faulted(error))
      throw PulsarClientError.connectionFailed("Failed to connect: \(error)")
    }
  }

  func send(frame: PulsarFrame) async throws {
    guard _state == .connected else {
      throw PulsarClientError.connectionFailed("Not connected")
    }
    try await sendFrame(frame)
  }

  func isConnected() async -> Bool {
     return _state == .connected
  }
  
  func close() async {
    guard _state != .closing && _state != .closed else {
      logger.debug("Connection already closing or closed")
      return
    }

    logger.debug("Starting connection close process")
    updateState(.closing)

    // Fail all pending requests
    await failAllPendingRequests(error: PulsarClientError.connectionFailed("Connection closing"))

    // Close channels registered to this connection
    await channelManager?.closeAll()

    // Finish outbound writer if available
    outboundWriter?.finish()
    self.outboundWriter = nil
    
    // Close the underlying channel
    if let channel = channel {
      logger.debug("Closing NIO channel")
      do {
        try await channel.close()
        logger.debug("NIO channel closed successfully")
      } catch {
        logger.debug("Error closing NIO channel", metadata: ["error": "\(error)"])
      }
    }
    
    self.asyncChannel = nil
    self.channel = nil

    // Close any background tasks
    backgroundProcessingTask?.cancel()
    backgroundProcessingTask = nil

    authRefreshTask?.cancel()
    authRefreshTask = nil

    healthMonitoringTask?.cancel()
    healthMonitoringTask = nil

    updateState(.closed)
    logger.debug("Connection closed")
  }

  // MARK: - Internal Methods

  internal func updateState(_ newState: ConnectionState) {
    _state = newState
    stateContinuation.yield(newState)
  }
  
  /// Handle connection error by updating state and failing all pending operations
  private func handleConnectionError(_ error: Error) async {
    logger.error("Connection error occurred", metadata: ["error": "\(error)"])
    updateState(.faulted(error))
    
    // Fail all pending requests
    await failAllPendingRequests(error: error)
    
    // Notify channel manager of connection failure
    await channelManager?.handleConnectionFailure(error: error)
    
    // Close the connection
    await close()
  }
  
  /// Fail all pending requests with the given error
  private func failAllPendingRequests(error: Error) async {
    for (requestId, continuation) in pendingRequests {
      logger.debug("Failing pending request", metadata: ["requestId": "\(requestId)"])
      continuation.finish(throwing: error)
    }
    pendingRequests.removeAll()
  }

  private nonisolated func setupChannelPipeline(
    channel: NIOCore.Channel, connectCommand: Pulsar_Proto_BaseCommand
  ) -> EventLoopFuture<Void> {

    var handlers: [ChannelHandler] = []

    // Add SSL handler if needed
    if url.isSSL {
      do {
        let sslContext = try NIOSSLContext(configuration: .makeClientConfiguration())
        let sslHandler = try NIOSSLClientHandler(context: sslContext, serverHostname: url.host)
        handlers.append(sslHandler)
      } catch {
        return channel.eventLoop.makeFailedFuture(error)
      }
    }

    // Add a raw data logger for debugging
    handlers.append(RawDataLogger())

    // Add frame codec handlers (decoder and encoder)
    handlers.append(ByteToMessageHandler(PulsarFrameByteDecoder()))
    handlers.append(MessageToByteHandler(PulsarFrameByteEncoder()))
    
    // Add the handler that manages connection handshake
    let connectedHandler = ConnectedFrameHandler(
      connectCommand: connectCommand,
      handshakeTimeout: 10.0,
      logger: logger
    )
    handlers.append(connectedHandler)
    
    // NIOAsyncChannel will be added after this pipeline setup

    return channel.pipeline.addHandlers(handlers, position: .last)
  }

  private func setOutboundWriter(_ writer: NIOAsyncChannelOutboundWriter<PulsarFrame>) {
    self.outboundWriter = writer
  }
  
  internal func sendFrame(_ frame: PulsarFrame) async throws {
    guard let outboundWriter = outboundWriter else {
      throw PulsarClientError.connectionFailed("No channel available")
    }

    // Debug: Log the frame being sent
    let encoder = PulsarFrameEncoder()
    if let data = try? encoder.encode(frame: frame) {
      let hexString = data.prefix(100).map { String(format: "%02x", $0) }.joined(separator: " ")
      logger.trace("Sending frame", metadata: ["bytes": "\(data.count)", "preview": "\(hexString)..."])
      totalBytesSent += UInt64(data.count)
    }

    // NIOAsyncChannel handles backpressure automatically
    try await outboundWriter.write(frame)

    // Update statistics
    totalMessagesSent += 1
  }

  /// Start background processing (equivalent to C# Setup method)
  private func startBackgroundProcessing(_ asyncChannel: NIOAsyncChannel<PulsarFrame, PulsarFrame>) {
    logger.trace("Creating background processing task")
    // Use structured Task instead of detached to ensure proper cleanup
    backgroundProcessingTask = Task { [weak self] in
      guard let self = self else { return }
      self.logger.trace("Background processing task started")
      await self.markFrameProcessingStarted()
      
      // Use executeThenClose for scoped access to inbound/outbound streams
      do {
        try await asyncChannel.executeThenClose { inbound, outbound in
          try await self.processIncomingFramesContinuously(inbound: inbound, outbound: outbound)
        }
      } catch {
        if !Task.isCancelled {
          self.logger.error("Background processing error", metadata: ["error": "\(error)"])
          // Propagate error to connection state and fail all pending operations
          await self.handleConnectionError(error)
        }
      }
    }
    logger.trace("Background processing task created")
  }

  /// Mark that frame processing has started
  private func markFrameProcessingStarted() {
    frameProcessingStarted = true
    logger.trace("Frame processing marked as started")
  }

  /// Wait for frame processing to start
  private func waitForFrameProcessing() async {
    var attempts = 0
    while !frameProcessingStarted && attempts < 50 {  // Max 500ms wait
      try? await Task.sleep(nanoseconds: 10_000_000)  // 10ms
      attempts += 1
    }
    if frameProcessingStarted {
      logger.trace("Frame processing confirmed started", metadata: ["attempts": "\(attempts)"])
    } else {
      logger.debug("Frame processing start timeout", metadata: ["attempts": "\(attempts)"])
    }
  }

  /// Set connected timestamp
  private func setConnectedAt(_ date: Date) {
    connectedAt = date
  }
  
  /// Get request ID from response frames
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
    case .newTxnResponse:
      return command.newTxnResponse.requestID
    case .addPartitionToTxnResponse:
      return command.addPartitionToTxnResponse.requestID
    case .endTxnResponse:
      return command.endTxnResponse.requestID
    case .addSubscriptionToTxnResponse:
      return command.addSubscriptionToTxnResponse.requestID
    default:
      return nil
    }
  }

  /// Continuously process incoming frames (equivalent to C# ProcessIncomingFrames)
  private func processIncomingFramesContinuously(
    inbound: NIOAsyncChannelInboundStream<PulsarFrame>,
    outbound: NIOAsyncChannelOutboundWriter<PulsarFrame>
  ) async throws {
    // Store the outbound writer for sending frames
    setOutboundWriter(outbound)
    
    logger.trace("Starting continuous frame processing")

    var frameCount = 0
    do {
      for try await frame in inbound {
        frameCount += 1
        logger.trace("Received frame", metadata: [
          "frameNumber": "\(frameCount)", 
          "type": "\(frame.command.type)"
        ])

        // Handle frames that complete or fail continuations
        switch frame.command.type {
        case .sendReceipt:
          // SendReceipt completes a producer send operation
          logger.trace("SendReceipt details", metadata: [
            "producerId": "\(frame.command.sendReceipt.producerID)", 
            "sequenceId": "\(frame.command.sendReceipt.sequenceID)"
          ])
          // Forward to producer channel for handling
          Task { [weak self] in
            guard let self = self else { return }
            if let producerChannel = await self.channelManager?.getProducer(id: frame.command.sendReceipt.producerID) {
              await producerChannel.handleSendReceipt(frame.command.sendReceipt)
            }
          }
          
        case .sendError:
          // SendError fails a producer send operation
          logger.error("SendError received", metadata: [
            "producerId": "\(frame.command.sendError.producerID)",
            "sequenceId": "\(frame.command.sendError.sequenceID)",
            "error": "\(frame.command.sendError.error)"
          ])
          // Forward to producer channel for handling
          // Note: ProducerChannel handles send errors internally via SendOperation
          // We could enhance this to fail the specific send operation if needed
          
        case .error:
          // Error fails a pending request continuation
          logger.error("Error frame received", metadata: [
            "requestId": "\(frame.command.error.requestID)",
            "message": "\(frame.command.error.message)"
          ])
          // Check if this error is for a pending request
          if let continuation = pendingRequests.removeValue(forKey: frame.command.error.requestID) {
            // Map server error code to appropriate client error
            let error: PulsarClientError
            switch frame.command.error.error {
            case .authenticationError, .authorizationError:
              error = .authorizationFailed(frame.command.error.message)
            case .metadataError:
              error = .metadataFailed(frame.command.error.message)
            case .persistenceError:
              error = .persistenceFailed(frame.command.error.message)
            case .checksumError:
              error = .checksumFailed
            case .consumerBusy:
              error = .consumerBusy(frame.command.error.message)
            case .producerBusy:
              error = .producerBusy(frame.command.error.message)
            case .producerBlockedQuotaExceededError:
              error = .producerBlockedQuotaExceeded
            case .topicTerminatedError:
              error = .topicTerminated(frame.command.error.message)
            case .incompatibleSchema:
              error = .incompatibleSchema(frame.command.error.message)
            case .consumerAssignError:
              error = .consumerAssignFailed(frame.command.error.message)
            case .notAllowedError:
              error = .notAllowed(frame.command.error.message)
            default:
              error = .protocolError("Server error: \(frame.command.error.message)")
            }
            continuation.finish(throwing: error)
          }
          
        case .producerSuccess, .success, .lookupResponse, .partitionedMetadataResponse,
             .getLastMessageIDResponse, .getSchemaResponse, .newTxnResponse,
             .addPartitionToTxnResponse, .endTxnResponse, .addSubscriptionToTxnResponse:
          // These are response frames that complete pending requests
          if let requestId = getResponseRequestId(from: frame.command),
             let continuation = pendingRequests.removeValue(forKey: requestId) {
            logger.debug("Completing request", metadata: [
              "requestId": "\(requestId)",
              "responseType": "\(frame.command.type)"
            ])
            continuation.yield(frame.command)
            continuation.finish()
          } else {
            logger.warning("Received response without matching request", metadata: [
              "type": "\(frame.command.type)"
            ])
          }
          
        default:
          // Process all other frames through the enhanced handler
          // This includes server-initiated frames like ping, message, etc.
          handleIncomingFrame(frame)
        }

        // Exit if connection is no longer active or task is cancelled
        if _state == .closed || _state == .closing || Task.isCancelled {
          logger.trace("Stopping frame processing - connection closed or task cancelled")
          break
        }
      }
    } catch {
      logger.error("Frame processing error", metadata: ["error": "\(error)"])
      // Re-throw the error to propagate it up
      throw error
    }

    logger.trace("Frame processing loop ended", metadata: ["frameCount": "\(frameCount)"])
  }

  // MARK: - Authentication Refresh

  /// Start authentication refresh task if authentication supports it
  private func startAuthenticationRefreshTask() {
    guard let auth = authentication else { return }

    authRefreshTask = Task { [weak self] in
      guard let self = self else { return }

      logger.debug("Starting authentication refresh task")

      while !Task.isCancelled {
        let currentState = await self._state
        guard currentState == .connected else { break }
        do {
          // Check if authentication needs refresh
          if await auth.needsRefresh() {
            logger.debug("Authentication needs refresh")

            // Get fresh authentication data
            let authData = try await auth.getAuthenticationData()

            // Create auth data
            var authDataProto = Pulsar_Proto_AuthData()
            authDataProto.authMethodName = auth.authenticationMethodName
            authDataProto.authData = authData

            // Send auth response (broker will validate and update)
            let authResponse = await commandBuilder.authResponse(response: authDataProto)
            let frame = PulsarFrame(command: authResponse)

            try await sendFrame(frame)
            logger.debug("Sent refreshed authentication data")
          }

          // Wait before next check
          try await Task.sleep(nanoseconds: UInt64(authRefreshInterval * 1_000_000_000))

        } catch {
          if !Task.isCancelled {
            logger.error("Authentication refresh failed: \(error)")
            // Continue trying unless task is cancelled
            try? await Task.sleep(nanoseconds: 5_000_000_000)  // Wait 5 seconds before retry
          }
        }
      }

      logger.debug("Authentication refresh task ended")
    }
  }

}

// MARK: - Raw Data Logger

final class RawDataLogger: ChannelDuplexHandler, @unchecked Sendable {
  typealias InboundIn = ByteBuffer
  typealias InboundOut = ByteBuffer
  typealias OutboundIn = ByteBuffer
  typealias OutboundOut = ByteBuffer

  private let logger = Logger(label: "RawDataLogger")

  func channelRead(context: ChannelHandlerContext, data: NIOAny) {
    let buffer = self.unwrapInboundIn(data)
    let readableBytes = buffer.readableBytes

    if readableBytes > 0 {
      var bufferCopy = buffer
      if let bytes = bufferCopy.readBytes(length: min(readableBytes, 50)) {
        let hexString = bytes.map { String(format: "%02x", $0) }.joined(separator: " ")
        logger.trace("Raw incoming data", metadata: ["bytes": "\(readableBytes)", "preview": "\(hexString)..."])
      }
    }

    // Forward the data unchanged
    context.fireChannelRead(data)
  }

  func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
    let buffer = self.unwrapOutboundIn(data)
    let readableBytes = buffer.readableBytes

    if readableBytes > 0 {
      var bufferCopy = buffer
      if let bytes = bufferCopy.readBytes(length: min(readableBytes, 100)) {
        let hexString = bytes.map { String(format: "%02x", $0) }.joined(separator: " ")
        logger.trace("Raw outgoing data", metadata: ["bytes": "\(readableBytes)", "preview": "\(hexString)..."])
      }
    }

    // Forward the data unchanged
    context.write(data, promise: promise)
  }
}

// MARK: - Frame Codec

final class PulsarFrameByteDecoder: ByteToMessageDecoder, @unchecked Sendable {
  typealias InboundOut = PulsarFrame

  private let frameDecoder = PulsarFrameDecoder()
  private let logger = Logger(label: "PulsarFrameByteDecoder")

  func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
    logger.trace("Decode called", metadata: ["readableBytes": "\(buffer.readableBytes)"])

    guard buffer.readableBytes >= 4 else {
      return .needMoreData
    }

    // Peek at total size - NIO uses native byte order, we need big endian
    let totalSizeBytes = buffer.getBytes(at: buffer.readerIndex, length: 4)!
    let totalSize =
      UInt32(totalSizeBytes[0]) << 24 | UInt32(totalSizeBytes[1]) << 16 | UInt32(totalSizeBytes[2])
      << 8 | UInt32(totalSizeBytes[3])
    logger.trace("Frame total size: \(totalSize)")

    guard buffer.readableBytes >= Int(totalSize) + 4 else {
      logger.trace("Need more data: have \(buffer.readableBytes), need \(Int(totalSize) + 4)")
      return .needMoreData
    }

    // Read the complete frame
    guard let bytes = buffer.readBytes(length: Int(totalSize) + 4) else {
      return .needMoreData
    }
    let data = Data(bytes)

    // Log first bytes
    let hexString = data.prefix(50).map { String(format: "%02x", $0) }.joined(separator: " ")
    logger.trace("Received frame", metadata: [
      "bytes": "\(data.count)",
      "preview": "\(hexString)..."
    ])

    if let frame = try frameDecoder.decode(from: data) {
      logger.trace("Decoded frame", metadata: ["type": "\(frame.command.type)"])
      context.fireChannelRead(wrapInboundOut(frame))
    } else {
      logger.error("Failed to decode frame")
    }

    return .continue
  }
}

final class PulsarFrameByteEncoder: MessageToByteEncoder, @unchecked Sendable {
  typealias OutboundIn = PulsarFrame

  private let frameEncoder = PulsarFrameEncoder()

  func encode(data: PulsarFrame, out: inout ByteBuffer) throws {
    let encoded = try frameEncoder.encode(frame: data)
    out.writeBytes(encoded)
  }
}


// MARK: - URL Parsing

struct PulsarURL: Sendable {
  let scheme: String
  let host: String
  let port: Int
  let path: String

  var isSSL: Bool {
    scheme == "pulsar+ssl"
  }

  var socketAddress: SocketAddress? {
    return try? SocketAddress(ipAddress: host, port: port)
  }

  init(string: String) throws {
    guard let url = URL(string: string),
      let host = url.host,
      let scheme = url.scheme
    else {
      throw PulsarClientError.invalidServiceUrl(string)
    }

    self.scheme = scheme
    self.host = host
    self.port = url.port ?? (scheme == "pulsar+ssl" ? 6651 : 6650)
    self.path = url.path
  }
}

// MARK: - Utilities

internal func withTimeout<T: Sendable>(
  seconds: TimeInterval, operation: @escaping @Sendable () async throws -> T
) async throws -> T {
  try await withThrowingTaskGroup(of: T.self) { group in
    group.addTask {
      try await operation()
    }

    group.addTask {
      try await Task.sleep(nanoseconds: UInt64(seconds * 1_000_000_000))
      throw PulsarClientError.timeout("Operation timed out")
    }

    let result = try await group.next()!
    group.cancelAll()
    return result
  }
}
