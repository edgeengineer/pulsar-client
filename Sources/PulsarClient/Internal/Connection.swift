import Foundation
import NIOCore
import NIOPosix
import NIOSSL
import NIO
import Logging

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
            return true // Consider all faulted states equal
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
    private let authentication: Authentication?
    private let encryptionPolicy: EncryptionPolicy
    
    private var _state: ConnectionState = .disconnected
    private let stateStream: AsyncStream<ConnectionState>
    private let stateContinuation: AsyncStream<ConnectionState>.Continuation
    
    internal var pendingRequests: [UInt64: AsyncThrowingStream<Pulsar_Proto_BaseCommand, Error>.Continuation] = [:]
    private var frameHandler: PulsarFrameHandler?
    internal var channelManager: ChannelManager?
    
    // Statistics tracking
    internal var connectedAt: Date?
    internal var totalMessagesSent: UInt64 = 0
    internal var totalMessagesReceived: UInt64 = 0
    internal var totalBytesSent: UInt64 = 0
    internal var totalBytesReceived: UInt64 = 0
    
    var state: ConnectionState { _state }
    var stateChanges: AsyncStream<ConnectionState> { stateStream }
    
    init(url: PulsarURL, eventLoopGroup: EventLoopGroup, logger: Logger, authentication: Authentication? = nil, encryptionPolicy: EncryptionPolicy = .preferUnencrypted) {
        self.url = url
        self.eventLoopGroup = eventLoopGroup
        self.logger = logger
        self.authentication = authentication
        self.encryptionPolicy = encryptionPolicy
        self.channelManager = ChannelManager(logger: logger)
        
        (self.stateStream, self.stateContinuation) = AsyncStream<ConnectionState>.makeStream()
    }
    
    /// Get the channel manager
    func getChannelManager() -> ChannelManager {
        return channelManager!
    }
    
    deinit {
        stateContinuation.finish()
    }
    
    func connect() async throws {
        guard _state == .disconnected || _state == .closed else {
            throw PulsarClientError.connectionFailed("Already connected or connecting")
        }
        
        // Validate encryption policy
        if encryptionPolicy.isEncryptionRequired && !url.isSSL {
            throw PulsarClientError.connectionFailed("Encryption is required by policy but URL is not SSL: \(url)")
        }
        
        if encryptionPolicy == .enforceUnencrypted && url.isSSL {
            throw PulsarClientError.connectionFailed("Unencrypted connection is required by policy but URL is SSL: \(url)")
        }
        
        updateState(.connecting)
        
        do {
            let frameHandler = PulsarFrameHandler(connection: self)
            self.frameHandler = frameHandler
            
            let bootstrap = ClientBootstrap(group: eventLoopGroup)
                .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
                .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
                .channelInitializer { channel in
                    self.setupChannelPipeline(channel: channel, frameHandler: frameHandler)
                }
            
            let channel = try await bootstrap.connect(to: url.socketAddress).get()
            self.channel = channel
            
            // Send CONNECT command with authentication if provided
            let connectCommand: Pulsar_Proto_BaseCommand
            if let auth = authentication {
                let authData = try await auth.getAuthenticationData()
                connectCommand = commandBuilder.connect(
                    authMethodName: auth.authenticationMethodName,
                    authData: authData
                )
            } else {
                connectCommand = commandBuilder.connect()
            }
            let frame = PulsarFrame(command: connectCommand)
            try await sendFrame(frame)
            
            // Wait for CONNECTED response
            try await withTimeout(seconds: 10) {
                await self.waitForConnected()
            }
            
            updateState(.connected)
            connectedAt = Date()
            logger.info("Connected to Pulsar at \(url.host):\(url.port)")
            
        } catch {
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
    
    func close() async {
        guard _state == .connected else { return }
        
        updateState(.closing)
        
        // Cancel all pending requests
        for (_, continuation) in pendingRequests {
            continuation.finish(throwing: PulsarClientError.connectionFailed("Connection closing"))
        }
        pendingRequests.removeAll()
        
        // Close the channel
        try? await channel?.close()
        channel = nil
        
        updateState(.closed)
        logger.info("Connection closed")
    }
    
    // MARK: - Internal Methods
    
    internal func updateState(_ newState: ConnectionState) {
        _state = newState
        stateContinuation.yield(newState)
    }
    
    private nonisolated func setupChannelPipeline(channel: NIOCore.Channel, frameHandler: PulsarFrameHandler) -> EventLoopFuture<Void> {
        
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
        
        // Add frame codec handlers
        handlers.append(ByteToMessageHandler(PulsarFrameByteDecoder()))
        handlers.append(MessageToByteHandler(PulsarFrameByteEncoder()))
        handlers.append(frameHandler)
        
        return channel.pipeline.addHandlers(handlers)
    }
    
    internal func sendFrame(_ frame: PulsarFrame) async throws {
        guard let channel = channel else {
            throw PulsarClientError.connectionFailed("No channel available")
        }
        
        try await channel.writeAndFlush(NIOAny(frame))
        
        // Update statistics
        totalMessagesSent += 1
        
        // Calculate approximate frame size
        let encoder = PulsarFrameEncoder()
        if let data = try? encoder.encode(frame: frame) {
            totalBytesSent += UInt64(data.count)
        }
    }
    
    private func waitForConnected() async {
        await withCheckedContinuation { continuation in
            Task {
                for await frame in frameHandler?.incomingFrames ?? AsyncStream<PulsarFrame>.makeStream().stream {
                    if frame.command.type == .connected {
                        continuation.resume()
                        break
                    }
                }
            }
        }
    }
    
}

// MARK: - Frame Codec

final class PulsarFrameByteDecoder: ByteToMessageDecoder {
    typealias InboundOut = PulsarFrame
    
    private let frameDecoder = PulsarFrameDecoder()
    
    func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard buffer.readableBytes >= 4 else {
            return .needMoreData
        }
        
        // Peek at total size
        let totalSize = buffer.getInteger(at: buffer.readerIndex, as: UInt32.self)!.bigEndian
        
        guard buffer.readableBytes >= Int(totalSize) + 4 else {
            return .needMoreData
        }
        
        // Read the complete frame
        guard let bytes = buffer.readBytes(length: Int(totalSize) + 4) else {
            return .needMoreData
        }
        let data = Data(bytes)
        
        if let frame = try frameDecoder.decode(from: data) {
            context.fireChannelRead(wrapInboundOut(frame))
        }
        
        return .continue
    }
}

final class PulsarFrameByteEncoder: MessageToByteEncoder {
    typealias OutboundIn = PulsarFrame
    
    private let frameEncoder = PulsarFrameEncoder()
    
    func encode(data: PulsarFrame, out: inout ByteBuffer) throws {
        let encoded = try frameEncoder.encode(frame: data)
        out.writeBytes(encoded)
    }
}

// MARK: - Frame Handler

final class PulsarFrameHandler: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = PulsarFrame
    
    private weak var connection: Connection?
    private let frameStreamContinuation: AsyncStream<PulsarFrame>.Continuation
    let incomingFrames: AsyncStream<PulsarFrame>
    
    init(connection: Connection) {
        self.connection = connection
        (self.incomingFrames, self.frameStreamContinuation) = AsyncStream<PulsarFrame>.makeStream()
    }
    
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        frameStreamContinuation.yield(frame)
        
        Task {
            await connection?.handleIncomingFrame(frame)
        }
    }
    
    func errorCaught(context: ChannelHandlerContext, error: Error) {
        frameStreamContinuation.finish()
        context.close(promise: nil)
    }
    
    func channelInactive(context: ChannelHandlerContext) {
        frameStreamContinuation.finish()
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
    
    var socketAddress: SocketAddress {
        try! SocketAddress(ipAddress: host, port: port)
    }
    
    init(string: String) throws {
        guard let url = URL(string: string),
              let host = url.host,
              let scheme = url.scheme else {
            throw PulsarClientError.invalidServiceUrl(string)
        }
        
        self.scheme = scheme
        self.host = host
        self.port = url.port ?? (scheme == "pulsar+ssl" ? 6651 : 6650)
        self.path = url.path
    }
}

// MARK: - Utilities

internal func withTimeout<T: Sendable>(seconds: TimeInterval, operation: @escaping @Sendable () async throws -> T) async throws -> T {
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