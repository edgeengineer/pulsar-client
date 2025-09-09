import Foundation
import Logging
import NIOCore
import NIOPosix
import NIO

/// Connection pool for managing multiple Pulsar connections
actor ConnectionPool {
  internal let eventLoopGroup: EventLoopGroup
  internal let logger: Logger
  internal var connections: [String: Connection] = [:]
  internal let serviceUrl: String
  internal let authentication: Authentication?
  internal let encryptionPolicy: EncryptionPolicy
  internal let authRefreshInterval: TimeInterval

  // Handle to health monitoring task
  internal var healthMonitoringTask: Task<Void, Never>?

  init(
    serviceUrl: String, eventLoopGroup: EventLoopGroup? = nil,
    logger: Logger = Logger(label: "ConnectionPool"), authentication: Authentication? = nil,
    encryptionPolicy: EncryptionPolicy = .preferUnencrypted,
    authRefreshInterval: TimeInterval = 30.0
  ) {
    self.serviceUrl = serviceUrl
    self.eventLoopGroup =
      eventLoopGroup ?? MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    self.logger = logger
    self.authentication = authentication
    self.encryptionPolicy = encryptionPolicy
    self.authRefreshInterval = authRefreshInterval
  }

  func withConnection<T: Sendable>(
    for brokerUrl: String,
    operation: @escaping @Sendable (
      NIOAsyncChannelInboundStream<PulsarFrame>,
      NIOAsyncChannelOutboundWriter<PulsarFrame>
    ) async throws -> T
  ) async throws -> T {
    let url = try PulsarURL(string: brokerUrl)
    
    // Bootstrap TCP connection with Pulsar-specific pipeline
    let bootstrap = ClientBootstrap(group: eventLoopGroup)
      .channelOption(ChannelOptions.socketOption(.so_reuseaddr), value: 1)
      .channelOption(ChannelOptions.tcpOption(.tcp_nodelay), value: 1)
      .channelInitializer { channel in
        self.setupChannelPipeline(channel: channel, url: url)
      }
    
    let channel = try await bootstrap.connect(host: url.host, port: url.port).get()

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
    
    // Execute operation and then close (following the example pattern)
    return try await asyncChannel.executeThenClose { inbound, outbound in
      // Send CONNECT command
      let connectCommand: Pulsar_Proto_BaseCommand
      let commandBuilder = PulsarCommandBuilder()
      if let auth = authentication {
        let authData = try await auth.getAuthenticationData()
        var authBuffer = ByteBufferAllocator().buffer(capacity: authData.count)
        authBuffer.writeBytes(authData)
        connectCommand = commandBuilder.connect(
          authMethodName: auth.authenticationMethodName,
          authData: authBuffer
        )
      } else {
        connectCommand = commandBuilder.connect()
      }
      
      let connectFrame = PulsarFrame(command: connectCommand)
      try await outbound.write(connectFrame)
      
      // Wait for CONNECTED response
      for try await frame in inbound {
        if frame.command.type == .connected {
          // Connection established, execute the operation
          return try await operation(inbound, outbound)
        } else {
          throw PulsarClientError.connectionFailed("Unexpected response: \(frame.command.type)")
        }
      }
      
      throw PulsarClientError.connectionFailed("Connection closed before CONNECTED response")
    }
  }
  
  /// Helper to setup channel pipeline for raw connections
  private nonisolated func setupChannelPipeline(
    channel: NIOCore.Channel,
    url: PulsarURL
  ) -> EventLoopFuture<Void> {
    // Use the centralized pipeline builder for base pipeline
    return ChannelPipelineBuilder.setupBasePipeline(on: channel, for: url)
  }
  
  /// Get or create a persistent connection for the given broker URL
  // TODO: remove and replace with a service pattern
  func getConnection(for brokerUrl: String) async throws -> Connection {
    // Check if we already have a connection
    if let existingConnection = connections[brokerUrl] {
      let state = await existingConnection.state
      switch state {
      case .connected:
        return existingConnection
      case .faulted, .closed:
        // Remove the failed connection and create a new one
        logger.debug("Removing failed connection, will create new one", metadata: ["brokerUrl": "\(brokerUrl)"])
        connections.removeValue(forKey: brokerUrl)
        await existingConnection.close()
        // Fall through to create new connection
      case .connecting:
        // Wait for connection to establish
        for _ in 0..<30 {  // Max 3 seconds wait
          let currentState = await existingConnection.state
          if currentState == .connected {
            return existingConnection
          } else if case .faulted = currentState {
            break  // Will remove and recreate
          }
          try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
        }
        // If still not connected, remove and recreate
        connections.removeValue(forKey: brokerUrl)
        await existingConnection.close()
      default:
        // For other states, return existing and let caller handle
        return existingConnection
      }
    }

    // Create new persistent connection
    let url = try PulsarURL(string: brokerUrl)
    let connection = Connection(
      url: url, eventLoopGroup: eventLoopGroup, logger: logger, authentication: authentication,
      encryptionPolicy: encryptionPolicy, authRefreshInterval: authRefreshInterval)
    connections[brokerUrl] = connection

    // Start the connection in a background task (it will run for its lifetime)
    Task {
      do {
        try await connection.run()
      } catch {
        logger.error("Connection failed", metadata: ["error": "\(error)"])
        // Connection will handle its own state updates
      }
    }
    
    // Wait for connection to be established
    for _ in 0..<50 {  // Max 5 seconds wait
      let state = await connection.state
      if state == .connected {
        return connection
      } else if case .faulted = state {
        connections.removeValue(forKey: brokerUrl)
        await connection.close()
        throw PulsarClientError.connectionFailed("Connection failed to establish")
      }
      try await Task.sleep(nanoseconds: 100_000_000)  // 100ms
    }
    
    // Timeout
    connections.removeValue(forKey: brokerUrl)
    await connection.close()
    throw PulsarClientError.timeout("Connection establishment timeout")
  }

  /// Close all connections
  func close() async {
    healthMonitoringTask?.cancel()
    healthMonitoringTask = nil

    for (_, connection) in connections {
      await connection.close()
    }
    connections.removeAll()
  }

}

/// Result of a broker lookup
struct BrokerLookupResult: Sendable {
  let brokerUrl: String
  let brokerUrlTls: String?
  let proxyThroughServiceUrl: Bool
}
