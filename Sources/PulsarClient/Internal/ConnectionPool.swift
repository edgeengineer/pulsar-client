import Foundation
import Logging
import NIOCore
import NIOPosix

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

  /// Get or create a connection for the given broker URL
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

    // Create new connection
    let url = try PulsarURL(string: brokerUrl)
    let connection = Connection(
      url: url, eventLoopGroup: eventLoopGroup, logger: logger, authentication: authentication,
      encryptionPolicy: encryptionPolicy, authRefreshInterval: authRefreshInterval)
    connections[brokerUrl] = connection

    // Start the connection in a background task
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
        throw PulsarClientError.connectionFailed("Connection failed to establish")
      }
      try? await Task.sleep(nanoseconds: 100_000_000)  // 100ms
    }
    
    // Timeout
    connections.removeValue(forKey: brokerUrl)
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
