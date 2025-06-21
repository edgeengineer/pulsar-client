import Foundation
import NIOCore
import NIOPosix
import Logging

/// Connection pool for managing multiple Pulsar connections
actor ConnectionPool {
    internal let eventLoopGroup: EventLoopGroup
    internal let logger: Logger
    internal var connections: [String: Connection] = [:]
    internal let serviceUrl: String
    
    init(serviceUrl: String, eventLoopGroup: EventLoopGroup? = nil, logger: Logger = Logger(label: "ConnectionPool")) {
        self.serviceUrl = serviceUrl
        self.eventLoopGroup = eventLoopGroup ?? MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
        self.logger = logger
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
                // Remove the failed connection
                connections.removeValue(forKey: brokerUrl)
            default:
                // Wait for connection to complete
                return existingConnection
            }
        }
        
        // Create new connection
        let url = try PulsarURL(string: brokerUrl)
        let connection = Connection(url: url, eventLoopGroup: eventLoopGroup, logger: logger)
        connections[brokerUrl] = connection
        
        do {
            try await connection.connect()
            return connection
        } catch {
            // Remove failed connection
            connections.removeValue(forKey: brokerUrl)
            throw error
        }
    }
    
    
    /// Close all connections
    func close() async {
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