import Foundation
import NIOCore
import NIOPosix
import Logging

// MARK: - Enhanced Connection Pool

extension ConnectionPool {
    
    /// Get connection for a specific topic
    func getConnectionForTopic(_ topic: String) async throws -> Connection {
        // First, lookup the broker for this topic
        let lookupResult = try await lookup(topic: topic)
        
        // Get or create connection to the broker
        let brokerUrl = lookupResult.brokerUrl
        return try await getConnection(for: brokerUrl)
    }
    
    /// Enhanced lookup with retries and caching
    func lookup(topic: String) async throws -> BrokerLookupResult {
        // Try to get a connection to the service URL
        let connection = try await getConnection(for: serviceUrl)
        
        var attempts = 0
        let maxAttempts = 3
        
        while attempts < maxAttempts {
            do {
                let lookupResponse = try await connection.lookup(topic: topic)
                
                switch lookupResponse.response {
                case .connect(let brokerUrl, let brokerUrlTls):
                    // Direct connection to broker
                    return BrokerLookupResult(
                        brokerUrl: brokerUrl,
                        brokerUrlTls: brokerUrlTls,
                        proxyThroughServiceUrl: lookupResponse.proxyThroughServiceUrl
                    )
                    
                case .redirect(let brokerUrl, let brokerUrlTls):
                    // Need to connect to a different broker for lookup
                    logger.debug("Redirected to \(brokerUrl) for topic \(topic)")
                    
                    // If we should proxy through service URL, use the original connection
                    if lookupResponse.proxyThroughServiceUrl {
                        return BrokerLookupResult(
                            brokerUrl: serviceUrl,
                            brokerUrlTls: nil,
                            proxyThroughServiceUrl: true
                        )
                    }
                    
                    // Otherwise, try lookup on the redirected broker
                    let redirectConnection = try await getConnection(for: brokerUrl)
                    return try await redirectConnection.lookup(topic: topic).toBrokerLookupResult()
                    
                case .failed(let error):
                    throw mapServerError(error)
                }
                
            } catch {
                attempts += 1
                if attempts >= maxAttempts {
                    throw error
                }
                
                logger.warning("Lookup attempt \(attempts) failed for topic \(topic): \(error)")
                
                // Wait before retry with exponential backoff
                let backoffSeconds = Double(attempts) * 2.0
                try await Task.sleep(nanoseconds: UInt64(backoffSeconds * 1_000_000_000))
            }
        }
        
        throw PulsarClientError.lookupFailed("Failed to lookup topic after \(maxAttempts) attempts")
    }
    
    /// Monitor connection health and remove failed connections
    func startHealthMonitoring() {
        Task {
            while true {
                try await Task.sleep(nanoseconds: 60_000_000_000) // Check every minute
                
                var failedConnections: [String] = []
                
                for (url, connection) in connections {
                    let state = await connection.state
                    switch state {
                    case .faulted, .closed:
                        failedConnections.append(url)
                    default:
                        break
                    }
                }
                
                // Remove failed connections
                for url in failedConnections {
                    connections.removeValue(forKey: url)
                    logger.info("Removed failed connection to \(url)")
                }
            }
        }
    }
    
    /// Get statistics about the connection pool
    func getStatistics() async -> ConnectionPoolStatistics {
        var stats = ConnectionPoolStatistics()
        
        for (url, connection) in connections {
            let state = await connection.state
            stats.totalConnections += 1
            
            switch state {
            case .connected:
                stats.activeConnections += 1
            case .connecting:
                stats.connectingConnections += 1
            case .faulted:
                stats.faultedConnections += 1
            default:
                break
            }
            
            stats.connectionsByBroker[url] = state
        }
        
        return stats
    }
}

// MARK: - Helper Types

struct ConnectionPoolStatistics {
    var totalConnections = 0
    var activeConnections = 0
    var connectingConnections = 0
    var faultedConnections = 0
    var connectionsByBroker: [String: ConnectionState] = [:]
}

// MARK: - Error Mapping

private func mapServerError(_ error: Pulsar_Proto_ServerError) -> Error {
    switch error {
    case .unknownError:
        return PulsarClientError.unknownError("Unknown server error")
    case .metadataError:
        return PulsarClientError.lookupFailed("Metadata error")
    case .persistenceError:
        return PulsarClientError.unknownError("Persistence error")
    case .authenticationError:
        return PulsarClientError.authenticationFailed("Authentication error")
    case .authorizationError:
        return PulsarClientError.authenticationFailed("Authorization error")
    case .consumerBusy:
        return PulsarClientError.consumerBusy("Consumer busy")
    case .serviceNotReady:
        return PulsarClientError.connectionFailed("Service not ready")
    case .producerBlockedQuotaExceededError:
        return PulsarClientError.producerQueueFull
    case .producerBlockedQuotaExceededException:
        return PulsarClientError.producerQueueFull
    case .checksumError:
        return PulsarClientError.checksumFailed
    case .unsupportedVersionError:
        return PulsarClientError.protocolError("Unsupported version")
    case .topicNotFound:
        return PulsarClientError.topicNotFound("Topic not found")
    case .subscriptionNotFound:
        return PulsarClientError.subscriptionNotFound("Subscription not found")
    case .consumerNotFound:
        return PulsarClientError.consumerBusy("Consumer not found")
    case .tooManyRequests:
        return PulsarClientError.tooManyRequests
    case .topicTerminatedError:
        return PulsarClientError.topicNotFound("Topic terminated")
    case .producerBusy:
        return PulsarClientError.producerBusy("Producer busy")
    case .invalidTopicName:
        return PulsarClientError.topicNotFound("Invalid topic name")
    case .incompatibleSchema:
        return PulsarClientError.schemaError("Incompatible schema")
    case .consumerAssignError:
        return PulsarClientError.consumerBusy("Consumer assign error")
    case .transactionCoordinatorNotFound:
        return PulsarClientError.transactionCoordinatorNotFound
    case .invalidTxnStatus:
        return PulsarClientError.transactionOperationFailed("Invalid transaction status")
    case .notAllowedError:
        return PulsarClientError.unsupportedOperation("Operation not allowed")
    case .transactionConflict:
        return PulsarClientError.transactionConflict
    case .transactionNotFound:
        return PulsarClientError.transactionNotFound
    case .producerFenced:
        return PulsarClientError.producerBusy("Producer fenced")
    default:
        return PulsarClientError.unknownError("Unknown server error: \(error)")
    }
}

// MARK: - LookupResponse Extension

extension LookupResponse {
    func toBrokerLookupResult() -> BrokerLookupResult {
        switch response {
        case .connect(let brokerUrl, let brokerUrlTls), .redirect(let brokerUrl, let brokerUrlTls):
            return BrokerLookupResult(
                brokerUrl: brokerUrl,
                brokerUrlTls: brokerUrlTls,
                proxyThroughServiceUrl: proxyThroughServiceUrl
            )
        case .failed:
            // This shouldn't be called for failed responses
            return BrokerLookupResult(
                brokerUrl: "",
                brokerUrlTls: nil,
                proxyThroughServiceUrl: false
            )
        }
    }
}