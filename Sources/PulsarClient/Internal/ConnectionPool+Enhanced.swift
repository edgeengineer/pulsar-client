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
        logger.info("Starting lookup for topic: \(topic)")
        
        // Try to get a connection to the service URL
        logger.info("Getting connection for service URL: \(serviceUrl)")
        let connection = try await getConnection(for: serviceUrl)
        logger.info("Got connection for service URL")
        
        var attempts = 0
        let maxAttempts = 3
        
        while attempts < maxAttempts {
            do {
                logger.info("Lookup attempt \(attempts + 1) for topic: \(topic)")
                let lookupResponse = try await connection.lookup(topic: topic)
                
                switch lookupResponse.response {
                case .connect(let brokerUrl, let brokerUrlTls):
                    // Direct connection to broker
                    logger.info("Lookup successful - connect to \(brokerUrl)")
                    return BrokerLookupResult(
                        brokerUrl: brokerUrl,
                        brokerUrlTls: brokerUrlTls,
                        proxyThroughServiceUrl: lookupResponse.proxyThroughServiceUrl
                    )
                    
                case .redirect(let brokerUrl, _):
                    // Need to connect to a different broker for lookup
                    logger.info("Redirected to \(brokerUrl) for topic \(topic)")
                    
                    // If we should proxy through service URL, use the original connection
                    if lookupResponse.proxyThroughServiceUrl {
                        logger.info("Using proxy through service URL")
                        return BrokerLookupResult(
                            brokerUrl: serviceUrl,
                            brokerUrlTls: nil,
                            proxyThroughServiceUrl: true
                        )
                    }
                    
                    // Otherwise, try lookup on the redirected broker
                    logger.info("Following redirect to \(brokerUrl)")
                    let redirectConnection = try await getConnection(for: brokerUrl)
                    return try await redirectConnection.lookup(topic: topic).toBrokerLookupResult()
                    
                case .failed(let error):
                    logger.error("Lookup failed with server error: \(error)")
                    throw mapServerError(error)
                }
                
            } catch {
                attempts += 1
                if attempts >= maxAttempts {
                    logger.error("Lookup failed after \(maxAttempts) attempts: \(error)")
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
        // keep handle so it can be cancelled later
        healthMonitoringTask = Task { [weak self] in 
                guard let self = self else { return } 
                while !Task.isCancelled {
                    try? await Task.sleep(nanoseconds: 60_000_000_000) // Check every minute
                    
                    if (Task.isCancelled) { break }

                    var failed: [String] = []
                    for (url, connection) in await self.connections {
                        if case .faulted = await connection.state { failed.append(url) }
                        if case .closed  = await connection.state { failed.append(url) }
                    }

                    await self.removeConnections(failed)
                }
            }
    }    

    /// Detached helper function to remove failed connections
    private func removeConnections(_ urls: [String]) async {
        for url in urls {
            connections.removeValue(forKey: url)
            logger.info("Removed failed connection to \(url)")
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