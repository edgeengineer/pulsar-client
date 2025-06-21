import Foundation

/// Errors thrown by the Pulsar client
public enum PulsarClientError: Error, LocalizedError, Sendable {
    case authenticationFailed(String)
    case authorizationFailed(String)
    case channelNotReady
    case checksumFailed
    case clientClosed
    case clientDisposed
    case compressionFailed(String)
    case connectionFailed(String)
    case connectionSecurity(String)
    case consumerAssignFailed(String)
    case consumerBusy(String)
    case consumerClosed
    case consumerDisposed
    case consumerFaulted
    case consumerNotFound(String)
    case consumerQueueFull
    case decryptionFailed(String)
    case encryptionFailed(String)
    case incompatibleSchema(String)
    case invalidConfiguration(String)
    case invalidScheme(String)
    case invalidServiceUrl(String)
    case invalidTopicName(String)
    case invalidTopicsPattern(String)
    case invalidTransactionStatus(String)
    case lookupFailed(String)
    case messageAlreadyAcknowledged
    case messageNotFound
    case messageTooLarge
    case metadataFailed(String)
    case notAllowed(String)
    case notImplemented
    case persistenceFailed(String)
    case processingFailed(String)
    case producerBlockedQuotaExceeded
    case producerBusy(String)
    case producerClosed
    case producerDisposed
    case producerFaulted
    case producerFenced
    case producerQueueFull
    case protocolError(String)
    case readerClosed
    case readerDisposed
    case readerFaulted
    case schemaError(String)
    case schemaSerializationFailed(String)
    case serviceNotReady
    case subscriptionNotFound(String)
    case timeout(String)
    case topicNotFound(String)
    case topicTerminated(String)
    case tooManyRequests
    case transactionConflict
    case transactionCoordinatorNotFound
    case transactionNotFound
    case transactionOperationFailed(String)
    case unknownError(String)
    case unsupportedOperation(String)
    case unsupportedVersion
    
    public var errorDescription: String? {
        switch self {
        case .authenticationFailed(let reason):
            return "Authentication failed: \(reason)"
        case .authorizationFailed(let reason):
            return "Authorization failed: \(reason)"
        case .channelNotReady:
            return "Channel is not ready"
        case .checksumFailed:
            return "Checksum verification failed"
        case .clientClosed:
            return "Pulsar client is closed"
        case .clientDisposed:
            return "Pulsar client is disposed"
        case .compressionFailed(let reason):
            return "Compression failed: \(reason)"
        case .connectionFailed(let reason):
            return "Connection failed: \(reason)"
        case .connectionSecurity(let reason):
            return "A security error occured during connection: \(reason)"
        case .consumerAssignFailed(let reason):
            return "Failed to assign consumer: \(reason)"
        case .consumerBusy(let reason):
            return "Consumer busy: \(reason)"
        case .consumerClosed:
            return "Consumer is closed"
        case .consumerDisposed:
            return "Consumer is disposed"
        case .consumerFaulted:
            return "Consumer is faulted"
        case .consumerNotFound(let reason):
            return "Consumer not found: \(reason)"
        case .consumerQueueFull:
            return "Consumer queue is full"
        case .decryptionFailed(let reason):
            return "Decryption failed: \(reason)"
        case .encryptionFailed(let reason):
            return "Encryption failed: \(reason)"
        case .incompatibleSchema(let reason):
            return "Schema is incompatible: \(reason)"
        case .invalidConfiguration(let reason):
            return "Invalid configuration: \(reason)"
        case .invalidScheme(let reason):
            return "Invalid scheme: \(reason)"
        case .invalidServiceUrl(let url):
            return "Invalid service URL: \(url)"
        case .invalidTopicName(let reason):
            return "Invalid topic name: \(reason)"
        case .invalidTopicsPattern(let reason):
            return "Invalid topics pattern: \(reason)"
        case .invalidTransactionStatus(let reason):
            return "Invalid transaction status: \(reason)"
        case .lookupFailed(let reason):
            return "Lookup failed: \(reason)"
        case .messageAlreadyAcknowledged:
            return "Message already acknowledged"
        case .messageNotFound:
            return "Message not found"
        case .messageTooLarge:
            return "Message is too large"
        case .metadataFailed(let reason):
            return "Metadata error: \(reason)"
        case .notAllowed(let reason):
            return "Operation not allowed: \(reason)"
        case .notImplemented:
            return "This feature is not yet implemented"
        case .persistenceFailed(let reason):
            return "Persistence error: \(reason)"
        case .processingFailed(let reason):
            return "Processing failed: \(reason)"
        case .producerBlockedQuotaExceeded:
            return "Producer is blocked because quota was exceeded"
        case .producerBusy(let reason):
            return "Producer busy: \(reason)"
        case .producerClosed:
            return "Producer is closed"
        case .producerDisposed:
            return "Producer is disposed"
        case .producerFaulted:
            return "Producer is faulted"
        case .producerFenced:
            return "Producer is fenced"
        case .producerQueueFull:
            return "Producer queue is full"
        case .protocolError(let reason):
            return "Protocol error: \(reason)"
        case .readerClosed:
            return "Reader is closed"
        case .readerDisposed:
            return "Reader is disposed"
        case .readerFaulted:
            return "Reader is faulted"
        case .schemaError(let reason):
            return "Schema error: \(reason)"
        case .schemaSerializationFailed(let reason):
            return "Schema serialization failed: \(reason)"
        case .serviceNotReady:
            return "Service is not ready"
        case .subscriptionNotFound(let subscription):
            return "Subscription not found: \(subscription)"
        case .timeout(let operation):
            return "Operation timed out: \(operation)"
        case .topicNotFound(let topic):
            return "Topic not found: \(topic)"
        case .topicTerminated(let reason):
            return "Topic was terminated: \(reason)"
        case .tooManyRequests:
            return "Too many requests"
        case .transactionConflict:
            return "Transaction conflict"
        case .transactionCoordinatorNotFound:
            return "Transaction coordinator not found"
        case .transactionNotFound:
            return "Transaction not found"
        case .transactionOperationFailed(let reason):
            return "Transaction operation failed: \(reason)"
        case .unknownError(let reason):
            return "Unknown error: \(reason)"
        case .unsupportedOperation(let operation):
            return "Unsupported operation: \(operation)"
        case .unsupportedVersion:
            return "Unsupported version"
        }
    }
}