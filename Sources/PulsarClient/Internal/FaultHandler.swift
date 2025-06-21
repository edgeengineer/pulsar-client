import Foundation
import Logging

/// Actions to take when an exception occurs during an operation
public enum FaultAction: Sendable {
    /// Rethrow the original exception
    case rethrow
    /// Retry the operation after a delay
    case retry
    /// Retry with a custom delay
    case retryAfter(TimeInterval)
    /// Fail permanently (don't retry)
    case fail
}

/// Context for exception handling
public struct ExceptionContext {
    let exception: Error
    let operationType: String
    let componentType: String
    let attemptNumber: Int
    var result: FaultAction = .rethrow
    var isHandled: Bool = false
    
    public init(exception: Error, operationType: String, componentType: String, attemptNumber: Int = 1) {
        self.exception = exception
        self.operationType = operationType
        self.componentType = componentType
        self.attemptNumber = attemptNumber
    }
}

/// Protocol for handling exceptions and determining fault actions
public protocol ExceptionHandler: Sendable {
    /// Handle an exception and determine the appropriate action
    func handleException(_ context: inout ExceptionContext) async
}

/// Default exception handler based on Pulsar C# client patterns
public struct DefaultExceptionHandler: ExceptionHandler {
    private let logger: Logger
    
    public init(logger: Logger = Logger(label: "DefaultExceptionHandler")) {
        self.logger = logger
    }
    
    public func handleException(_ context: inout ExceptionContext) async {
        let action = determineFaultAction(for: context.exception, attempt: context.attemptNumber)
        context.result = action
        context.isHandled = true
        
        logger.debug("Exception handled for \(context.componentType).\(context.operationType): \(action)")
    }
    
    private func determineFaultAction(for error: Error, attempt: Int) -> FaultAction {
        // Handle PulsarClientError cases
        if let pulsarError = error as? PulsarClientError {
            return handlePulsarError(pulsarError, attempt: attempt)
        }
        
        // Handle system errors
        if let nsError = error as? NSError {
            return handleSystemError(nsError, attempt: attempt)
        }
        
        // Handle other Swift errors
        return handleGenericError(error, attempt: attempt)
    }
    
    private func handlePulsarError(_ error: PulsarClientError, attempt: Int) -> FaultAction {
        switch error {
        // Transient errors that should be retried
        case .tooManyRequests:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        case .channelNotReady:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 0.5))
        case .serviceNotReady:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 2.0))
        case .connectionFailed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        case .consumerBusy:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 0.5))
        case .producerBusy:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 0.5))
        case .timeout:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        case .lookupFailed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 2.0))
        case .metadataFailed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        case .processingFailed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 0.5))
            
        // Permanent errors that should not be retried
        case .producerFenced:
            return .fail
        case .authenticationFailed:
            return .fail
        case .authorizationFailed:
            return .fail
        case .invalidConfiguration:
            return .fail
        case .invalidServiceUrl:
            return .fail
        case .invalidTopicName:
            return .fail
        case .topicNotFound:
            return .fail
        case .subscriptionNotFound:
            return .fail
        case .messageAlreadyAcknowledged:
            return .fail
        case .notImplemented:
            return .fail
        case .unsupportedOperation:
            return .fail
        case .unsupportedVersion:
            return .fail
        case .incompatibleSchema:
            return .fail
        case .schemaError:
            return .fail
        case .checksumFailed:
            return .fail
        case .encryptionFailed:
            return .fail
        case .decryptionFailed:
            return .fail
            
        // Disposed/closed errors - retry might help if reconnecting
        case .clientDisposed, .clientClosed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 2.0))
        case .consumerDisposed, .consumerClosed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        case .producerDisposed, .producerClosed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        case .readerDisposed, .readerClosed:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
            
        // Faulted errors - might recover with retry
        case .consumerFaulted, .producerFaulted, .readerFaulted:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 2.0))
            
        // Other errors - default to retry with backoff
        default:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        }
    }
    
    private func handleSystemError(_ error: NSError, attempt: Int) -> FaultAction {
        switch error.domain {
        case NSURLErrorDomain:
            return handleURLError(error, attempt: attempt)
        case NSPOSIXErrorDomain:
            return handlePOSIXError(error, attempt: attempt)
        default:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        }
    }
    
    private func handleURLError(_ error: NSError, attempt: Int) -> FaultAction {
        switch error.code {
        // Network errors that might recover
        case NSURLErrorTimedOut,
             NSURLErrorCannotFindHost,
             NSURLErrorCannotConnectToHost,
             NSURLErrorNetworkConnectionLost,
             NSURLErrorNotConnectedToInternet:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 2.0))
            
        // DNS errors that are usually permanent
        case NSURLErrorDNSLookupFailed:
            return attempt < 3 ? .retryAfter(5.0) : .fail
            
        // Other network errors
        default:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        }
    }
    
    private func handlePOSIXError(_ error: NSError, attempt: Int) -> FaultAction {
        switch error.code {
        // Connection errors
        case Int(ECONNREFUSED), Int(ECONNRESET), Int(ECONNABORTED):
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        // Host unreachable
        case Int(EHOSTUNREACH), Int(ENETUNREACH):
            return attempt < 3 ? .retryAfter(5.0) : .fail
        // Other POSIX errors
        default:
            return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
        }
    }
    
    private func handleGenericError(_ error: Error, attempt: Int) -> FaultAction {
        // For cancellation errors, check if it's user-initiated
        if error is CancellationError {
            return .fail // Don't retry cancelled operations
        }
        
        // Default: retry with backoff
        return .retryAfter(calculateBackoffDelay(attempt: attempt, baseDelay: 1.0))
    }
    
    /// Calculate exponential backoff delay with jitter
    private func calculateBackoffDelay(attempt: Int, baseDelay: TimeInterval, maxDelay: TimeInterval = 30.0) -> TimeInterval {
        let exponentialDelay = baseDelay * pow(2.0, Double(attempt - 1))
        let cappedDelay = min(exponentialDelay, maxDelay)
        
        // Add jitter (±25%)
        let jitterRange = cappedDelay * 0.25
        let jitter = Double.random(in: -jitterRange...jitterRange)
        
        return max(0.1, cappedDelay + jitter) // Minimum 100ms delay
    }
}