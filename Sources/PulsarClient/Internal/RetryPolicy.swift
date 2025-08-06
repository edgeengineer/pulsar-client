import Foundation
import Logging

/// Configuration for retry behavior
public struct RetryPolicy: Sendable {
  public let maxRetries: Int
  public let baseDelay: TimeInterval
  public let maxDelay: TimeInterval
  public let backoffMultiplier: Double
  public let jitterEnabled: Bool

  public init(
    maxRetries: Int = 10,
    baseDelay: TimeInterval = 1.0,
    maxDelay: TimeInterval = 30.0,
    backoffMultiplier: Double = 2.0,
    jitterEnabled: Bool = true
  ) {
    self.maxRetries = maxRetries
    self.baseDelay = baseDelay
    self.maxDelay = maxDelay
    self.backoffMultiplier = backoffMultiplier
    self.jitterEnabled = jitterEnabled
  }

  /// Calculate delay for the given attempt
  public func delayForAttempt(_ attempt: Int) -> TimeInterval {
    guard attempt > 0 else { return 0 }

    let exponentialDelay = baseDelay * pow(backoffMultiplier, Double(attempt - 1))
    let cappedDelay = min(exponentialDelay, maxDelay)

    if jitterEnabled {
      // Add jitter (±25%)
      let jitterRange = cappedDelay * 0.25
      let jitter = Double.random(in: -jitterRange...jitterRange)
      return max(0.1, cappedDelay + jitter)
    } else {
      return cappedDelay
    }
  }

  /// Check if retry should be attempted for the given attempt number
  public func shouldRetry(attempt: Int) -> Bool {
    return attempt <= maxRetries
  }

  /// Default retry policies for different components
  public static let connection = RetryPolicy(
    maxRetries: 15,
    baseDelay: 1.0,
    maxDelay: 60.0,
    backoffMultiplier: 2.0
  )

  public static let producer = RetryPolicy(
    maxRetries: 10,
    baseDelay: 0.5,
    maxDelay: 30.0,
    backoffMultiplier: 2.0
  )

  public static let consumer = RetryPolicy(
    maxRetries: 10,
    baseDelay: 0.5,
    maxDelay: 30.0,
    backoffMultiplier: 2.0
  )

  public static let operation = RetryPolicy(
    maxRetries: 5,
    baseDelay: 0.1,
    maxDelay: 5.0,
    backoffMultiplier: 1.5
  )
}

/// Context for retry operations
public struct RetryContext: Sendable {
  public let operation: String
  public let componentType: String
  public let retryPolicy: RetryPolicy
  public let exceptionHandler: ExceptionHandler
  public var attemptNumber: Int = 0

  public init(
    operation: String,
    componentType: String,
    retryPolicy: RetryPolicy = .operation,
    exceptionHandler: ExceptionHandler = DefaultExceptionHandler()
  ) {
    self.operation = operation
    self.componentType = componentType
    self.retryPolicy = retryPolicy
    self.exceptionHandler = exceptionHandler
  }
}

/// Retry execution engine
public actor RetryExecutor {
  private let logger: Logger

  public init(logger: Logger = Logger(label: "RetryExecutor")) {
    self.logger = logger
  }

  /// Execute an operation with retry logic
  public func execute<T: Sendable>(
    context: RetryContext,
    operation: @escaping @Sendable () async throws -> T
  ) async throws -> T {
    var mutableContext = context
    var lastError: Error?

    while mutableContext.attemptNumber < mutableContext.retryPolicy.maxRetries {
      mutableContext.attemptNumber += 1

      do {
        let result = try await operation()
        if mutableContext.attemptNumber > 1 {
          logger.info(
            "Operation \(mutableContext.operation) succeeded after \(mutableContext.attemptNumber) attempts"
          )
        }
        return result
      } catch {
        lastError = error

        // Create exception context and handle it
        var exceptionContext = ExceptionContext(
          exception: error,
          operationType: mutableContext.operation,
          componentType: mutableContext.componentType,
          attemptNumber: mutableContext.attemptNumber
        )

        await mutableContext.exceptionHandler.handleException(&exceptionContext)

        guard exceptionContext.isHandled else {
          throw error
        }

        switch exceptionContext.result {
        case .rethrow:
          throw error

        case .fail:
          logger.error("Operation \(mutableContext.operation) failed permanently: \(error)")
          throw error

        case .retry:
          let delay = mutableContext.retryPolicy.delayForAttempt(mutableContext.attemptNumber)
          await delayRetry(
            delay, operation: mutableContext.operation, attempt: mutableContext.attemptNumber)

        case .retryAfter(let customDelay):
          await delayRetry(
            customDelay, operation: mutableContext.operation, attempt: mutableContext.attemptNumber)
        }
      }
    }

    // All retries exhausted
    logger.error(
      "Operation \(mutableContext.operation) failed after \(mutableContext.attemptNumber) attempts")
    throw lastError ?? PulsarClientError.timeout("Maximum retries exceeded")
  }

  private func delayRetry(_ delay: TimeInterval, operation: String, attempt: Int) async {
    logger.debug("Retrying \(operation) in \(String(format: "%.2f", delay))s (attempt \(attempt))")
    try? await Task.sleep(nanoseconds: UInt64(delay * 1_000_000_000))
  }
}

/// Extension for common retry patterns
extension RetryExecutor {

  /// Execute with exponential backoff for connection operations
  public func executeWithConnectionRetry<T: Sendable>(
    operation: String,
    _ block: @escaping @Sendable () async throws -> T
  ) async throws -> T {
    let context = RetryContext(
      operation: operation,
      componentType: "Connection",
      retryPolicy: .connection
    )
    return try await execute(context: context, operation: block)
  }

  /// Execute with retry for producer operations
  public func executeWithProducerRetry<T: Sendable>(
    operation: String,
    _ block: @escaping @Sendable () async throws -> T
  ) async throws -> T {
    let context = RetryContext(
      operation: operation,
      componentType: "Producer",
      retryPolicy: .producer
    )
    return try await execute(context: context, operation: block)
  }

  /// Execute with retry for consumer operations
  public func executeWithConsumerRetry<T: Sendable>(
    operation: String,
    _ block: @escaping @Sendable () async throws -> T
  ) async throws -> T {
    let context = RetryContext(
      operation: operation,
      componentType: "Consumer",
      retryPolicy: .consumer
    )
    return try await execute(context: context, operation: block)
  }
}
