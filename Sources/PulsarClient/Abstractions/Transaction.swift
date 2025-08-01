import Foundation

/// Represents a unique transaction identifier
public struct TxnID: Sendable, Hashable {
    /// The most significant bits of the transaction ID
    public let mostSigBits: UInt64
    
    /// The least significant bits of the transaction ID
    public let leastSigBits: UInt64
    
    public init(mostSigBits: UInt64, leastSigBits: UInt64) {
        self.mostSigBits = mostSigBits
        self.leastSigBits = leastSigBits
    }
}

/// Represents the state of a transaction
public enum TransactionState: Sendable {
    /// Transaction is open and can accept operations
    case open
    
    /// Transaction is in the process of committing
    case committing
    
    /// Transaction is in the process of aborting
    case aborting
    
    /// Transaction has been successfully committed
    case committed
    
    /// Transaction has been aborted
    case aborted
    
    /// Transaction encountered an error
    case error
    
    /// Transaction has timed out
    case timeout
}

/// Protocol defining a Pulsar transaction
public protocol Transaction: Sendable {
    /// The unique identifier for this transaction
    var txnID: TxnID { get }
    
    /// The current state of the transaction
    var state: TransactionState { get async }
    
    /// Commits the transaction
    /// - Throws: `PulsarClientError` if the commit fails
    func commit() async throws
    
    /// Aborts the transaction
    /// - Throws: `PulsarClientError` if the abort fails
    func abort() async throws
}

/// Protocol for building transactions
public protocol TransactionBuilder: Sendable {
    /// Sets the transaction timeout
    /// - Parameter timeout: The timeout duration
    /// - Returns: The builder instance for chaining
    func withTransactionTimeout(_ timeout: TimeInterval) -> TransactionBuilder
    
    /// Builds the transaction
    /// - Returns: A new transaction instance
    /// - Throws: `PulsarClientError` if transaction creation fails
    func build() async throws -> Transaction
}