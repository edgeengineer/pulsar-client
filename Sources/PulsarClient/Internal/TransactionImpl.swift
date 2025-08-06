import Foundation
import Logging

/// Actor-based implementation of a Pulsar transaction
actor TransactionImpl: Transaction {
    private let logger = Logger(label: "TransactionImpl")
    
    public let txnID: TxnID
    private let coordinator: TransactionCoordinator
    private let timeout: TimeInterval
    private var _state: TransactionState = .open
    private let createdAt = Date()
    
    init(txnID: TxnID, coordinator: TransactionCoordinator, timeout: TimeInterval) {
        self.txnID = txnID
        self.coordinator = coordinator
        self.timeout = timeout
        
        // Start timeout timer
        Task {
            try? await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
            await checkTimeout()
        }
    }
    
    public var state: TransactionState {
        return _state
    }
    
    public func commit() async throws {
        guard _state == .open else {
            throw PulsarClientError.invalidTransactionStatus(
                "Cannot commit transaction in state: \(_state)"
            )
        }
        
        _state = .committing
        logger.info("Committing transaction", metadata: [
            "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)"
        ])
        
        do {
            try await coordinator.commitTransaction(txnID)
            _state = .committed
            logger.info("Transaction committed successfully", metadata: [
                "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)"
            ])
        } catch {
            _state = .error
            logger.error("Failed to commit transaction", metadata: [
                "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
                "error": "\(error)"
            ])
            throw error
        }
    }
    
    public func abort() async throws {
        guard _state == .open || _state == .timeout else {
            throw PulsarClientError.invalidTransactionStatus(
                "Cannot abort transaction in state: \(_state)"
            )
        }
        
        _state = .aborting
        logger.info("Aborting transaction", metadata: [
            "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)"
        ])
        
        do {
            try await coordinator.abortTransaction(txnID)
            _state = .aborted
            logger.info("Transaction aborted successfully", metadata: [
                "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)"
            ])
        } catch {
            _state = .error
            logger.error("Failed to abort transaction", metadata: [
                "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
                "error": "\(error)"
            ])
            throw error
        }
    }
    
    private func checkTimeout() async {
        let elapsed = Date().timeIntervalSince(createdAt)
        if elapsed >= timeout && _state == .open {
            _state = .timeout
            logger.warning("Transaction timed out", metadata: [
                "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
                "timeout": "\(timeout)s"
            ])
            
            // Attempt to abort the transaction
            do {
                try await abort()
            } catch {
                logger.error("Failed to abort timed out transaction", metadata: [
                    "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
                    "error": "\(error)"
                ])
            }
        }
    }
}

/// Transaction builder implementation
public actor TransactionBuilderImpl: TransactionBuilder {
    private let coordinator: TransactionCoordinator
    private var timeout: TimeInterval = 60.0 // Default 60 seconds
    
    init(coordinator: TransactionCoordinator) {
        self.coordinator = coordinator
    }
    
    public nonisolated func withTransactionTimeout(_ timeout: TimeInterval) -> TransactionBuilder {
        Task {
            await self.setTimeout(timeout)
        }
        return self
    }
    
    private func setTimeout(_ timeout: TimeInterval) {
        self.timeout = timeout
    }
    
    public func build() async throws -> Transaction {
        let txnID = try await coordinator.beginTransaction(timeout: timeout)
        return TransactionImpl(txnID: txnID, coordinator: coordinator, timeout: timeout)
    }
}