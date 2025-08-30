import Foundation
import Logging

/// Protocol for transaction coordinator operations
protocol TransactionCoordinator: Actor {
    /// Begin a new transaction
    func beginTransaction(timeout: TimeInterval) async throws -> TxnID
    
    /// Add a partition to the transaction
    func addPartitionToTransaction(_ txnID: TxnID, partition: String) async throws
    
    /// Commit a transaction
    func commitTransaction(_ txnID: TxnID) async throws
    
    /// Abort a transaction
    func abortTransaction(_ txnID: TxnID) async throws
}

/// Transaction coordinator client implementation
actor TransactionCoordinatorClient: TransactionCoordinator {
    private let logger = Logger(label: "TransactionCoordinator")
    private let connectionPool: ConnectionPool
    private let tcId: UInt64 // Transaction coordinator ID
    private var connection: Connection?
    private let requestIdGenerator = RequestIdGenerator()
    
    init(connectionPool: ConnectionPool, tcId: UInt64) {
        self.connectionPool = connectionPool
        self.tcId = tcId
    }
    
    func beginTransaction(timeout: TimeInterval) async throws -> TxnID {
        let connection = try await getConnection()
        let requestId = await requestIdGenerator.next()
        
        var newTxn = Pulsar_Proto_CommandNewTxn()
        newTxn.requestID = requestId
        newTxn.tcID = tcId
        newTxn.txnTtlSeconds = UInt64(timeout)
        
        var command = Pulsar_Proto_BaseCommand()
        command.type = .newTxn
        command.newTxn = newTxn
        
        logger.debug("Beginning new transaction", metadata: [
            "tcId": "\(tcId)",
            "timeout": "\(timeout)s"
        ])
        
        let response = try await connection.sendCommand(command, expectResponse: true)
        
        guard response.type == .newTxnResponse,
              response.hasNewTxnResponse else {
            throw PulsarClientError.connectionFailed("Invalid response for new transaction")
        }
        
        let txnResponse = response.newTxnResponse
        
        guard !txnResponse.hasError else {
            throw mapServerError(txnResponse.error, txnResponse.message)
        }
        
        let txnID = TxnID(
            mostSigBits: txnResponse.txnidMostBits,
            leastSigBits: txnResponse.txnidLeastBits
        )
        
        logger.debug("Created new transaction", metadata: [
            "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)"
        ])
        
        return txnID
    }
    
    func addPartitionToTransaction(_ txnID: TxnID, partition: String) async throws {
        let connection = try await getConnection()
        let requestId = await requestIdGenerator.next()
        
        var addPartition = Pulsar_Proto_CommandAddPartitionToTxn()
        addPartition.requestID = requestId
        addPartition.txnidMostBits = txnID.mostSigBits
        addPartition.txnidLeastBits = txnID.leastSigBits
        addPartition.partitions = [partition]
        
        var command = Pulsar_Proto_BaseCommand()
        command.type = .addPartitionToTxn
        command.addPartitionToTxn = addPartition
        
        logger.debug("Adding partition to transaction", metadata: [
            "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
            "partition": "\(partition)"
        ])
        
        let response = try await connection.sendCommand(command, expectResponse: true)
        
        guard response.type == .addPartitionToTxnResponse,
              response.hasAddPartitionToTxnResponse else {
            throw PulsarClientError.connectionFailed("Invalid response for add partition to transaction")
        }
        
        let addPartitionResponse = response.addPartitionToTxnResponse
        
        guard !addPartitionResponse.hasError else {
            throw mapServerError(addPartitionResponse.error, addPartitionResponse.message)
        }
    }
    
    func commitTransaction(_ txnID: TxnID) async throws {
        try await endTransaction(txnID, action: .commit)
    }
    
    func abortTransaction(_ txnID: TxnID) async throws {
        try await endTransaction(txnID, action: .abort)
    }
    
    private func endTransaction(_ txnID: TxnID, action: Pulsar_Proto_TxnAction) async throws {
        let connection = try await getConnection()
        let requestId = await requestIdGenerator.next()
        
        var endTxn = Pulsar_Proto_CommandEndTxn()
        endTxn.requestID = requestId
        endTxn.txnidMostBits = txnID.mostSigBits
        endTxn.txnidLeastBits = txnID.leastSigBits
        endTxn.txnAction = action
        
        var command = Pulsar_Proto_BaseCommand()
        command.type = .endTxn
        command.endTxn = endTxn
        
        logger.debug("Ending transaction", metadata: [
            "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
            "action": "\(action)"
        ])
        
        let response = try await connection.sendCommand(command, expectResponse: true)
        
        guard response.type == .endTxnResponse,
              response.hasEndTxnResponse else {
            throw PulsarClientError.connectionFailed("Invalid response for end transaction")
        }
        
        let endTxnResponse = response.endTxnResponse
        
        guard !endTxnResponse.hasError else {
            throw mapServerError(endTxnResponse.error, endTxnResponse.message)
        }
        
        logger.debug("Transaction ended successfully", metadata: [
            "txnID": "\(txnID.mostSigBits):\(txnID.leastSigBits)",
            "action": "\(action)"
        ])
    }
    
    private func getConnection() async throws -> Connection {
        if let connection = connection,
           await connection.isConnected() {
            return connection
        }
        
        // For transaction coordinators, we need to connect to the broker directly
        // In a real implementation, we would use service discovery to find the right broker
        // For now, we'll extract the broker URL from the connection pool's service URL
        guard let firstConnection = await connectionPool.connections.values.first else {
            // Create a connection to the default broker
            let brokerUrl = connectionPool.serviceUrl
            connection = try await connectionPool.getConnection(for: brokerUrl)
            guard let connection = connection else {
                throw PulsarClientError.connectionFailed("Failed to establish connection to broker")
            }
            return connection
        }
        
        // Use existing connection's broker
        return firstConnection
    }
    
    private func mapServerError(_ error: Pulsar_Proto_ServerError, _ message: String) -> PulsarClientError {
        switch error {
        case .transactionCoordinatorNotFound:
            return .transactionCoordinatorNotFound
        case .invalidTxnStatus:
            return .invalidTransactionStatus(message)
        case .transactionConflict:
            return .transactionConflict
        case .transactionNotFound:
            return .transactionNotFound
        default:
            return .connectionFailed(message)
        }
    }
}

/// Request ID generator for transaction coordinator
private actor RequestIdGenerator {
    private var nextId: UInt64 = 0
    
    func next() -> UInt64 {
        let id = nextId
        nextId &+= 1
        return id
    }
}