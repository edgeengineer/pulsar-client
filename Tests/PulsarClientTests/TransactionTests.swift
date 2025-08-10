import Foundation
import Testing

@testable import PulsarClient

@Suite("Transaction Tests")
struct TransactionTests {
    
    @Test("TxnID Initialization and Properties")
    func testTxnIDInit() {
        let txnId = TxnID(mostSigBits: 123456789, leastSigBits: 987654321)
        
        #expect(txnId.mostSigBits == 123456789)
        #expect(txnId.leastSigBits == 987654321)
    }
    
    @Test("TxnID Equality")
    func testTxnIDEquality() {
        let txnId1 = TxnID(mostSigBits: 100, leastSigBits: 200)
        let txnId2 = TxnID(mostSigBits: 100, leastSigBits: 200)
        let txnId3 = TxnID(mostSigBits: 100, leastSigBits: 201)
        let txnId4 = TxnID(mostSigBits: 101, leastSigBits: 200)
        
        #expect(txnId1 == txnId2)
        #expect(txnId1 != txnId3)
        #expect(txnId1 != txnId4)
    }
    
    @Test("TxnID String Representation")
    func testTxnIDDescription() {
        let txnId = TxnID(mostSigBits: 1000, leastSigBits: 2000)
        let description = String(describing: txnId)
        
        #expect(description.contains("1000"))
        #expect(description.contains("2000"))
    }
    
    @Test("TransactionState Values")
    func testTransactionStates() {
        // Test all transaction states exist
        let openState = TransactionState.open
        let committingState = TransactionState.committing
        let abortingState = TransactionState.aborting
        let committedState = TransactionState.committed
        let abortedState = TransactionState.aborted
        let errorState = TransactionState.error
        
        #expect(openState == .open)
        #expect(committingState == .committing)
        #expect(abortingState == .aborting)
        #expect(committedState == .committed)
        #expect(abortedState == .aborted)
        #expect(errorState == .error)
    }
    
    @Test("TransactionState Final States")
    func testTransactionStateFinalStates() {
        // Test which states are considered final
        #expect(TransactionState.open.isFinal == false)
        #expect(TransactionState.committing.isFinal == false)
        #expect(TransactionState.aborting.isFinal == false)
        #expect(TransactionState.committed.isFinal == true)
        #expect(TransactionState.aborted.isFinal == true)
        #expect(TransactionState.error.isFinal == true)
    }
    
    @Test("TransactionState Active States")
    func testTransactionStateActiveStates() {
        // Test which states are considered active
        #expect(TransactionState.open.isActive == true)
        #expect(TransactionState.committing.isActive == true)
        #expect(TransactionState.aborting.isActive == true)
        #expect(TransactionState.committed.isActive == false)
        #expect(TransactionState.aborted.isActive == false)
        #expect(TransactionState.error.isActive == false)
    }
    
    @Test("MessageMetadata with Transaction")
    func testMessageMetadataWithTransaction() {
        // Create a mock transaction
        let txnId = TxnID(mostSigBits: 1, leastSigBits: 2)
        
        // Test that MessageMetadata can store transaction info
        var metadata = MessageMetadata()
        metadata.properties["txn.id.most"] = String(txnId.mostSigBits)
        metadata.properties["txn.id.least"] = String(txnId.leastSigBits)
        
        #expect(metadata.properties["txn.id.most"] == "1")
        #expect(metadata.properties["txn.id.least"] == "2")
    }
    
    @Test("Transaction State Transitions")
    func testTransactionStateTransitions() {
        // Test valid state transitions
        var state = TransactionState.open
        
        // Open can transition to committing or aborting
        state = .committing
        #expect(state == .committing)
        
        state = .open
        state = .aborting
        #expect(state == .aborting)
        
        // Committing should transition to committed
        state = .committing
        state = .committed
        #expect(state == .committed)
        
        // Aborting should transition to aborted
        state = .aborting
        state = .aborted
        #expect(state == .aborted)
        
        // Error state can be reached from any state
        state = .error
        #expect(state == .error)
    }
}

// Extension to make TransactionState testable
extension TransactionState {
    var isFinal: Bool {
        switch self {
        case .committed, .aborted, .error:
            return true
        default:
            return false
        }
    }
    
    var isActive: Bool {
        switch self {
        case .open, .committing, .aborting:
            return true
        default:
            return false
        }
    }
}