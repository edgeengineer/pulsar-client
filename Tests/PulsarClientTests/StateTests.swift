import Testing
import Foundation
@testable import PulsarClient

@Suite("Client State Tests")
struct StateTests {
    
    @Test("ProducerState Functionality")
    func testProducerState() {
        // Test state properties
        #expect(ProducerState.closed.isFinal == true)
        #expect(ProducerState.fenced.isFinal == true)
        #expect(ProducerState.connected.isFinal == false)
        #expect(ProducerState.disconnected.isFinal == false)
        
        // Test active states
        #expect(ProducerState.connected.isActive == true)
        #expect(ProducerState.partiallyConnected.isActive == true)
        #expect(ProducerState.disconnected.isActive == false)
        #expect(ProducerState.fenced.isActive == false)
        
        // Test equality
        #expect(ProducerState.connected == ProducerState.connected)
        #expect(ProducerState.faulted(TestError.test) == ProducerState.faulted(TestError.test))
        #expect(ProducerState.connected != ProducerState.disconnected)
        
        // Test conversion to ClientState
        #expect(ProducerState.connected.toClientState == ClientState.connected)
        #expect(ProducerState.closed.toClientState == ClientState.closed)
        #expect(ProducerState.fenced.toClientState == ClientState.closed)
        #expect(ProducerState.waitingForExclusive.toClientState == ClientState.connecting)
    }
    
    @Test("ConsumerState Functionality")
    func testConsumerState() {
        // Test state properties
        #expect(ConsumerState.closed.isFinal == true)
        #expect(ConsumerState.faulted(TestError.test).isFinal == true)
        #expect(ConsumerState.reachedEndOfTopic.isFinal == true)
        #expect(ConsumerState.unsubscribed.isFinal == true)
        #expect(ConsumerState.active.isFinal == false)
        #expect(ConsumerState.inactive.isFinal == false)
        
        // Test active states
        #expect(ConsumerState.active.isActive == true)
        #expect(ConsumerState.partiallyConnected.isActive == true)
        #expect(ConsumerState.inactive.isActive == false)
        #expect(ConsumerState.disconnected.isActive == false)
        
        // Test equality
        #expect(ConsumerState.active == ConsumerState.active)
        #expect(ConsumerState.faulted(TestError.test) == ConsumerState.faulted(TestError.test))
        #expect(ConsumerState.active != ConsumerState.inactive)
        
        // Test conversion to ClientState
        #expect(ConsumerState.active.toClientState == ClientState.connected)
        #expect(ConsumerState.inactive.toClientState == ClientState.connected)
        #expect(ConsumerState.closed.toClientState == ClientState.closed)
        #expect(ConsumerState.unsubscribed.toClientState == ClientState.closed)
    }
    
    @Test("ReaderState Functionality")
    func testReaderState() {
        // Test state properties
        #expect(ReaderState.closed.isFinal == true)
        #expect(ReaderState.faulted(TestError.test).isFinal == true)
        #expect(ReaderState.reachedEndOfTopic.isFinal == true)
        #expect(ReaderState.connected.isFinal == false)
        #expect(ReaderState.disconnected.isFinal == false)
        
        // Test active states
        #expect(ReaderState.connected.isActive == true)
        #expect(ReaderState.partiallyConnected.isActive == true)
        #expect(ReaderState.disconnected.isActive == false)
        #expect(ReaderState.closed.isActive == false)
        
        // Test equality
        #expect(ReaderState.connected == ReaderState.connected)
        #expect(ReaderState.faulted(TestError.test) == ReaderState.faulted(TestError.test))
        #expect(ReaderState.connected != ReaderState.disconnected)
        
        // Test conversion to ClientState
        #expect(ReaderState.connected.toClientState == ClientState.connected)
        #expect(ReaderState.closed.toClientState == ClientState.closed)
        #expect(ReaderState.reachedEndOfTopic.toClientState == ClientState.closed)
    }
    
    @Test("State Conversions")
    func testStateConversions() {
        // Test round-trip conversions
        let clientStates: [ClientState] = [
            .disconnected,
            .initializing,
            .connecting,
            .connected,
            .reconnecting,
            .closing,
            .closed,
            .faulted(TestError.test)
        ]
        
        for clientState in clientStates {
            // Producer state conversion
            let producerState = ProducerState(from: clientState)
            let backToClient1 = producerState.toClientState
            
            // Consumer state conversion
            let consumerState = ConsumerState(from: clientState)
            let backToClient2 = consumerState.toClientState
            
            // Reader state conversion
            let readerState = ReaderState(from: clientState)
            let backToClient3 = readerState.toClientState
            
            // Note: Not all conversions are reversible due to state mapping
            // But the converted states should be valid
            #expect(backToClient1.isFinal == clientState.isFinal || clientState == .closing)
            #expect(backToClient2.isFinal == clientState.isFinal || clientState == .closing)
            #expect(backToClient3.isFinal == clientState.isFinal || clientState == .closing)
        }
    }
}

// Test error for state testing
private enum TestError: Error {
    case test
}