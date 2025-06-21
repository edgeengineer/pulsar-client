import Testing
@testable import PulsarClient
import Foundation
import NIOCore
import NIOPosix
import Logging

@Suite("Connection Tests")
struct ConnectionTests {
    
    @Test("Parse Pulsar URLs")
    func testPulsarURLParsing() throws {
        // Test non-SSL URL
        let url1 = try PulsarURL(string: "pulsar://localhost:6650")
        #expect(url1.scheme == "pulsar")
        #expect(url1.host == "localhost")
        #expect(url1.port == 6650)
        #expect(url1.isSSL == false)
        
        // Test SSL URL
        let url2 = try PulsarURL(string: "pulsar+ssl://broker.example.com:6651")
        #expect(url2.scheme == "pulsar+ssl")
        #expect(url2.host == "broker.example.com")
        #expect(url2.port == 6651)
        #expect(url2.isSSL == true)
        
        // Test default ports
        let url3 = try PulsarURL(string: "pulsar://localhost")
        #expect(url3.port == 6650)
        
        let url4 = try PulsarURL(string: "pulsar+ssl://localhost")
        #expect(url4.port == 6651)
    }
    
    @Test("Connection state transitions")
    func testConnectionStateTransitions() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            try? eventLoopGroup.syncShutdownGracefully()
        }
        
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test")
        )
        
        // Initial state should be disconnected
        let initialState = await connection.state
        #expect(initialState == .disconnected)
        
        // Track state changes
        let stateTask = Task {
            var states: [ConnectionState] = []
            for await state in await connection.stateChanges {
                states.append(state)
                if state == .closed {
                    break
                }
            }
            return states
        }
        
        // Close immediately (since we can't actually connect in tests)
        await connection.close()
        
        let states = await stateTask.value
        #expect(states.isEmpty) // No state changes if already disconnected
    }
    
    @Test("Connection pool basic operations")
    func testConnectionPool() async throws {
        let pool = ConnectionPool(serviceUrl: "pulsar://localhost:6650")
        defer {
            Task {
                await pool.close()
            }
        }
        
        // Test that we can create a pool (actual connection will fail in tests)
        #expect(pool != nil)
    }
    
    @Test("Channel manager operations")
    func testChannelManager() async throws {
        let manager = ChannelManager()
        
        // Create mock connection
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            try? eventLoopGroup.syncShutdownGracefully()
        }
        
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test")
        )
        
        // Test producer channel
        let producerChannel = ProducerChannel(
            id: 1,
            topic: "test-topic",
            producerName: "test-producer",
            connection: connection
        )
        
        await manager.registerProducer(producerChannel)
        let retrieved = await manager.getProducer(id: 1)
        #expect(retrieved != nil)
        #expect(await retrieved?.topic == "test-topic")
        
        // Test consumer channel
        let consumerChannel = ConsumerChannel(
            id: 2,
            topic: "test-topic",
            subscription: "test-sub",
            consumerName: "test-consumer",
            connection: connection
        )
        
        await manager.registerConsumer(consumerChannel)
        let retrievedConsumer = await manager.getConsumer(id: 2)
        #expect(retrievedConsumer != nil)
        #expect(await retrievedConsumer?.subscription == "test-sub")
        
        // Test removal
        await manager.removeProducer(id: 1)
        let removedProducer = await manager.getProducer(id: 1)
        #expect(removedProducer == nil)
        
        // Test close all
        await manager.closeAll()
        let afterClose = await manager.getConsumer(id: 2)
        #expect(afterClose == nil)
    }
}