import Testing
import Foundation
import NIOCore
import NIOPosix
import Logging
@testable import PulsarClient

@Suite("Connection Enhanced Tests")
struct ConnectionEnhancedTests {
    
    @Test("Connection accepts ping interval configuration")
    func testConnectionPingIntervalConfiguration() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        // Test with custom ping interval
        let fastConnection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 1_000_000_000 // 1 second
        )
        
        let slowConnection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 30_000_000_000 // 30 seconds
        )
        
        // Verify connections have different ping intervals
        #expect(await fastConnection.pingIntervalNanos == 1_000_000_000)
        #expect(await slowConnection.pingIntervalNanos == 30_000_000_000)
    }
    
    @Test("Connection monitoring task cancellation", .timeLimit(.minutes(1)))
    func testConnectionMonitoringCancellation() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        // Create connection with very short ping interval for testing
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 100_000_000 // 100ms for fast testing
        )
        
        // Test that startReconnectionMonitoring can be cancelled quickly
        let startTime = Date()
        
        let monitoringTask = Task {
            await connection.startReconnectionMonitoring()
        }
        
        // Let it run for a short time
        try await Task.sleep(nanoseconds: 200_000_000) // 200ms
        
        // Cancel the monitoring
        monitoringTask.cancel()
        
        // Wait for cancellation to complete
        _ = await monitoringTask.result
        
        let endTime = Date()
        let duration = endTime.timeIntervalSince(startTime)
        
        // Should complete quickly after cancellation (well under 1 second)
        #expect(duration < 1.0)
    }
    
    @Test("Connection state transitions during monitoring")
    func testConnectionStateTransitions() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 50_000_000 // 50ms for fast testing
        )
        
        // Initial state should be disconnected
        let initialState = await connection.state
        #expect(initialState == .disconnected)
        
        // Close the connection and verify state change
        await connection.close()
        let closedState = await connection.state
        #expect(closedState == .closed)
    }
    
    @Test("Multiple connections with different ping intervals")
    func testMultipleConnectionsWithDifferentPingIntervals() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 2)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        // Create connections with different ping intervals
        let connections = await withTaskGroup(of: Connection.self) { group in
            var results: [Connection] = []
            
            for i in 1...3 {
                group.addTask {
                    return Connection(
                        url: url,
                        eventLoopGroup: eventLoopGroup,
                        logger: Logger(label: "test-\(i)"),
                        pingIntervalNanos: UInt64(i * 1_000_000_000) // 1s, 2s, 3s
                    )
                }
            }
            
            for await connection in group {
                results.append(connection)
            }
            
            return results
        }
        
        #expect(connections.count == 3)
        
        // Verify each connection has its own ping interval
        let intervals = await withTaskGroup(of: UInt64.self) { group in
            var results: [UInt64] = []
            
            for connection in connections {
                group.addTask {
                    return await connection.pingIntervalNanos
                }
            }
            
            for await interval in group {
                results.append(interval)
            }
            
            return results.sorted()
        }
        
        #expect(intervals == [1_000_000_000, 2_000_000_000, 3_000_000_000])
        
        // Clean up
        await withTaskGroup(of: Void.self) { group in
            for connection in connections {
                group.addTask {
                    await connection.close()
                }
            }
        }
    }
}

@Suite("Connection Health Monitoring Internal Tests")
struct ConnectionHealthMonitoringInternalTests {
    
    @Test("Connection monitoring responds to state changes")
    func testConnectionMonitoringStateChanges() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 100_000_000 // 100ms
        )
        
        // Test that monitoring task exits when connection is closed
        let monitoringTask = Task {
            await connection.startReconnectionMonitoring()
        }
        
        // Let monitoring start
        try await Task.sleep(nanoseconds: 50_000_000) // 50ms
        
        // Close connection (should cause monitoring to exit)
        await connection.close()
        
        // Monitoring task should complete quickly after close
        let startTime = Date()
        _ = await monitoringTask.result
        let endTime = Date()
        
        #expect(endTime.timeIntervalSince(startTime) < 0.5)
    }
    
    @Test("Fast ping interval for testing scenarios", .timeLimit(.minutes(1)))
    func testFastPingIntervalForTesting() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        // Test with very fast ping interval (like our integration tests)
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 10_000_000 // 10ms for very fast testing
        )
        
        let pingInterval = await connection.pingIntervalNanos
        #expect(pingInterval == 10_000_000)
        
        // Start monitoring and let it run briefly
        let monitoringTask = Task {
            await connection.startReconnectionMonitoring()
        }
        
        // With 10ms intervals, several monitoring cycles should complete quickly
        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        
        // Cancel and verify quick response
        let cancelStart = Date()
        monitoringTask.cancel()
        _ = await monitoringTask.result
        let cancelEnd = Date()
        
        // Should cancel very quickly
        #expect(cancelEnd.timeIntervalSince(cancelStart) < 0.1)
    }
    
    @Test("Connection ping interval bounds checking")
    func testConnectionPingIntervalBounds() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer { 
            Task.detached { 
                try? await eventLoopGroup.shutdownGracefully() 
            }
        }
        
        // Test minimum reasonable ping interval (1ms)
        let minConnection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 1_000_000 // 1ms
        )
        
        // Test maximum reasonable ping interval (1 hour)
        let maxConnection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test"),
            pingIntervalNanos: 3_600_000_000_000 // 1 hour
        )
        
        #expect(await minConnection.pingIntervalNanos == 1_000_000)
        #expect(await maxConnection.pingIntervalNanos == 3_600_000_000_000)
        
        // Both should be created successfully
        let minState = await minConnection.state
        let maxState = await maxConnection.state
        
        #expect(minState == .disconnected) // Initial state
        #expect(maxState == .disconnected) // Initial state
        
        // Clean up
        await minConnection.close()
        await maxConnection.close()
    }
}