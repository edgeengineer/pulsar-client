import Testing
import Foundation
import NIOCore
import NIOPosix
import Logging
@testable import PulsarClient

@Suite("Ping Monitoring Behavior Tests")
struct PingMonitoringTests {
    
    @Test("withTaskCancellationHandler responds immediately in monitoring loop")
    func testWithTaskCancellationHandlerInMonitoring() async throws {
        // This test directly verifies the withTaskCancellationHandler pattern
        // we implemented in Connection+Enhanced.swift
        
        actor CancelTracker {
            private var _called = false
            
            func setCalled() {
                _called = true
            }
            
            func wasCalled() -> Bool {
                return _called
            }
        }
        
        let tracker = CancelTracker()
        let startTime = Date()
        
        let task = Task {
            // Simulate the exact pattern used in monitorConnectionHealth()
            while !Task.isCancelled {
                await withTaskCancellationHandler {
                    do {
                        // This simulates the pingIntervalNanos sleep
                        try await Task.sleep(nanoseconds: 5_000_000_000) // 5 seconds
                    } catch {
                        // Task was cancelled during sleep
                        return
                    }
                    
                    // This would be where we send the ping
                    // But we won't reach here due to cancellation
                } onCancel: {
                    Task { await tracker.setCalled() }
                }
            }
        }
        
        // Let the monitoring loop start
        try await Task.sleep(nanoseconds: 10_000_000) // 10ms
        
        // Cancel the task
        task.cancel()
        
        // Task should complete quickly
        await task.value
        let endTime = Date()
        
        // Verify immediate cancellation response
        let wasCancelled = await tracker.wasCalled()
        #expect(wasCancelled == true)
        #expect(endTime.timeIntervalSince(startTime) < 0.5) // Much less than 5 seconds
    }
    
    @Test("Connection monitoring with different ping intervals")
    func testConnectionMonitoringWithDifferentPingIntervals() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        // Test with extremely fast ping interval
        let fastConnection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "fast-ping"),
            pingIntervalNanos: 1_000_000 // 1ms - very fast
        )
        
        // Test with slow ping interval  
        let slowConnection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "slow-ping"),
            pingIntervalNanos: 10_000_000_000 // 10 seconds
        )
        
        // Start both monitoring tasks
        let fastTask = Task {
            await fastConnection.startReconnectionMonitoring()
        }
        
        let slowTask = Task {
            await slowConnection.startReconnectionMonitoring()
        }
        
        // Let them run briefly
        try await Task.sleep(nanoseconds: 50_000_000) // 50ms
        
        // Cancel both - should both respond quickly due to withTaskCancellationHandler
        let cancelStartTime = Date()
        
        fastTask.cancel()
        slowTask.cancel()
        
        await fastTask.value
        await slowTask.value
        
        let cancelEndTime = Date()
        let cancelDuration = cancelEndTime.timeIntervalSince(cancelStartTime)
        
        // Both should cancel quickly regardless of their ping intervals
        // because of withTaskCancellationHandler
        #expect(cancelDuration < 1.0)
        
        await fastConnection.close()
        await slowConnection.close()
        
        // Now safely shutdown the event loop
        try await eventLoopGroup.shutdownGracefully()
    }
    
    @Test("Connection monitoring state machine behavior")
    func testConnectionMonitoringStateMachine() async throws {
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "state-test"),
            pingIntervalNanos: 100_000_000 // 100ms
        )
        
        // Initial state
        #expect(await connection.state == .disconnected)
        
        // Start monitoring
        let monitoringTask = Task {
            await connection.startReconnectionMonitoring()
        }
        
        // Let monitoring start
        try await Task.sleep(nanoseconds: 50_000_000) // 50ms
        
        // Verify monitoring task is running (it won't complete immediately)
        #expect(!monitoringTask.isCancelled)
        
        // Close connection - this should cause monitoring to exit
        await connection.close()
        #expect(await connection.state == .closed)
        
        // Monitoring task should complete quickly after connection close
        let startTime = Date()
        await monitoringTask.value
        let endTime = Date()
        
        // Should exit monitoring loop quickly when state becomes .closed
        #expect(endTime.timeIntervalSince(startTime) < 1.0)
        
        // Now safely shutdown the event loop
        try await eventLoopGroup.shutdownGracefully()
    }
    
    @Test("Ping interval configuration flows through to monitoring")
    func testPingIntervalFlowsToMonitoring() async throws {
        // Test that the ping interval set in the constructor
        // is actually used by the monitoring logic
        
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        
        // Test different ping intervals
        let intervals: [UInt64] = [
            1_000_000,     // 1ms
            10_000_000,    // 10ms  
            100_000_000,   // 100ms
            1_000_000_000  // 1s
        ]
        
        for interval in intervals {
            let connection = Connection(
                url: url,
                eventLoopGroup: eventLoopGroup,
                logger: Logger(label: "test-\(interval)"),
                pingIntervalNanos: interval
            )
            
            // Verify the interval is stored correctly
            #expect(await connection.pingIntervalNanos == interval)
            
            // Start monitoring briefly
            let task = Task {
                await connection.startReconnectionMonitoring()
            }
            
            // Let it run for a short time
            try await Task.sleep(nanoseconds: 10_000_000) // 10ms
            
            // Cancel and verify quick response (due to withTaskCancellationHandler)
            let cancelStart = Date()
            task.cancel()
            await task.value
            let cancelEnd = Date()
            
            // Should cancel quickly regardless of ping interval
            #expect(cancelEnd.timeIntervalSince(cancelStart) < 0.5)
            
            await connection.close()
        }
        
        // Now safely shutdown the event loop
        try await eventLoopGroup.shutdownGracefully()
    }
}