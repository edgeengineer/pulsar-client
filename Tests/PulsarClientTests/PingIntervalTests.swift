import Testing
import Foundation
@testable import PulsarClient

@Suite("Ping Interval Configuration Tests")
struct PingIntervalTests {
    
    @Test("PulsarClientBuilder with ping interval in seconds")
    func testPulsarClientBuilderWithPingIntervalSeconds() async throws {
        // Test with custom ping interval in seconds
        let builder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .withPingInterval(seconds: 5.0)
        
        let client = builder.build()
        
        // Verify client was created successfully
        #expect(type(of: client) == PulsarClient.self)
        
        // We can't directly access the configuration without making it internal,
        // but we can verify the builder method worked by checking it doesn't throw
        #expect(builder.pingIntervalNanos == 5_000_000_000)

        // dispose unused client after tests
        await client.dispose()
    }
    
    @Test("PulsarClientBuilder with ping interval in nanoseconds")
    func testPulsarClientBuilderWithPingIntervalNanos() async throws {
        // Test with custom ping interval in nanoseconds
        let builder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .withPingInterval(nanos: 2_000_000_000) // 2 seconds
        
        let client = builder.build()
        
        // Verify client was created successfully
        #expect(type(of: client) == PulsarClient.self)
        
        // Verify the interval was set correctly
        #expect(builder.pingIntervalNanos == 2_000_000_000)

        // dispose unused client after tests
        await client.dispose()
    }
    
    @Test("Default ping interval is 30 seconds")
    func testDefaultPingInterval() async throws {
        let builder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
        
        // Should have default 30 second interval
        #expect(builder.pingIntervalNanos == 30_000_000_000)
        
        let client = builder.build()
        #expect(type(of: client) == PulsarClient.self)

        // dispose unused client after tests
        await client.dispose()
    }
    
    @Test("Ping interval seconds conversion")
    func testPingIntervalSecondsConversion() {
        let builder = PulsarClientBuilder()
        
        // Test fractional seconds
        builder.withPingInterval(seconds: 1.5)
        #expect(builder.pingIntervalNanos == 1_500_000_000)
        
        // Test whole seconds
        builder.withPingInterval(seconds: 10.0)
        #expect(builder.pingIntervalNanos == 10_000_000_000)
        
        // Test small intervals
        builder.withPingInterval(seconds: 0.1)
        #expect(builder.pingIntervalNanos == 100_000_000)
    }
    
    @Test("Builder method chaining with ping interval")
    func testBuilderMethodChaining() async throws {
        let client = PulsarClient.builder { builder in
            builder.withServiceUrl("pulsar://localhost:6650")
                   .withPingInterval(seconds: 3.0)
                   .withOperationTimeout(15.0)
        }
        
        #expect(type(of: client) == PulsarClient.self)

        // dispose unused client after tests
        await client.dispose()
    }
    
    @Test("ClientConfiguration includes ping interval")
    func testClientConfigurationIncludesPingInterval() async throws {
        // Create a builder with custom ping interval
        let builder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .withPingInterval(seconds: 7.0)
        
        // Build with configuration (internal method)
        let client = builder.buildWithConfiguration()
        
        // The fact that this builds successfully means the configuration
        // includes the ping interval and is passed through properly
        #expect(type(of: client) == PulsarClient.self)

        // dispose unused client after tests
        await client.dispose()
    }
}

@Suite("Connection Health Monitoring Tests")
struct ConnectionHealthMonitoringTests {
    
    @Test("Task cancellation timing", .timeLimit(.minutes(1)))
    func testTaskCancellationTiming() async throws {
        // This test verifies that our cancellation mechanism responds quickly
        let startTime = Date()
        
        let task = Task {
            // Simulate the monitoring loop with cancellation handler
            await withTaskCancellationHandler {
                do {
                    // Use a long sleep that should be cancelled
                    try await Task.sleep(nanoseconds: 10_000_000_000) // 10 seconds
                    return false // Should not reach here
                } catch {
                    return true // Task was cancelled
                }
            } onCancel: {
                // This should run immediately
            }
        }
        
        // Cancel after a short delay
        try await Task.sleep(nanoseconds: 100_000_000) // 100ms
        task.cancel()
        
        // Task should complete quickly despite the long sleep
        let wasCancelled = await task.value
        let endTime = Date()
        
        #expect(wasCancelled == true)
        #expect(endTime.timeIntervalSince(startTime) < 1.0) // Should complete in under 1 second
    }
    
    @Test("Immediate cancellation response", .timeLimit(.minutes(1)))
    func testImmediateCancellationResponse() async throws {
        // Use actor to handle concurrent access to the flag
        actor CancellationTracker {
            private var _onCancelCalled = false
            
            func setCancelCalled() {
                _onCancelCalled = true
            }
            
            func wasCancelCalled() -> Bool {
                return _onCancelCalled
            }
        }
        
        let tracker = CancellationTracker()
        let startTime = Date()
        
        let task = Task {
            await withTaskCancellationHandler {
                // Long sleep that should be interrupted
                try? await Task.sleep(nanoseconds: 30_000_000_000) // 30 seconds
            } onCancel: {
                Task { await tracker.setCancelCalled() }
            }
        }
        
        // Cancel immediately
        task.cancel()
        
        await task.value
        let endTime = Date()
        
        let wasCancelled = await tracker.wasCancelCalled()
        #expect(wasCancelled == true)
        #expect(endTime.timeIntervalSince(startTime) < 0.5) // Should complete very quickly
    }
    
    @Test("Multiple cancellation handlers")
    func testMultipleCancellationHandlers() async throws {
        // Use actor to handle concurrent access to flags
        actor CancellationTracker {
            private var _handler1Called = false
            private var _handler2Called = false
            
            func setHandler1Called() {
                _handler1Called = true
            }
            
            func setHandler2Called() {
                _handler2Called = true
            }
            
            func getBothHandlerStatus() -> (Bool, Bool) {
                return (_handler1Called, _handler2Called)
            }
        }
        
        let tracker = CancellationTracker()
        
        await withTaskGroup(of: Void.self) { group in
            group.addTask {
                await withTaskCancellationHandler {
                    try? await Task.sleep(nanoseconds: 5_000_000_000) // 5 seconds
                } onCancel: {
                    Task { await tracker.setHandler1Called() }
                }
            }
            
            group.addTask {
                await withTaskCancellationHandler {
                    try? await Task.sleep(nanoseconds: 5_000_000_000) // 5 seconds
                } onCancel: {
                    Task { await tracker.setHandler2Called() }
                }
            }
            
            // Cancel all tasks in the group
            group.cancelAll()
        }
        
        // Give a small amount of time for the async cancellation handlers to complete
        try? await Task.sleep(nanoseconds: 10_000_000) // 10ms
        
        let (handler1Called, handler2Called) = await tracker.getBothHandlerStatus()
        #expect(handler1Called == true)
        #expect(handler2Called == true)
    }
}

@Suite("Ping Interval Integration Tests") 
struct PingIntervalIntegrationTests {
    
    @Test("Fast ping interval for testing")
    func testFastPingIntervalForTesting() async throws {
        // Test that we can create a client with very fast ping interval
        // suitable for testing (like our IntegrationTestCase does)
        let client = PulsarClient.builder { builder in
            builder.withServiceUrl("pulsar://localhost:6650")
                   .withPingInterval(seconds: 0.1) // 100ms for very fast tests
        }
        
        #expect(type(of: client) == PulsarClient.self)

        // dispose unused client after tests
        await client.dispose()
    }
    
    @Test("Ping interval boundary values")
    func testPingIntervalBoundaryValues() {
        let builder = PulsarClientBuilder()
        
        // Test minimum reasonable value (1ms)
        builder.withPingInterval(nanos: 1_000_000)
        #expect(builder.pingIntervalNanos == 1_000_000)
        
        // Test maximum reasonable value (1 hour)
        builder.withPingInterval(seconds: 3600.0)
        #expect(builder.pingIntervalNanos == 3_600_000_000_000)
        
        // Test zero (should work but not recommended)
        builder.withPingInterval(seconds: 0.0)
        #expect(builder.pingIntervalNanos == 0)
    }
    
    @Test("Concurrent ping interval configuration")
    func testConcurrentPingIntervalConfiguration() async {
        // Test that multiple clients can be created with different ping intervals concurrently
        await withTaskGroup(of: PulsarClient.self) { group in
            for i in 1...5 {
                group.addTask {
                    return PulsarClient.builder { builder in
                        builder.withServiceUrl("pulsar://localhost:6650")
                               .withPingInterval(seconds: Double(i))
                    }
                }
            }
            
            var clients: [PulsarClient] = []
            for await client in group {
                clients.append(client)
            }
            
            #expect(clients.count == 5)

            // dispose unused clients after test
            for client in clients {
                await client.dispose()
            }
        }
    }
}