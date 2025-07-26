import Testing
import Foundation
@testable import PulsarClient

@Suite("Ping Timing Validation Tests")
struct PingTimingValidationTests {
    
    @Test("Ping interval affects configuration correctly")
    func testPingIntervalConfiguration() async {
        let fastBuilder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .withPingInterval(seconds: 0.5) // 500ms
        
        let slowBuilder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .withPingInterval(seconds: 60.0) // 1 minute
            
        // Verify the intervals are set correctly
        #expect(fastBuilder.pingIntervalNanos == 500_000_000) // 500ms in nanoseconds
        #expect(slowBuilder.pingIntervalNanos == 60_000_000_000) // 60s in nanoseconds
        
        // Both should create clients successfully
        let fastClient = fastBuilder.build()
        let slowClient = slowBuilder.build()
        
        #expect(type(of: fastClient) == PulsarClient.self)
        #expect(type(of: slowClient) == PulsarClient.self)

        // dispose unused clients after tests
        await fastClient.dispose()
        await slowClient.dispose()
    }
    
    @Test("Integration test ping interval matches documentation")
    func testIntegrationTestPingInterval() async {
        // Verify that integration tests use the fast ping interval we set
        let testBuilder = PulsarClientBuilder()
            .withServiceUrl("pulsar://localhost:6650")
            .withPingInterval(seconds: 1.0) // Same as IntegrationTestCase
        
        #expect(testBuilder.pingIntervalNanos == 1_000_000_000) // 1 second in nanoseconds
        
        let client = testBuilder.build()
        #expect(type(of: client) == PulsarClient.self)

        // dispose unused client after test
        await client.dispose()
    }
    
    @Test("Production vs test ping intervals")
    func testProductionVsTestPingIntervals() {
        // Production: default 30 seconds
        let productionBuilder = PulsarClientBuilder()
        #expect(productionBuilder.pingIntervalNanos == 30_000_000_000)
        
        // Test: 1 second (30x faster)
        let testBuilder = PulsarClientBuilder()
            .withPingInterval(seconds: 1.0)
        #expect(testBuilder.pingIntervalNanos == 1_000_000_000)
        
        // Verify the ratio is exactly 30:1
        let speedupRatio = Double(productionBuilder.pingIntervalNanos) / Double(testBuilder.pingIntervalNanos)
        #expect(speedupRatio == 30.0)
    }
    
    @Test("Ping interval edge cases")
    func testPingIntervalEdgeCases() {
        let builder = PulsarClientBuilder()
        
        // Very small intervals
        builder.withPingInterval(seconds: 0.001) // 1ms
        #expect(builder.pingIntervalNanos == 1_000_000)
        
        // Fractional seconds
        builder.withPingInterval(seconds: 2.5)
        #expect(builder.pingIntervalNanos == 2_500_000_000)
        
        // Large intervals
        builder.withPingInterval(seconds: 300.0) // 5 minutes
        #expect(builder.pingIntervalNanos == 300_000_000_000)
        
        // Direct nanosecond setting
        builder.withPingInterval(nanos: 750_000_000) // 750ms
        #expect(builder.pingIntervalNanos == 750_000_000)
    }
}