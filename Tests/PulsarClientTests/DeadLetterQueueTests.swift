import Foundation
import Testing

@testable import PulsarClient

@Suite("Dead Letter Queue Tests")
struct DeadLetterQueueTests {
    
    @Test("DeadLetterPolicy Initialization")
    func testDeadLetterPolicyInit() {
        let policy = DeadLetterPolicy(
            maxRedeliverCount: 3,
            deadLetterTopic: "persistent://public/default/dlq-topic",
            retryLetterTopic: "persistent://public/default/retry-topic",
            initialSubscriptionName: "test-subscription"
        )
        
        #expect(policy.maxRedeliverCount == 3)
        #expect(policy.deadLetterTopic == "persistent://public/default/dlq-topic")
        #expect(policy.retryLetterTopic == "persistent://public/default/retry-topic")
        #expect(policy.initialSubscriptionName == "test-subscription")
    }
    
    @Test("DeadLetterPolicy Default Values")
    func testDeadLetterPolicyDefaults() {
        let policy = DeadLetterPolicy(maxRedeliverCount: 5)
        
        #expect(policy.maxRedeliverCount == 5)
        #expect(policy.deadLetterTopic == nil)
        #expect(policy.retryLetterTopic == nil)
        #expect(policy.initialSubscriptionName == nil)
    }
    
    @Test("DeadLetterPolicyBuilder Basic Configuration")
    func testDeadLetterPolicyBuilder() async {
        let builder = DeadLetterPolicyBuilder()
        let policy = await builder
            .maxRedeliverCount(10)
            .deadLetterTopic("dlq-topic")
            .retryLetterTopic("retry-topic")
            .initialSubscriptionName("sub-name")
            .build()
        
        #expect(policy.maxRedeliverCount == 10)
        #expect(policy.deadLetterTopic == "dlq-topic")
        #expect(policy.retryLetterTopic == "retry-topic")
        #expect(policy.initialSubscriptionName == "sub-name")
    }
    
    @Test("DeadLetterPolicyBuilder Defaults")
    func testDeadLetterPolicyBuilderDefaults() async {
        let builder = DeadLetterPolicyBuilder()
        let policy = await builder.build()
        
        #expect(policy.maxRedeliverCount == 3) // Default is 3 in builder
        #expect(policy.deadLetterTopic == nil)
        #expect(policy.retryLetterTopic == nil)
        #expect(policy.initialSubscriptionName == nil)
    }
}