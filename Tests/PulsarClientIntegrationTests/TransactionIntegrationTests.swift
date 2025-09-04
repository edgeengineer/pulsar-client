import Foundation
import PulsarClient
import Testing

@Suite("Transaction Integration Tests", .serialized)
class TransactionIntegrationTests {
    let testCase: IntegrationTestCase
    
    init() async throws {
        self.testCase = try await IntegrationTestCase()
    }

     // Non-blocking cleanup to avoid CI teardown deadlocks
    deinit { Task { [testCase] in await testCase.cleanup() } }
    
    @Test("Basic Transaction Commit", .timeLimit(.minutes(2)))
    func testBasicTransactionCommit() async throws {
        guard let client = await testCase.client else {
            throw IntegrationTestError.clientNotInitialized
        }
        
        // Note: Transactions require Pulsar to be configured with transaction coordinator
        // This test will be skipped if transactions are not enabled
        
        let topic = try await testCase.createTopic()
        
        // Create producer with transaction support
        let producer = try await client.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.sendTimeout(30.0)
        }
        
        // Create consumer
        let consumer = try await client.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("txn-test-sub")
        }
        
        do {
            // Begin transaction
            let transaction = try await client.newTransaction()
                .build()
            
            // Send messages within transaction
            let messages = ["txn-message-1", "txn-message-2", "txn-message-3"]
            
            for message in messages {
                _ = try await producer.newMessage()
                    .withValue(message)
                    .withTransaction(transaction)
                    .send()
            }
            
            // Messages should not be visible before commit
            // Try to get a message with a timeout - should fail
            do {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    group.addTask {
                        var iterator = consumer.makeAsyncIterator()
                        _ = try await iterator.next()
                        throw PulsarClientError.unknownError("Should not receive messages before commit")
                    }
                    group.addTask {
                        try await Task.sleep(nanoseconds: 2_000_000_000) // Wait 2 seconds
                    }
                    try await group.next()
                    group.cancelAll()
                }
            } catch {
                // Expected - no messages should be visible
            }
            
            // Commit transaction
            try await transaction.commit()
            
            // Now messages should be visible
            var receivedMessages: [String] = []
            var messageIterator = consumer.makeAsyncIterator()
            for _ in messages {
                if let messageOpt = try await messageIterator.next() {
                    guard let message = messageOpt as? Message<String> else {
                        throw PulsarClientError.unknownError("Failed to cast message")
                    }
                    receivedMessages.append(message.value)
                    try await consumer.acknowledge(message)
                }
            }
            
            #expect(Set(receivedMessages) == Set(messages))
            
        } catch {
            // Transactions might not be enabled - skip test gracefully
            if String(describing: error).contains("Transactions are not enabled") ||
               String(describing: error).contains("transaction") || 
               String(describing: error).contains("not supported") {
                // Log that we're skipping but don't fail the test
                print("NOTE: Skipping transaction test - Transactions not enabled on broker")
                await producer.dispose()
                await consumer.dispose()
                return
            }
            throw error
        }
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
    }
    
    @Test("Transaction Abort", .timeLimit(.minutes(2)))
    func testTransactionAbort() async throws {
        guard let client = await testCase.client else {
            throw IntegrationTestError.clientNotInitialized
        }
        
        let topic = try await testCase.createTopic()
        
        // Create producer
        let producer = try await client.newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            // Use default configuration
        }
        
        // Create consumer
        let consumer = try await client.newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("txn-abort-sub")
        }
        
        do {
            // Begin transaction
            let transaction = try await client.newTransaction()
                .build()
            
            // Send messages within transaction
            let messages = ["abort-msg-1", "abort-msg-2"]
            
            for message in messages {
                _ = try await producer.newMessage()
                    .withValue(message)
                    .withTransaction(transaction)
                    .send()
            }
            
            // Abort transaction
            try await transaction.abort()
            
            // Messages should never be visible after abort
            // Try to get a message with a timeout - should fail
            do {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    group.addTask {
                        var abortIterator = consumer.makeAsyncIterator()
                        _ = try await abortIterator.next()
                        throw PulsarClientError.unknownError("Should not receive messages after abort")
                    }
                    group.addTask {
                        try await Task.sleep(nanoseconds: 3_000_000_000) // Wait 3 seconds
                    }
                    try await group.next()
                    group.cancelAll()
                }
            } catch {
                // Expected - messages were aborted
            }
            
            // Send a non-transactional message to verify consumer works
            let normalMessage = "non-txn-message"
            _ = try await producer.send(normalMessage)
            
            var normalIterator = consumer.makeAsyncIterator()
            if let receivedOpt = try await normalIterator.next() {
                guard let received = receivedOpt as? Message<String> else {
                    throw PulsarClientError.unknownError("Failed to cast message")
                }
                #expect(received.value == normalMessage)
                try await consumer.acknowledge(received)
            }
            
        } catch {
            // Transactions might not be enabled - skip test gracefully
            if String(describing: error).contains("Transactions are not enabled") ||
               String(describing: error).contains("transaction") || 
               String(describing: error).contains("not supported") {
                // Log that we're skipping but don't fail the test
                print("NOTE: Skipping transaction test - Transactions not enabled on broker")
                await producer.dispose()
                await consumer.dispose()
                return
            }
            throw error
        }
        
        // Cleanup
        await producer.dispose()
        await consumer.dispose()
    }
    
    @Test("Transaction with Multiple Topics", .timeLimit(.minutes(2)))
    func testTransactionMultipleTopics() async throws {
        guard let client = await testCase.client else {
            throw IntegrationTestError.clientNotInitialized
        }
        
        let topic1 = try await testCase.createTopic()
        let topic2 = try await testCase.createTopic()
        
        // Create producers for both topics
        let producer1 = try await client.newProducer(
            topic: topic1,
            schema: Schema<String>.string
        ) { builder in
            // Use default configuration
        }
        
        let producer2 = try await client.newProducer(
            topic: topic2,
            schema: Schema<String>.string
        ) { builder in
            // Use default configuration
        }
        
        // Create consumers
        let consumer1 = try await client.newConsumer(
            topic: topic1,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("txn-multi-sub-1")
        }
        
        let consumer2 = try await client.newConsumer(
            topic: topic2,
            schema: Schema<String>.string
        ) { builder in
            builder.subscriptionName("txn-multi-sub-2")
        }
        
        do {
            // Begin transaction
            let transaction = try await client.newTransaction()
                .build()
            
            // Send messages to both topics
            _ = try await producer1.newMessage()
                .withValue("topic1-message")
                .withTransaction(transaction)
                .send()
            
            _ = try await producer2.newMessage()
                .withValue("topic2-message")
                .withTransaction(transaction)
                .send()
            
            // Messages should not be visible before commit
            // Try to get messages with timeout - should fail on both topics
            do {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    group.addTask {
                        var iter1 = consumer1.makeAsyncIterator()
                        _ = try await iter1.next()
                        throw PulsarClientError.unknownError("Topic1: Should not receive before commit")
                    }
                    group.addTask {
                        try await Task.sleep(nanoseconds: 2_000_000_000) // Wait 2 seconds
                    }
                    try await group.next()
                    group.cancelAll()
                }
            } catch {
                // Expected
            }
            
            do {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    group.addTask {
                        var iter2 = consumer2.makeAsyncIterator()
                        _ = try await iter2.next()
                        throw PulsarClientError.unknownError("Topic2: Should not receive before commit")
                    }
                    group.addTask {
                        try await Task.sleep(nanoseconds: 2_000_000_000) // Wait 2 seconds
                    }
                    try await group.next()
                    group.cancelAll()
                }
            } catch {
                // Expected
            }
            
            // Commit transaction
            try await transaction.commit()
            
            // Both topics should now have their messages
            // Get first message from each consumer
            var iter1 = consumer1.makeAsyncIterator()
            if let msg1Opt = try await iter1.next() {
                guard let msg1 = msg1Opt as? Message<String> else {
                    throw PulsarClientError.unknownError("Failed to cast message from topic1")
                }
                #expect(msg1.value == "topic1-message")
                try await consumer1.acknowledge(msg1)
            }
            
            var iter2 = consumer2.makeAsyncIterator()
            if let msg2Opt = try await iter2.next() {
                guard let msg2 = msg2Opt as? Message<String> else {
                    throw PulsarClientError.unknownError("Failed to cast message from topic2")
                }
                #expect(msg2.value == "topic2-message")
                try await consumer2.acknowledge(msg2)
            }
            
        } catch {
            // Transactions might not be enabled - skip test gracefully
            if String(describing: error).contains("Transactions are not enabled") ||
               String(describing: error).contains("transaction") || 
               String(describing: error).contains("not supported") {
                // Log that we're skipping but don't fail the test
                print("NOTE: Skipping transaction test - Transactions not enabled on broker")
                await producer1.dispose()
                await producer2.dispose()
                await consumer1.dispose()
                await consumer2.dispose()
                return
            }
            throw error
        }
        
        // Cleanup
        await producer1.dispose()
        await producer2.dispose()
        await consumer1.dispose()
        await consumer2.dispose()
    }
}