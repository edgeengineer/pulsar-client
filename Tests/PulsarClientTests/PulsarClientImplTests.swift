import Testing
@testable import PulsarClient
import Foundation
import Logging

@Suite("PulsarClient Implementation Tests")
struct PulsarClientImplTests {
    
    @Test("Client builder configuration")
    func testClientBuilder() async throws {
        let client = PulsarClient.builder { builder in
            builder
                .serviceUrl("pulsar://broker1:6650")
                .operationTimeout(45.0)
                .ioThreads(4)
                .logger(Logger(label: "test-client"))
        }
        
        #expect(client != nil)
        
        // Test that client is created with proper configuration
        let stats = await client.getStatistics()
        #expect(stats.totalConnections == 0) // No connections yet
        
        await client.dispose()
    }
    
    @Test("Producer configuration builder")
    func testProducerConfigBuilder() {
        let config = ProducerConfigBuilder<String>()
            .producerName("test-producer")
            .initialSequenceId(100)
            .sendTimeout(15.0)
            .batching(enabled: true)
            .compression(.lz4)
        
        #expect(config.producerName == "test-producer")
        #expect(config.initialSequenceId == 100)
        #expect(config.sendTimeout == 15.0)
        #expect(config.batchingEnabled == true)
        #expect(config.compressionType == .lz4)
    }
    
    @Test("Consumer configuration builder")
    func testConsumerConfigBuilder() {
        let config = ConsumerConfigBuilder<String>()
            .subscription("test-subscription")
            .consumerName("test-consumer")
            .subscriptionType(.shared)
            .subscriptionInitialPosition(.earliest)
            .ackTimeout(45.0)
            .receiverQueueSize(2000)
        
        #expect(config.subscription == "test-subscription")
        #expect(config.consumerName == "test-consumer")
        #expect(config.subscriptionType == .shared)
        #expect(config.subscriptionInitialPosition == .earliest)
        #expect(config.ackTimeout == 45.0)
        #expect(config.receiverQueueSize == 2000)
    }
    
    @Test("Reader configuration builder")
    func testReaderConfigBuilder() {
        let config = ReaderConfigBuilder<String>()
            .readerName("test-reader")
            .startMessageId(.earliest)
            .receiverQueueSize(500)
            .readCompacted(true)
        
        #expect(config.readerName == "test-reader")
        #expect(config.startMessageId == .earliest)
        #expect(config.receiverQueueSize == 500)
        #expect(config.readCompacted == true)
    }
    
    @Test("Message creation")
    func testMessageCreation() {
        let messageId = MessageId(ledgerId: 100, entryId: 200, partition: 0, batchIndex: -1)
        let publishTime = Date()
        
        let message = Message(
            id: messageId,
            value: "Hello, Pulsar!",
            key: "test-key",
            properties: ["env": "test", "version": "1.0"],
            eventTime: nil,
            publishTime: publishTime,
            producerName: "test-producer",
            sequenceId: 12345,
            replicatedFrom: nil,
            partitionKey: "test-key",
            schemaVersion: nil,
            topicName: "test-topic"
        )
        
        #expect(message.id == messageId)
        #expect(message.value == "Hello, Pulsar!")
        #expect(message.key == "test-key")
        #expect(message.properties["env"] == "test")
        #expect(message.publishTime == publishTime)
        #expect(message.producerName == "test-producer")
        #expect(message.sequenceId == 12345)
        #expect(message.topicName == "test-topic")
    }
    
    @Test("Client statistics")
    func testClientStatistics() {
        let stats = ClientStatistics(
            activeConnections: 2,
            totalConnections: 5,
            activeProducers: 3,
            activeConsumers: 4,
            memoryUsage: 1024 * 1024
        )
        
        #expect(stats.activeConnections == 2)
        #expect(stats.totalConnections == 5)
        #expect(stats.activeProducers == 3)
        #expect(stats.activeConsumers == 4)
        #expect(stats.memoryUsage == 1024 * 1024)
    }
    
    @Test("Error cases")
    func testErrorCases() async throws {
        let client = PulsarClient.builder { builder in
            builder.serviceUrl("pulsar://nonexistent:6650")
        }
        
        // Test producer creation with invalid topic
        do {
            _ = try await client.newProducer(
                topic: "",
                schema: Schema<String>.string
            ) { _ in }
            Issue.record("Expected error for empty topic")
        } catch {
            // Expected
        }
        
        // Test consumer creation without subscription
        do {
            _ = try await client.newConsumer(
                topic: "test-topic",
                schema: Schema<String>.string
            ) { _ in }
            Issue.record("Expected error for missing subscription")
        } catch PulsarClientError.invalidConfiguration(let message) {
            #expect(message.contains("Subscription name is required"))
        } catch {
            Issue.record("Expected invalidConfiguration error")
        }
        
        await client.dispose()
    }
}