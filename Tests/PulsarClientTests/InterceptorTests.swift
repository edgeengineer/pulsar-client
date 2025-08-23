import Foundation
import Testing

@testable import PulsarClient

@Suite("Interceptor Tests")
struct InterceptorTests {
    
    // Mock Producer Interceptor for testing
    actor MockProducerInterceptor<T: Sendable>: ProducerInterceptor {
        typealias MessageType = T
        
        var beforeSendCallCount = 0
        var onSendAckCallCount = 0
        var lastError: Error?
        var lastMessageId: MessageId?
        
        func beforeSend(producer: any ProducerProtocol<T>, message: Message<T>) async throws -> Message<T> {
            beforeSendCallCount += 1
            
            // Create new metadata with modified properties
            var modifiedMetadata = message.metadata
            modifiedMetadata.properties["intercepted"] = "true"
            modifiedMetadata.properties["interceptor"] = "MockProducerInterceptor"
            
            // Create new message with modified metadata
            let modifiedMessage = Message(
                id: message.id,
                value: message.value,
                metadata: modifiedMetadata,
                publishTime: message.publishTime,
                producerName: message.producerName,
                replicatedFrom: message.replicatedFrom,
                topicName: message.topicName,
                redeliveryCount: message.redeliveryCount,
                data: message.data
            )
            
            return modifiedMessage
        }
        
        func onSendAcknowledgement(
            producer: any ProducerProtocol<T>,
            message: Message<T>,
            messageId: MessageId?,
            error: Error?
        ) async {
            onSendAckCallCount += 1
            lastMessageId = messageId
            lastError = error
        }
    }
    
    // Mock Consumer Interceptor for testing
    actor MockConsumerInterceptor<T: Sendable>: ConsumerInterceptor {
        typealias MessageType = T
        
        var beforeConsumeCallCount = 0
        var onAckCallCount = 0
        var onNackCallCount = 0
        var lastAckedMessageIds: [MessageId] = []
        var lastNackedMessageIds: [MessageId] = []
        
        func beforeConsume(consumer: any ConsumerProtocol<T>, message: Message<T>) async -> Message<T> {
            beforeConsumeCallCount += 1
            
            // Create new metadata with modified properties
            var modifiedMetadata = message.metadata
            modifiedMetadata.properties["consumed"] = "true"
            modifiedMetadata.properties["interceptor"] = "MockConsumerInterceptor"
            
            // Create new message with modified metadata
            let modifiedMessage = Message(
                id: message.id,
                value: message.value,
                metadata: modifiedMetadata,
                publishTime: message.publishTime,
                producerName: message.producerName,
                replicatedFrom: message.replicatedFrom,
                topicName: message.topicName,
                redeliveryCount: message.redeliveryCount,
                data: message.data
            )
            
            return modifiedMessage
        }
        
        func onAcknowledge(
            consumer: any ConsumerProtocol<T>,
            messageIds: [MessageId]
        ) async {
            onAckCallCount += 1
            lastAckedMessageIds = messageIds
        }
        
        func onNegativeAcksSend(
            consumer: any ConsumerProtocol<T>,
            messageIds: [MessageId]
        ) async {
            onNackCallCount += 1
            lastNackedMessageIds = messageIds
        }
    }
    
    @Test("Producer Interceptor Message Modification")
    func testProducerInterceptorModification() async throws {
        let interceptor = MockProducerInterceptor<String>()
        
        // Create a test message
        let originalMessage = Message<String>(
            id: MessageId(ledgerId: 1, entryId: 1),
            value: "test",
            metadata: MessageMetadata(),
            publishTime: Date(),
            producerName: "test-producer",
            replicatedFrom: nil,
            topicName: "test-topic",
            redeliveryCount: 0,
            data: nil
        )
        
        // Create a mock producer (simplified for testing)
        // In real tests, this would be a proper mock
        let modifiedMessage = try await interceptor.beforeSend(producer: MockProducer<String>(), message: originalMessage)
        
        #expect(await interceptor.beforeSendCallCount == 1)
        #expect(modifiedMessage.metadata.properties["intercepted"] == "true")
        #expect(modifiedMessage.metadata.properties["interceptor"] == "MockProducerInterceptor")
    }
    
    @Test("Consumer Interceptor Message Modification")
    func testConsumerInterceptorModification() async {
        let interceptor = MockConsumerInterceptor<String>()
        
        // Create a test message
        let originalMessage = Message<String>(
            id: MessageId(ledgerId: 1, entryId: 1),
            value: "test",
            metadata: MessageMetadata(),
            publishTime: Date(),
            producerName: "test-producer",
            replicatedFrom: nil,
            topicName: "test-topic",
            redeliveryCount: 0,
            data: nil
        )
        
        // Create a mock consumer (simplified for testing)
        let modifiedMessage = await interceptor.beforeConsume(consumer: MockConsumer<String>(), message: originalMessage)
        
        #expect(await interceptor.beforeConsumeCallCount == 1)
        #expect(modifiedMessage.metadata.properties["consumed"] == "true")
        #expect(modifiedMessage.metadata.properties["interceptor"] == "MockConsumerInterceptor")
    }
    
    @Test("Producer Interceptor Acknowledgement Callback")
    func testProducerInterceptorAcknowledgement() async {
        let interceptor = MockProducerInterceptor<String>()
        let testMessage = Message<String>(
            id: MessageId(ledgerId: 1, entryId: 1),
            value: "test",
            metadata: MessageMetadata(),
            publishTime: Date(),
            producerName: "test-producer",
            replicatedFrom: nil,
            topicName: "test-topic",
            redeliveryCount: 0,
            data: nil
        )
        let messageId = MessageId(ledgerId: 10, entryId: 20)
        
        await interceptor.onSendAcknowledgement(
            producer: MockProducer<String>(),
            message: testMessage,
            messageId: messageId,
            error: nil
        )
        
        #expect(await interceptor.onSendAckCallCount == 1)
        #expect(await interceptor.lastMessageId == messageId)
        #expect(await interceptor.lastError == nil)
    }
    
    @Test("Consumer Interceptor Acknowledgement Callback")
    func testConsumerInterceptorAcknowledgement() async {
        let interceptor = MockConsumerInterceptor<String>()
        let messageIds = [
            MessageId(ledgerId: 1, entryId: 1),
            MessageId(ledgerId: 1, entryId: 2),
            MessageId(ledgerId: 1, entryId: 3)
        ]
        
        await interceptor.onAcknowledge(
            consumer: MockConsumer<String>(),
            messageIds: messageIds
        )
        
        #expect(await interceptor.onAckCallCount == 1)
        #expect(await interceptor.lastAckedMessageIds == messageIds)
    }
    
    @Test("Consumer Interceptor Negative Acknowledgement")
    func testConsumerInterceptorNegativeAck() async {
        let interceptor = MockConsumerInterceptor<String>()
        let messageIds = [
            MessageId(ledgerId: 2, entryId: 1),
            MessageId(ledgerId: 2, entryId: 2)
        ]
        
        await interceptor.onNegativeAcksSend(
            consumer: MockConsumer<String>(),
            messageIds: messageIds
        )
        
        #expect(await interceptor.onNackCallCount == 1)
        #expect(await interceptor.lastNackedMessageIds == messageIds)
    }
}

// Mock Producer for testing
struct MockProducer<T: Sendable>: ProducerProtocol {
    typealias MessageType = T
    
    var topic: String { "mock-topic" }
    var state: ClientState { .connected }
    
    // StateHolder conformance
    var stateStream: AsyncStream<ClientState> {
        AsyncStream { _ in }
    }
    
    func onStateChange(_ handler: @escaping @Sendable (ClientState) -> Void) {}
    func isFinal() -> Bool { false }
    func handleException(_ error: any Error) {}
    func stateChangedTo(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState { state }
    func stateChangedFrom(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState { state }
    
    // ProducerProtocol methods
    func send(_ message: T) async throws -> MessageId {
        return MessageId(ledgerId: 1, entryId: 1)
    }
    
    func send(_ message: T, metadata: MessageMetadata) async throws -> MessageId {
        return MessageId(ledgerId: 1, entryId: 1)
    }
    
    func sendBatch(_ messages: [T]) async throws -> [MessageId] {
        return messages.enumerated().map { MessageId(ledgerId: 1, entryId: UInt64($0.offset)) }
    }
    
    func newMessage() -> MessageBuilder<T> {
        // Return a basic message builder
        fatalError("Mock newMessage not implemented")
    }
    
    func flush() async throws {}
    func dispose() async {}
    func getProducerName() -> String { "mock-producer" }
    func getLastSequenceId() -> Int64 { 0 }
    func getStats() -> ProducerStats? { nil }
}

// Mock Consumer for testing
struct MockConsumer<T: Sendable>: ConsumerProtocol {
    typealias MessageType = T
    
    var topics: [String] { ["mock-topic"] }
    var subscription: String { "mock-subscription" }
    var state: ClientState { .connected }
    
    // StateHolder conformance
    var stateStream: AsyncStream<ClientState> {
        AsyncStream { _ in }
    }
    
    func onStateChange(_ handler: @escaping @Sendable (ClientState) -> Void) {}
    func isFinal() -> Bool { false }
    func handleException(_ error: any Error) {}
    func stateChangedTo(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState { state }
    func stateChangedFrom(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState { state }
    
    // ConsumerProtocol methods
    func receive() async throws -> Message<T> {
        // Return a default message for testing
        fatalError("Mock receive not implemented for actual use")
    }
    
    func receive(timeout: TimeInterval) async throws -> Message<T> {
        // Return a default message for testing
        fatalError("Mock receive not implemented for actual use")
    }
    
    func receiveBatch(maxMessages: Int) async throws -> [Message<T>] {
        return []
    }
    
    func acknowledge(_ message: Message<T>) async throws {}
    func acknowledge(_ messageId: MessageId) async throws {}
    func acknowledgeBatch(_ messages: [Message<T>]) async throws {}
    func acknowledgeCumulative(_ message: Message<T>) async throws {}
    func acknowledgeCumulative(_ messageId: MessageId) async throws {}
    func negativeAcknowledge(_ message: Message<T>) async throws {}
    func negativeAcknowledge(_ messageId: MessageId) async throws {}
    func unsubscribe() async throws {}
    func dispose() async {}
    func seek(to messageId: MessageId) async throws {}
    func seek(to timestamp: Date) async throws {}
    func hasMessageAvailable() async -> Bool { false }
    func getBacklogSize() async -> Int { 0 }
    func getBufferedMessageCount() async -> Int { 0 }
    func getLastMessageId() async throws -> GetLastMessageIdResponse {
        // Create a mock response using available initializer
        // GetLastMessageIdResponse requires a BaseCommand, so we'll throw an error
        throw PulsarClientError.invalidConfiguration("Mock consumer does not support getLastMessageId")
    }
    func getCurrentPosition() async -> MessageId? { nil }
}