import Foundation
import Logging

/// Protocol for topic routing strategies
public protocol TopicRouter<T>: Sendable where T: Sendable {
    associatedtype T
    
    /// Select which topic to send a message to
    /// - Parameters:
    ///   - message: The message to route
    ///   - metadata: Message metadata
    ///   - topics: Available topics
    /// - Returns: The selected topic
    func selectTopic(
        for message: T,
        metadata: MessageMetadata,
        topics: [String]
    ) async -> String
}

/// Round-robin topic router
public actor RoundRobinTopicRouter<T: Sendable>: TopicRouter {
    private var index: Int = 0
    
    public init() {}
    
    public func selectTopic(
        for message: T,
        metadata: MessageMetadata,
        topics: [String]
    ) async -> String {
        guard !topics.isEmpty else {
            return ""
        }
        
        let topic = topics[index % topics.count]
        index = (index + 1) % topics.count
        return topic
    }
}

/// Key-based topic router
public struct KeyBasedTopicRouter<T: Sendable>: TopicRouter {
    public init() {}
    
    public func selectTopic(
        for message: T,
        metadata: MessageMetadata,
        topics: [String]
    ) async -> String {
        guard !topics.isEmpty else {
            return ""
        }
        
        // Route based on message key
        if let key = metadata.key {
            let hash = key.hashValue
            let index = abs(hash) % topics.count
            return topics[index]
        }
        
        // Fall back to first topic if no key
        return topics[0]
    }
}

/// Custom topic router that uses a closure
public struct CustomTopicRouter<T: Sendable>: TopicRouter {
    private let selector: @Sendable (T, MessageMetadata, [String]) async -> String
    
    public init(selector: @escaping @Sendable (T, MessageMetadata, [String]) async -> String) {
        self.selector = selector
    }
    
    public func selectTopic(
        for message: T,
        metadata: MessageMetadata,
        topics: [String]
    ) async -> String {
        return await selector(message, metadata, topics)
    }
}

/// Multi-topic producer implementation
public actor MultiTopicProducer<T: Sendable>: ProducerProtocol {
    public typealias MessageType = T
    
    private let logger = Logger(label: "MultiTopicProducer")
    private let client: PulsarClient
    private let topics: [String]
    private let schema: Schema<T>
    private let configuration: ProducerOptions<T>
    private let topicRouter: any TopicRouter<T>
    
    // Map of topic to producer
    private var producers: [String: any ProducerProtocol<T>] = [:]
    
    // State management
    private var _state: ClientState = .initializing
    public let stateStream: AsyncStream<ClientState>
    private let stateContinuation: AsyncStream<ClientState>.Continuation
    private var initializationTask: Task<Void, Never>?
    
    public nonisolated var topic: String {
        return topics.joined(separator: ",")
    }
    
    public nonisolated var state: ClientState {
        // For nonisolated access, return a conservative default
        return .connected
    }
    
    public var stateChanges: AsyncStream<ClientState> {
        return stateStream
    }
    
    public init(
        client: PulsarClient,
        topics: [String],
        schema: Schema<T>,
        configuration: ProducerOptions<T>,
        topicRouter: (any TopicRouter<T>)? = nil
    ) async {
        self.client = client
        self.topics = topics
        self.schema = schema
        self.configuration = configuration
        self.topicRouter = topicRouter ?? RoundRobinTopicRouter<T>()
        
        (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()
        
        self.initializationTask = Task {
            await initialize()
        }
    }
    
    private func initialize() async {
        do {
            // Create producers for all topics upfront
            for topic in topics {
                let producer = try await createProducer(for: topic)
                producers[topic] = producer
            }
            
            _state = .connected
            stateContinuation.yield(.connected)
            
            logger.debug("Multi-topic producer initialized", metadata: [
                "topics": "\(topics)",
                "count": "\(topics.count)"
            ])
        } catch {
            _state = .faulted(error)
            stateContinuation.yield(.faulted(error))
            logger.error("Failed to initialize multi-topic producer", metadata: [
                "error": "\(error)"
            ])
        }
    }
    
    private func createProducer(for topic: String) async throws -> any ProducerProtocol<T> {
        // Create a copy of configuration with the specific topic
        var topicConfig = configuration
        topicConfig.topic = topic
        
        return try await client.newProducer(
            topic: topic,
            schema: schema
        ) { builder in
            // Copy all configuration from the multi-topic producer
            builder.producerName(topicConfig.producerName.map { "\($0)-\(topic)" })
            builder.initialSequenceId(topicConfig.initialSequenceId)
            builder.sendTimeout(topicConfig.sendTimeout)
            builder.batchingEnabled(topicConfig.batchingEnabled)
            builder.batchingMaxMessages(topicConfig.batchingMaxMessages)
            builder.batchingMaxDelay(topicConfig.batchingMaxDelay)
            builder.batchingMaxBytes(topicConfig.batchingMaxBytes)
            builder.compressionType(topicConfig.compressionType)
            builder.hashingScheme(topicConfig.hashingScheme)
            builder.messageRouter(topicConfig.messageRouter)
            builder.cryptoKeyReader(topicConfig.cryptoKeyReader)
            builder.encryptionKeys(topicConfig.encryptionKeys)
            builder.producerAccessMode(topicConfig.producerAccessMode)
            builder.attachTraceInfoToMessages(topicConfig.attachTraceInfoToMessages)
            builder.maxPendingMessages(topicConfig.maxPendingMessages)
            builder.properties(topicConfig.producerProperties)
            
            // Note: Interceptors are not propagated to avoid double processing
        }
    }
    
    private func getOrCreateProducer(for topic: String) async throws -> any ProducerProtocol<T> {
        if let producer = producers[topic] {
            return producer
        }
        
        // Create new producer
        let producer = try await createProducer(for: topic)
        producers[topic] = producer
        
        logger.debug("Created producer for topic", metadata: [
            "topic": "\(topic)"
        ])
        
        return producer
    }
    
    public func send(_ message: T) async throws -> MessageId {
        return try await send(message, metadata: MessageMetadata())
    }
    
    public func send(_ message: T, metadata: MessageMetadata) async throws -> MessageId {
        // Wait for initialization to complete
        await initializationTask?.value
        
        guard _state == .connected else {
            throw PulsarClientError.producerBusy("Multi-topic producer not connected")
        }
        
        // Select topic using router
        let selectedTopic = await topicRouter.selectTopic(
            for: message,
            metadata: metadata,
            topics: topics
        )
        
        // Get or create producer for the topic
        let producer = try await getOrCreateProducer(for: selectedTopic)
        
        // Send message
        let messageId = try await producer.send(message, metadata: metadata)
        
        logger.trace("Sent message to topic", metadata: [
            "topic": "\(selectedTopic)",
            "messageId": "\(messageId)"
        ])
        
        return messageId
    }
    
    public func sendBatch(_ messages: [T]) async throws -> [MessageId] {
        // Wait for initialization to complete
        await initializationTask?.value
        
        guard _state == .connected else {
            throw PulsarClientError.producerBusy("Multi-topic producer not connected")
        }
        
        var messageIds: [MessageId] = []
        
        // Send each message individually
        // In a production implementation, we might batch by topic
        for message in messages {
            let messageId = try await send(message)
            messageIds.append(messageId)
        }
        
        return messageIds
    }
    
    public func flush() async throws {
        // Wait for initialization to complete
        await initializationTask?.value
        
        guard _state == .connected else {
            throw PulsarClientError.producerBusy("Multi-topic producer not connected")
        }
        
        // Flush all producers
        await withTaskGroup(of: Void.self) { group in
            for (_, producer) in producers {
                group.addTask {
                    try? await producer.flush()
                }
            }
        }
    }
    
    public func dispose() async {
        guard _state != .closed && _state != .closing else { return }
        
        _state = .closing
        stateContinuation.yield(.closing)
        
        // Dispose all producers
        await withTaskGroup(of: Void.self) { group in
            for (_, producer) in producers {
                group.addTask {
                    await producer.dispose()
                }
            }
        }
        
        producers.removeAll()
        
        _state = .closed
        stateContinuation.yield(.closed)
        stateContinuation.finish()
        
        logger.debug("Multi-topic producer disposed")
    }
    
    // MARK: - StateHolder
    
    public nonisolated func onStateChange(_ handler: @escaping @Sendable (ClientState) -> Void) {
        Task { [weak self] in
            guard let self = self else { return }
            let stateChanges = await self.stateChanges
            for await state in stateChanges {
                handler(state)
            }
        }
    }
    
    public nonisolated func isFinal() -> Bool {
        // For nonisolated access, we need to be conservative
        return false
    }
    
    public nonisolated func handleException(_ error: Error) {
        Task { [weak self] in
            await self?.setFaulted(error)
        }
    }
    
    private func setFaulted(_ error: Error) {
        _state = .faulted(error)
        stateContinuation.yield(.faulted(error))
    }
    
    public func stateChangedTo(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState {
        if _state == state {
            return _state
        }
        
        return try await withThrowingTaskGroup(of: ClientState.self) { group in
            // Add timeout task
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                throw PulsarClientError.timeout("State change timeout")
            }
            
            // Add state monitoring task
            group.addTask { [weak self] in
                guard let self = self else { throw PulsarClientError.clientClosed }
                
                for await newState in await self.stateChanges {
                    if newState == state {
                        return newState
                    }
                }
                throw PulsarClientError.clientClosed
            }
            
            let result = try await group.next()!
            group.cancelAll()
            return result
        }
    }
    
    public func stateChangedFrom(_ state: ClientState, timeout: TimeInterval) async throws -> ClientState {
        if _state != state {
            return _state
        }
        
        return try await withThrowingTaskGroup(of: ClientState.self) { group in
            // Add timeout task
            group.addTask {
                try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
                throw PulsarClientError.timeout("State change timeout")
            }
            
            // Add state monitoring task
            group.addTask { [weak self] in
                guard let self = self else { throw PulsarClientError.clientClosed }
                
                for await newState in await self.stateChanges {
                    if newState != state {
                        return newState
                    }
                }
                throw PulsarClientError.clientClosed
            }
            
            let result = try await group.next()!
            group.cancelAll()
            return result
        }
    }
}

/// Builder for multi-topic producer
public final class MultiTopicProducerBuilder<T: Sendable> {
    private let client: PulsarClient
    private var topics: [String]
    private let schema: Schema<T>
    private var configuration: ProducerOptions<T>
    private var topicRouter: (any TopicRouter<T>)?
    
    init(client: PulsarClient, topics: [String], schema: Schema<T>) {
        self.client = client
        self.topics = topics
        self.schema = schema
        self.configuration = ProducerOptions(topic: "", schema: schema)
    }
    
    @discardableResult
    public func topics(_ topics: [String]) -> Self {
        self.topics = topics
        return self
    }
    
    @discardableResult
    public func topicRouter(_ router: any TopicRouter<T>) -> Self {
        self.topicRouter = router
        return self
    }
    
    @discardableResult
    public func configure(_ block: (ProducerBuilder<T>) -> Void) -> Self {
        let builder = ProducerBuilder<T>(options: configuration)
        block(builder)
        self.configuration = builder.options
        return self
    }
    
    public func build() async throws -> MultiTopicProducer<T> {
        return await MultiTopicProducer(
            client: client,
            topics: topics,
            schema: schema,
            configuration: configuration,
            topicRouter: topicRouter
        )
    }
}

