import Foundation
import Logging
import NIOCore
import NIOPosix
import NIO

/// Full PulsarClient implementation
extension PulsarClient {
    
    /// Internal implementation details
    private struct Implementation {
        let serviceUrl: String
        let logger: Logger
        let eventLoopGroup: EventLoopGroup
        let connectionPool: ConnectionPool
        let authentication: Authentication?
        let operationTimeout: TimeInterval
        let statsInterval: TimeInterval
        let configuration: ClientConfiguration
        let tracker: ClientTracker
    }
    
    private static let implementationsLock = NSLock()
    nonisolated(unsafe) private static var implementations: [ObjectIdentifier: Implementation] = [:]
    
    private var implementation: Implementation {
        get {
            Self.implementationsLock.lock()
            defer { Self.implementationsLock.unlock() }
            return Self.implementations[ObjectIdentifier(self)]!
        }
        set {
            Self.implementationsLock.lock()
            defer { Self.implementationsLock.unlock() }
            Self.implementations[ObjectIdentifier(self)] = newValue
        }
    }
    
    /// Initialize with full configuration
    internal convenience init(configuration: ClientConfiguration) {
        self.init(serviceUrl: configuration.serviceUrl, logger: configuration.logger)
        
        let eventLoopGroup = configuration.eventLoopGroup ?? MultiThreadedEventLoopGroup(numberOfThreads: configuration.ioThreads)
        let connectionPool = ConnectionPool(
            serviceUrl: configuration.serviceUrl,
            eventLoopGroup: eventLoopGroup,
            logger: configuration.logger
        )
        
        self.implementation = Implementation(
            serviceUrl: configuration.serviceUrl,
            logger: configuration.logger,
            eventLoopGroup: eventLoopGroup,
            connectionPool: connectionPool,
            authentication: configuration.authentication,
            operationTimeout: configuration.operationTimeout,
            statsInterval: configuration.statsInterval,
            configuration: configuration,
            tracker: ClientTracker()
        )
        
        // Start connection pool health monitoring
        Task {
            await connectionPool.startHealthMonitoring()
        }
    }
    
    /// Get client statistics
    public func getStatistics() async -> ClientStatistics {
        let poolStats = await implementation.connectionPool.getStatistics()
        let (producers, consumers, _, _, baseMemory) = await implementation.tracker.getStatistics()
        
        // Calculate total memory usage including connections
        let connectionMemory = Int64(poolStats.totalConnections) * 200 * 1024 // 200KB per connection
        let totalMemory = baseMemory + connectionMemory
        
        return ClientStatistics(
            activeConnections: poolStats.activeConnections,
            totalConnections: poolStats.totalConnections,
            activeProducers: producers,
            activeConsumers: consumers,
            memoryUsage: totalMemory
        )
    }
    
    /// Enhanced dispose with cleanup
    public func dispose() async {
        implementation.logger.info("Disposing PulsarClient")
        
        // Close all connections
        await implementation.connectionPool.close()
        
        // Shutdown event loop group if we created it
        if let group = implementation.eventLoopGroup as? MultiThreadedEventLoopGroup {
            do {
                try await group.shutdownGracefully()
            } catch {
                implementation.logger.error("Error shutting down event loop group: \(error)")
            }
        }
        
        // Clear implementation
        await withCheckedContinuation { continuation in
            Self.implementationsLock.lock()
            Self.implementations.removeValue(forKey: ObjectIdentifier(self))
            Self.implementationsLock.unlock()
            continuation.resume()
        }
        
        implementation.logger.info("PulsarClient disposed")
    }
}

// MARK: - Configuration

struct ClientConfiguration {
    let serviceUrl: String
    let authentication: Authentication?
    let operationTimeout: TimeInterval
    let ioThreads: Int
    let messageListenerThreads: Int
    let connectionsPerBroker: Int
    let useTcpNoDelay: Bool
    let tlsAllowInsecureConnection: Bool
    let tlsValidateHostname: Bool
    let enableTransaction: Bool
    let statsInterval: TimeInterval
    let logger: Logger
    let eventLoopGroup: EventLoopGroup?
}

/// Client statistics
public struct ClientStatistics {
    public let activeConnections: Int
    public let totalConnections: Int
    public let activeProducers: Int
    public let activeConsumers: Int
    public let memoryUsage: Int64
}

// MARK: - Builder Extensions

extension PulsarClientBuilder {
    // Additional properties for full configuration
    internal var authentication: Authentication? = nil
    internal var operationTimeout: TimeInterval = 30.0
    internal var ioThreads: Int = 1
    internal var messageListenerThreads: Int = 1
    internal var connectionsPerBroker: Int = 1
    internal var useTcpNoDelay: Bool = true
    internal var tlsAllowInsecureConnection: Bool = false
    internal var tlsValidateHostname: Bool = true
    internal var enableTransaction: Bool = false
    internal var statsInterval: TimeInterval = 60.0
    
    /// Build the client with full configuration
    internal func buildWithConfiguration() -> PulsarClient {
        let configuration = ClientConfiguration(
            serviceUrl: serviceUrl,
            authentication: authentication,
            operationTimeout: operationTimeout,
            ioThreads: ioThreads,
            messageListenerThreads: messageListenerThreads,
            connectionsPerBroker: connectionsPerBroker,
            useTcpNoDelay: useTcpNoDelay,
            tlsAllowInsecureConnection: tlsAllowInsecureConnection,
            tlsValidateHostname: tlsValidateHostname,
            enableTransaction: enableTransaction,
            statsInterval: statsInterval,
            logger: logger,
            eventLoopGroup: nil
        )
        
        return PulsarClient(configuration: configuration)
    }
}

// MARK: - Client Tracker

/// Tracks active producers and consumers for statistics
actor ClientTracker {
    private var activeProducers: Set<ObjectIdentifier> = []
    private var activeConsumers: Set<ObjectIdentifier> = []
    private var producerTopics: [ObjectIdentifier: String] = [:]
    private var consumerTopics: [ObjectIdentifier: String] = [:]
    
    // Memory tracking
    private var estimatedMemoryUsage: Int64 = 0
    private let baseMemoryOverhead: Int64 = 1024 * 100 // 100KB base overhead
    
    func registerProducer<T>(_ producer: ProducerImpl<T>, topic: String) {
        activeProducers.insert(ObjectIdentifier(producer))
        producerTopics[ObjectIdentifier(producer)] = topic
    }
    
    func unregisterProducer<T>(_ producer: ProducerImpl<T>) {
        activeProducers.remove(ObjectIdentifier(producer))
        producerTopics.removeValue(forKey: ObjectIdentifier(producer))
    }
    
    func registerConsumer<T>(_ consumer: ConsumerImpl<T>, topic: String) {
        activeConsumers.insert(ObjectIdentifier(consumer))
        consumerTopics[ObjectIdentifier(consumer)] = topic
    }
    
    func unregisterConsumer<T>(_ consumer: ConsumerImpl<T>) {
        activeConsumers.remove(ObjectIdentifier(consumer))
        consumerTopics.removeValue(forKey: ObjectIdentifier(consumer))
    }
    
    func getStatistics() -> (producers: Int, consumers: Int, producerTopics: Set<String>, consumerTopics: Set<String>, memoryUsage: Int64) {
        let producerTopicSet = Set(producerTopics.values)
        let consumerTopicSet = Set(consumerTopics.values)
        
        // Calculate estimated memory usage
        var memoryUsage = baseMemoryOverhead
        
        // Each producer/consumer has overhead
        let producerOverhead: Int64 = 1024 * 50 // 50KB per producer (includes batching buffers)
        let consumerOverhead: Int64 = 1024 * 100 // 100KB per consumer (includes receive queue)
        
        memoryUsage += Int64(activeProducers.count) * producerOverhead
        memoryUsage += Int64(activeConsumers.count) * consumerOverhead
        
        // Note: Connection pool memory is calculated separately in getStatistics()
        
        return (activeProducers.count, activeConsumers.count, producerTopicSet, consumerTopicSet, memoryUsage)
    }
}

extension PulsarClient {
    public func newProducer<T: Sendable>(
        topic: String,
        schema: Schema<T>,
        configure: (ProducerBuilder<T>) -> Void
    ) async throws -> any ProducerProtocol<T> {
        let producerBuilder = ProducerBuilder(options: .init(topic: topic, schema: schema))
        configure(producerBuilder)
        return try await createProducer(options: producerBuilder.options)
    }

    public func newConsumer<T: Sendable>(
        topic: String,
        schema: Schema<T>,
        configure: (ConsumerBuilder<T>) -> Void
    ) async throws -> any ConsumerProtocol<T> {
        let consumerBuilder = ConsumerBuilder(options: .init(subscriptionName: "default", topic: topic, schema: schema))
        configure(consumerBuilder)
        return try await createConsumer(options: consumerBuilder.options)
    }

    public func newReader<T: Sendable>(
        topic: String,
        schema: Schema<T>,
        configure: (ReaderBuilder<T>) -> Void
    ) async throws -> any ReaderProtocol<T> {
        let readerBuilder = ReaderBuilder(options: .init(startMessageId: .earliest, topic: topic, schema: schema))
        configure(readerBuilder)
        return try await createReader(options: readerBuilder.options)
    }
}

extension PulsarClient {
    func createProducer<T: Sendable>(options: ProducerOptions<T>) async throws -> any ProducerProtocol<T> {
        try options.validate()
        
        // Get connection for the topic
        let connection = try await implementation.connectionPool.getConnection(for: options.topic)
        
        // Create producer
        let producer = try await ProducerImpl<T>(
            client: self,
            connection: connection,
            options: options
        )
        
        // Register with tracker
        await implementation.tracker.registerProducer(producer, topic: options.topic)
        
        return producer
    }

    func createConsumer<T: Sendable>(options: ConsumerOptions<T>) async throws -> any ConsumerProtocol<T> {
        try options.validate()
        
        // Get connection for the topic
        let connection = try await implementation.connectionPool.getConnection(for: options.topic)
        
        // Create consumer
        let consumer = try await ConsumerImpl<T>(
            client: self,
            connection: connection,
            options: options
        )
        
        // Register with tracker
        await implementation.tracker.registerConsumer(consumer, topic: options.topic)
        
        return consumer
    }

    func createReader<T: Sendable>(options: ReaderOptions<T>) async throws -> any ReaderProtocol<T> {
        try options.validate()
        
        // Create a consumer with reader configuration
        let consumerOptions = ConsumerOptions<T>(
            subscriptionName: options.subscriptionName ?? "reader-\(UUID().uuidString)",
            topic: options.topic,
            schema: options.schema
        )
            .withConsumerName(options.readerName)
            .withMessagePrefetchCount(options.messagePrefetchCount)
            .withReadCompacted(options.readCompacted)
        
        let consumer = try await createConsumer(options: consumerOptions)
        
        // Create reader wrapping the consumer
        let reader = ReaderImpl<T>(
            consumer: consumer,
            topic: options.topic,
            startMessageId: options.startMessageId,
            logger: implementation.logger,
            options: options
        )
        
        return reader
    }
}