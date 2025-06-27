/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation
import Logging
import NIOCore
import NIOPosix
import NIO

/// The main PulsarClient implementation
public final class PulsarClient: PulsarClientProtocol {
    private let serviceUrl: String
    private let logger: Logger
    
    /// Initialize a new PulsarClient
    /// - Parameters:
    ///   - serviceUrl: The Pulsar service URL (e.g., "pulsar://localhost:6650")
    ///   - logger: Logger instance for logging
    public init(serviceUrl: String, logger: Logger = Logger(label: "PulsarClient")) {
        self.serviceUrl = serviceUrl
        self.logger = logger
    }
    
    /// Create a new PulsarClient using a builder
    /// - Parameter configure: Configuration closure
    /// - Returns: A configured PulsarClient instance
    public static func builder(configure: (PulsarClientBuilder) -> Void) -> PulsarClient {
        let builder = PulsarClientBuilder()
        configure(builder)
        return builder.build()
    }
    
    // MARK: - Implementation Storage
    
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
            guard let impl = Self.implementations[ObjectIdentifier(self)] else {
                fatalError("PulsarClient implementation not initialized. Use PulsarClientBuilder to create clients.")
            }
            return impl
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
            logger: configuration.logger,
            authentication: configuration.authentication,
            encryptionPolicy: configuration.encryptionPolicy
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
    
    // MARK: - Public Methods
    
    /// Get client statistics
    public func getStatistics() async -> ClientStatistics {
        guard let impl = Self.implementations[ObjectIdentifier(self)] else {
            // Return empty statistics if disposed
            return ClientStatistics(
                activeConnections: 0,
                totalConnections: 0,
                activeProducers: 0,
                activeConsumers: 0,
                memoryUsage: 0
            )
        }
        
        let poolStats = await impl.connectionPool.getStatistics()
        let (producers, consumers, _, _, baseMemory) = await impl.tracker.getStatistics()
        
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
        // Get implementation before any cleanup
        guard let impl = Self.implementations[ObjectIdentifier(self)] else {
            // Already disposed
            return
        }
        
        impl.logger.info("Disposing PulsarClient")
        
        // Close all connections
        await impl.connectionPool.close()
        
        // Shutdown event loop group if we created it
        if let group = impl.eventLoopGroup as? MultiThreadedEventLoopGroup {
            do {
                try await group.shutdownGracefully()
            } catch {
                impl.logger.error("Error shutting down event loop group: \(error)")
            }
        }
        
        // Clear implementation
        await withCheckedContinuation { continuation in
            Self.implementationsLock.lock()
            Self.implementations.removeValue(forKey: ObjectIdentifier(self))
            Self.implementationsLock.unlock()
            continuation.resume()
        }
        
        impl.logger.info("PulsarClient disposed")
    }
    
    // MARK: - PulsarClientProtocol Implementation
    
    public func newProducer<T: Sendable>(
        topic: String,
        schema: Schema<T>,
        configure: (ProducerBuilder<T>) -> Void
    ) async throws -> any ProducerProtocol<T> {
        let producerBuilder = ProducerBuilder(options: .init(topic: topic, schema: schema))
        configure(producerBuilder)
        let producer = try await createProducer(options: producerBuilder.options)
        return producer as any ProducerProtocol<T>
    }

    public func newConsumer<T: Sendable>(
        topic: String,
        schema: Schema<T>,
        configure: (ConsumerBuilder<T>) -> Void
    ) async throws -> any ConsumerProtocol<T> {
        let consumerBuilder = ConsumerBuilder(options: .init(subscriptionName: "default", topic: topic, schema: schema))
        configure(consumerBuilder)
        let consumer = try await createConsumer(options: consumerBuilder.options)
        return consumer as any ConsumerProtocol<T>
    }

    public func newReader<T: Sendable>(
        topic: String,
        schema: Schema<T>,
        configure: (ReaderBuilder<T>) -> Void
    ) async throws -> any ReaderProtocol<T> {
        let readerBuilder = ReaderBuilder(options: .init(startMessageId: .earliest, topic: topic, schema: schema))
        configure(readerBuilder)
        let reader = try await createReader(options: readerBuilder.options)
        return reader as any ReaderProtocol<T>
    }
    
    // MARK: - Internal Factory Methods
    
    func createProducer<T: Sendable>(options: ProducerOptions<T>) async throws -> ProducerImpl<T> {
        try options.validate()
        
        // Get connection for the topic
        let connection = try await implementation.connectionPool.getConnectionForTopic(options.topic)
        
        // Create producer on the server
        let (producerId, _) = try await connection.createProducer(
            topic: options.topic,
            producerName: options.producerName,
            schema: nil,
            producerAccessMode: .shared,
            producerProperties: [:]
        )
        
        let channelManager = await connection.channelManager ?? ChannelManager()
        
        // Create producer channel and register it
        let producerChannel = ProducerChannel(
            id: producerId,
            topic: options.topic,
            producerName: options.producerName,
            connection: connection,
            schemaInfo: nil
        )
        await channelManager.registerProducer(producerChannel)
        await producerChannel.activate()
        
        let producer = ProducerImpl<T>(
            id: producerId,
            topic: options.topic,
            producerName: options.producerName,
            connection: connection,
            schema: options.schema,
            configuration: options,
            logger: implementation.logger,
            channelManager: channelManager,
            tracker: implementation.tracker
        )
        
        // Start batch sender if batching is enabled
        await producer.startBatchSender()
        
        // Register with tracker
        await implementation.tracker.registerProducer(producer, topic: options.topic)
        
        return producer
    }

    func createConsumer<T: Sendable>(options: ConsumerOptions<T>) async throws -> ConsumerImpl<T> {
        try options.validate()
        
        // Get connection for the topic
        let connection = try await implementation.connectionPool.getConnectionForTopic(options.topic)
        let channelManager = await connection.channelManager ?? ChannelManager()
        
        // CRITICAL: Pre-allocate consumer ID and register channel BEFORE sending SUBSCRIBE
        // This matches the C# DotPulsar implementation pattern
        let consumerId = await connection.commandBuilder.nextConsumerId()
        
        // Create consumer channel and register it BEFORE subscribing
        let consumerChannel = ConsumerChannel(
            id: consumerId,
            topic: options.topic,
            subscription: options.subscriptionName,
            consumerName: options.consumerName,
            connection: connection
        )
        await channelManager.registerConsumer(consumerChannel)
        
        // Subscribe to the topic on the server (now that channel is ready)
        let (actualConsumerId, _) = try await connection.subscribe(
            topic: options.topic,
            subscription: options.subscriptionName,
            subType: options.subscriptionType,
            consumerName: options.consumerName,
            initialPosition: options.initialPosition,
            schema: nil,
            preAssignedConsumerId: consumerId  // Use our pre-assigned ID
        )
        
        // Verify the IDs match (they should since we pre-assigned)
        assert(actualConsumerId == consumerId, "Consumer ID mismatch: expected \(consumerId), got \(actualConsumerId)")
        
        // Activate channel immediately after successful SUBSCRIBE (like C# implementation)
        await consumerChannel.activate()
        
        let consumer = ConsumerImpl<T>(
            id: consumerId,
            topics: options.topic.isEmpty ? options.topics : [options.topic],
            subscription: options.subscriptionName,
            connection: connection,
            schema: options.schema,
            configuration: options,
            logger: implementation.logger,
            channelManager: channelManager,
            tracker: implementation.tracker
        )
        
        // Set up message handler to route messages from channel to consumer
        await consumerChannel.setMessageHandler { [weak consumer] commandMessage, payload, metadata in
            guard let consumer = consumer else { return }
            await consumer.handleIncomingMessage(commandMessage, payload: payload, metadata: metadata)
        }
        
        // Start the consumer's receiver task
        await consumer.startReceiver()
        
        // Register with tracker
        await implementation.tracker.registerConsumer(consumer as ConsumerImpl<T>, topic: options.topic)
        
        return consumer
    }

    func createReader<T: Sendable>(options: ReaderOptions<T>) async throws -> ReaderImpl<T> {
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
        
        let consumer = try await createConsumer(options: consumerOptions) as ConsumerImpl<T>
        
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

// MARK: - PulsarClient Builder

/// PulsarClient builder
public final class PulsarClientBuilder {
    internal var serviceUrl: String = "pulsar://localhost:6650"
    internal var logger: Logger = Logger(label: "PulsarClient")
    internal var authentication: Authentication?
    internal var encryptionPolicy: EncryptionPolicy = .preferUnencrypted
    internal var operationTimeout: TimeInterval = 30.0
    
    /// Initialize a new PulsarClientBuilder
    public init() {}
    
    /// Set the service URL
    /// - Parameter url: The Pulsar service URL
    @discardableResult
    public func withServiceUrl(_ url: String) -> PulsarClientBuilder {
        self.serviceUrl = url
        return self
    }
    
    /// Set the logger
    /// - Parameter logger: The logger to use
    @discardableResult
    public func withLogger(_ logger: Logger) -> PulsarClientBuilder {
        self.logger = logger
        return self
    }
    
    /// Set the authentication
    /// - Parameter authentication: The authentication provider
    @discardableResult
    public func withAuthentication(_ authentication: Authentication) -> PulsarClientBuilder {
        self.authentication = authentication
        return self
    }
    
    /// Set the encryption policy
    /// - Parameter policy: The encryption policy to use
    @discardableResult
    public func withEncryptionPolicy(_ policy: EncryptionPolicy) -> PulsarClientBuilder {
        self.encryptionPolicy = policy
        return self
    }

    /// Set the operation timeout
    /// - Parameter timeout: The timeout (in seconds) to use
    @discardableResult
    public func withOperationTimeout(_ timeout: TimeInterval) -> PulsarClientBuilder {
        self.operationTimeout = timeout
        return self
    }
    
    /// Build the PulsarClient
    /// - Returns: A new PulsarClient instance
    public func build() -> PulsarClient {
        return buildWithConfiguration()
    }
    
    /// Build the client with full configuration
    internal func buildWithConfiguration() -> PulsarClient {
        let configuration = ClientConfiguration(
            serviceUrl: serviceUrl,
            authentication: authentication,
            encryptionPolicy: encryptionPolicy,
            operationTimeout: operationTimeout,
            ioThreads: 1,
            messageListenerThreads: 1,
            connectionsPerBroker: 1,
            useTcpNoDelay: true,
            tlsAllowInsecureConnection: false,
            tlsValidateHostname: true,
            enableTransaction: false,
            statsInterval: 60.0,
            logger: logger,
            eventLoopGroup: nil
        )
        
        return PulsarClient(configuration: configuration)
    }
}

// MARK: - Configuration

struct ClientConfiguration {
    let serviceUrl: String
    let authentication: Authentication?
    let encryptionPolicy: EncryptionPolicy
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