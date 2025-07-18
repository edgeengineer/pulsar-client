import Foundation
import Logging

/// Channel state
enum ChannelState: Sendable {
    case inactive
    case active
    case closing
    case closed
}

/// Base channel protocol
protocol PulsarChannel: Actor {
    var id: UInt64 { get }
    var state: ChannelState { get }
    func close() async
}

/// Producer channel
actor ProducerChannel: PulsarChannel {
    let id: UInt64
    let topic: String
    let producerName: String?
    private(set) var state: ChannelState = .inactive
    internal weak var connection: Connection?
    
    // Send receipt handling - store a type-erased wrapper
    private var pendingSends: [UInt64: SendOperationWrapper] = [:]
    
    // Store schema info for reconnection
    private var schemaInfo: SchemaInfo?
    
    init(id: UInt64, topic: String, producerName: String?, connection: Connection, schemaInfo: SchemaInfo? = nil) {
        self.id = id
        self.topic = topic
        self.producerName = producerName
        self.connection = connection
        self.schemaInfo = schemaInfo
    }
    
    func activate() {
        state = .active
    }
    
    func updateState(_ newState: ChannelState) {
        state = newState
    }
    
    
    
    
    /// Register a send operation (non-blocking, like C#)
    func registerSendOperation<T>(_ operation: SendOperation<T>) {
        logger.info("Registering send operation for sequence \(operation.sequenceId)")
        let wrapper = AnySendOperationWrapper(operation)
        pendingSends[operation.sequenceId] = wrapper
        logger.info("Send operation registered. Total pending: \(pendingSends.count)")
    }
    
    /// Handle incoming send receipt
    func handleSendReceipt(_ receipt: Pulsar_Proto_CommandSendReceipt) {
        logger.info("ProducerChannel handling send receipt for sequence \(receipt.sequenceID)")
        logger.info("Current pending sends: \(pendingSends.keys.sorted())")
        
        if let wrapper = pendingSends.removeValue(forKey: receipt.sequenceID) {
            let messageId = MessageId(
                ledgerId: receipt.messageID.ledgerID,
                entryId: receipt.messageID.entryID,
                partition: receipt.messageID.hasPartition ? receipt.messageID.partition : -1,
                batchIndex: receipt.messageID.hasBatchIndex ? receipt.messageID.batchIndex : -1
            )
            logger.info("Found send operation for sequence \(receipt.sequenceID), completing with message ID: \(messageId)")
            
            // Complete the operation
            wrapper.complete(with: messageId)
        } else {
            logger.warning("Received send receipt for unknown sequence ID \(receipt.sequenceID). Pending sequences: \(pendingSends.keys.sorted())")
        }
    }
    
    func close() async {
        guard state == .active else { return }
        state = .closing
        
        // Cancel all pending sends
        for (_, wrapper) in pendingSends {
            wrapper.fail(with: PulsarClientError.producerBusy("Producer closing"))
        }
        pendingSends.removeAll()
        
        state = .closed
    }
    
    /// Set schema info for reconnection
    func setSchemaInfo(_ schemaInfo: SchemaInfo?) {
        self.schemaInfo = schemaInfo
    }
    
    /// Get schema info
    func getSchemaInfo() -> SchemaInfo? {
        return schemaInfo
    }
}

/// Consumer channel
actor ConsumerChannel: PulsarChannel {
    let id: UInt64
    let topic: String
    let subscription: String
    let consumerName: String?
    private(set) var state: ChannelState = .inactive
    internal weak var connection: Connection?
    
    // Message delivery callback
    internal var messageHandler: ((Pulsar_Proto_CommandMessage, Data, Pulsar_Proto_MessageMetadata) async -> Void)?
    
    // Store subscription configuration for reconnection
    private var subscriptionType: SubscriptionType = .exclusive
    private var initialPosition: SubscriptionInitialPosition = .latest
    private var schemaInfo: SchemaInfo?
    
    init(id: UInt64, topic: String, subscription: String, consumerName: String?, connection: Connection,
         subscriptionType: SubscriptionType = .exclusive,
         initialPosition: SubscriptionInitialPosition = .latest,
         schemaInfo: SchemaInfo? = nil) {
        self.id = id
        self.topic = topic
        self.subscription = subscription
        self.consumerName = consumerName
        self.connection = connection
        self.subscriptionType = subscriptionType
        self.initialPosition = initialPosition
        self.schemaInfo = schemaInfo
    }
    
    func activate() {
        state = .active
    }
    
    func updateState(_ newState: ChannelState) {
        state = newState
    }
    
    /// Set the message handler callback
    func setMessageHandler(_ handler: @escaping (Pulsar_Proto_CommandMessage, Data, Pulsar_Proto_MessageMetadata) async -> Void) {
        self.messageHandler = handler
    }
    
    
    func close() async {
        guard state == .active else { return }
        state = .closing
        
        // Clear message handler
        messageHandler = nil
        
        state = .closed
    }
    
    /// Set subscription configuration
    func setSubscriptionConfig(type: SubscriptionType, initialPosition: SubscriptionInitialPosition) {
        self.subscriptionType = type
        self.initialPosition = initialPosition
    }
    
    /// Set schema info for reconnection
    func setSchemaInfo(_ schemaInfo: SchemaInfo?) {
        self.schemaInfo = schemaInfo
    }
    
    /// Get subscription configuration
    func getSubscriptionConfig() -> (type: SubscriptionType, initialPosition: SubscriptionInitialPosition, schemaInfo: SchemaInfo?) {
        return (subscriptionType, initialPosition, schemaInfo)
    }
}

// Add logger
private let logger = Logger(label: "ProducerChannel")

/// Channel manager for a connection
actor ChannelManager {
    private let logger: Logger
    internal var producers: [UInt64: ProducerChannel] = [:]
    internal var consumers: [UInt64: ConsumerChannel] = [:]
    
    init(logger: Logger = Logger(label: "ChannelManager")) {
        self.logger = logger
    }
    
    /// Get count of active producers
    func getProducerCount() -> Int {
        return producers.count
    }
    
    /// Get count of active consumers
    func getConsumerCount() -> Int {
        return consumers.count
    }
    
    /// Register a producer channel
    func registerProducer(_ channel: ProducerChannel) {
        producers[channel.id] = channel
        logger.debug("Registered producer \(channel.id) for topic \(channel.topic)")
    }
    
    /// Register a consumer channel
    func registerConsumer(_ channel: ConsumerChannel) {
        consumers[channel.id] = channel
        logger.debug("Registered consumer \(channel.id) for topic \(channel.topic)")
    }
    
    /// Get producer by ID
    func getProducer(id: UInt64) -> ProducerChannel? {
        producers[id]
    }
    
    /// Get consumer by ID
    func getConsumer(id: UInt64) -> ConsumerChannel? {
        consumers[id]
    }
    
    /// Remove producer
    func removeProducer(id: UInt64) {
        if let producer = producers.removeValue(forKey: id) {
            logger.debug("Removed producer \(id) for topic \(producer.topic)")
        }
    }
    
    /// Remove consumer
    func removeConsumer(id: UInt64) {
        if let consumer = consumers.removeValue(forKey: id) {
            logger.debug("Removed consumer \(id) for topic \(consumer.topic)")
        }
    }
    
    /// Close all channels
    func closeAll() async {
        // Close all producers
        await withTaskGroup(of: Void.self) { group in
            for (_, producer) in producers {
                group.addTask {
                    await producer.close()
                }
            }
        }
        producers.removeAll()
        
        // Close all consumers
        await withTaskGroup(of: Void.self) { group in
            for (_, consumer) in consumers {
                group.addTask {
                    await consumer.close()
                }
            }
        }
        consumers.removeAll()
    }
}