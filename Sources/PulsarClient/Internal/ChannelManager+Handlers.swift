import Foundation
import Logging

// MARK: - Message Handling Extensions

extension ChannelManager {
    
    /// Handle incoming message from broker
    func handleIncomingMessage(_ message: Pulsar_Proto_CommandMessage, payload: Data, metadata: Pulsar_Proto_MessageMetadata) async {
        guard let consumerChannel = consumers[message.consumerID] else {
            logger.warning("Received message for unknown consumer \(message.consumerID)")
            return
        }
        
        // Store the message in the consumer channel for the ConsumerImpl to retrieve
        await consumerChannel.handleMessage(message, payload: payload, metadata: metadata)
    }
    
    /// Handle active consumer change
    func handleActiveConsumerChange(_ change: Pulsar_Proto_CommandActiveConsumerChange) async {
        guard let consumer = consumers[change.consumerID] else {
            logger.warning("Received active consumer change for unknown consumer \(change.consumerID)")
            return
        }
        
        await consumer.handleActiveConsumerChange(change.isActive)
    }
    
    /// Handle close producer command from broker
    func handleCloseProducer(_ close: Pulsar_Proto_CommandCloseProducer) async {
        guard let producer = producers[close.producerID] else {
            logger.warning("Received close for unknown producer \(close.producerID)")
            return
        }
        
        logger.info("Broker requested to close producer \(close.producerID)")
        await producer.close()
        removeProducer(id: close.producerID)
    }
    
    /// Handle close consumer command from broker
    func handleCloseConsumer(_ close: Pulsar_Proto_CommandCloseConsumer) async {
        guard let consumer = consumers[close.consumerID] else {
            logger.warning("Received close for unknown consumer \(close.consumerID)")
            return
        }
        
        logger.info("Broker requested to close consumer \(close.consumerID)")
        await consumer.close()
        removeConsumer(id: close.consumerID)
    }
    
    /// Reconnect all channels after connection recovery
    func reconnectAll() async {
        logger.info("Reconnecting all channels")
        
        // Reconnect all producers
        await withTaskGroup(of: Void.self) { group in
            for (_, producer) in producers {
                group.addTask {
                    await producer.reconnect()
                }
            }
        }
        
        // Reconnect all consumers
        await withTaskGroup(of: Void.self) { group in
            for (_, consumer) in consumers {
                group.addTask {
                    await consumer.reconnect()
                }
            }
        }
    }
}

// MARK: - Enhanced Producer Channel

extension ProducerChannel {
    
    /// Reconnect the producer
    func reconnect() async {
        guard state == .active else { return }
        
        guard let connection = connection else {
            logger.warning("Cannot reconnect producer \(id) - no connection available")
            return
        }
        
        do {
            // Re-send PRODUCER command to re-establish the producer
            let commandBuilder = await connection.commandBuilder
            let (command, _) = commandBuilder.createProducer(
                topic: topic,
                producerName: producerName,
                schema: getSchemaInfo()
            )
            
            let frame = PulsarFrame(command: command)
            let _ = try await connection.sendRequest(frame, responseType: ProducerSuccessResponse.self)
            
            logger.info("Successfully reconnected producer \(id) for topic \(topic)")
        } catch {
            logger.error("Failed to reconnect producer \(id): \(error)")
            updateState(.closed)
        }
    }
    
    /// Send a message
    func send(_ frame: PulsarFrame) async throws {
        guard state == .active else {
            throw PulsarClientError.producerBusy("Producer not active")
        }
        
        guard let connection = connection else {
            throw PulsarClientError.connectionFailed("No connection available")
        }
        
        try await connection.send(frame: frame)
    }
}

// MARK: - Enhanced Consumer Channel

extension ConsumerChannel {
    private struct MessageState {
        var pendingMessages: [Pulsar_Proto_CommandMessage] = []
        var isActive: Bool = true
    }
    
    private static var messageStates: [UInt64: MessageState] = [:]
    
    /// Handle incoming message
    func handleMessage(_ message: Pulsar_Proto_CommandMessage, payload: Data, metadata: Pulsar_Proto_MessageMetadata) async {
        guard state == .active else {
            logger.warning("Received message for inactive consumer \(id)")
            return
        }
        
        // Forward to the handler if set
        if let handler = messageHandler {
            await handler(message, payload, metadata)
        } else {
            logger.warning("No message handler set for consumer \(id)")
        }
    }
    
    /// Handle active consumer change
    func handleActiveConsumerChange(_ isActive: Bool) async {
        Self.messageStates[id, default: MessageState()].isActive = isActive
        logger.info("Consumer \(id) active state changed to \(isActive)")
    }
    
    /// Reconnect the consumer
    func reconnect() async {
        guard state == .active else { return }
        
        guard let connection = connection else {
            logger.warning("Cannot reconnect consumer \(id) - no connection available")
            return
        }
        
        do {
            // Re-send SUBSCRIBE command to re-establish the consumer
            let commandBuilder = await connection.commandBuilder
            let config = getSubscriptionConfig()
            let (command, _) = commandBuilder.subscribe(
                topic: topic,
                subscription: subscription,
                subType: config.type,
                consumerName: consumerName,
                initialPosition: config.initialPosition,
                schema: config.schemaInfo
            )
            
            let frame = PulsarFrame(command: command)
            let _ = try await connection.sendRequest(frame, responseType: SuccessResponse.self)
            
            logger.info("Successfully reconnected consumer \(id) for topic \(topic), subscription \(subscription)")
        } catch {
            logger.error("Failed to reconnect consumer \(id): \(error)")
            updateState(.closed)
        }
    }
    
    /// Acknowledge a message
    func acknowledge(messageId: MessageId) async throws {
        guard state == .active else {
            throw PulsarClientError.consumerBusy("Consumer not active")
        }
        
        guard let connection = connection else {
            throw PulsarClientError.connectionFailed("No connection available")
        }
        
        // Send ACK command
        let commandBuilder = await connection.commandBuilder
        let ackCommand = commandBuilder.ack(consumerId: id, messageId: messageId)
        let frame = PulsarFrame(command: ackCommand)
        
        try await connection.sendCommand(frame)
        logger.debug("Acknowledged message \(messageId) for consumer \(id)")
    }
}

// Logging extension
private let logger = Logger(label: "ChannelManager")