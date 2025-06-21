import Testing
@testable import PulsarClient
import Foundation
import NIOCore
import NIOPosix
import Logging

@Suite("Enhanced Connection Tests")
struct ConnectionEnhancedTests {
    
    @Test("Response command parsing")
    func testResponseCommandParsing() throws {
        // Test ConnectedResponse
        var command = Pulsar_Proto_BaseCommand()
        command.type = .connected
        command.connected.serverVersion = "Pulsar Server 3.0.0"
        command.connected.protocolVersion = 19
        command.connected.maxMessageSize = 5242880
        
        let connectedResponse = try ConnectedResponse(from: command)
        #expect(connectedResponse.serverVersion == "Pulsar Server 3.0.0")
        #expect(connectedResponse.protocolVersion == 19)
        #expect(connectedResponse.maxMessageSize == 5242880)
        
        // Test ProducerSuccessResponse
        command = Pulsar_Proto_BaseCommand()
        command.type = .producerSuccess
        command.producerSuccess.requestID = 1
        command.producerSuccess.producerName = "test-producer-1"
        command.producerSuccess.lastSequenceID = 100
        
        let producerResponse = try ProducerSuccessResponse(from: command)
        #expect(producerResponse.producerName == "test-producer-1")
        #expect(producerResponse.lastSequenceId == 100)
        
        // Test LookupResponse
        command = Pulsar_Proto_BaseCommand()
        command.type = .lookupResponse
        command.lookupTopicResponse.response = .connect
        command.lookupTopicResponse.brokerServiceURL = "pulsar://broker-1:6650"
        command.lookupTopicResponse.brokerServiceURLTls = "pulsar+ssl://broker-1:6651"
        command.lookupTopicResponse.authoritative = true
        command.lookupTopicResponse.proxyThroughServiceURL = false
        
        let lookupResponse = try LookupResponse(from: command)
        #expect(lookupResponse.authoritative == true)
        #expect(lookupResponse.proxyThroughServiceUrl == false)
        
        if case .connect(let url, let tlsUrl) = lookupResponse.response {
            #expect(url == "pulsar://broker-1:6650")
            #expect(tlsUrl == "pulsar+ssl://broker-1:6651")
        } else {
            Issue.record("Expected connect response")
        }
    }
    
    @Test("Server error mapping")
    func testServerErrorMapping() {
        // Test various server errors
        let errors: [(Pulsar_Proto_ServerError, String)] = [
            (.topicNotFound, "Topic not found"),
            (.subscriptionNotFound, "Subscription not found"),
            (.authenticationError, "Authentication error"),
            (.authorizationError, "Authorization error"),
            (.consumerBusy, "Consumer busy"),
            (.tooManyRequests, "Too many requests"),
            (.checksumError, "Checksum verification failed")
        ]
        
        for (serverError, expectedMessage) in errors {
            let mappedError = mapServerError(serverError)
            if let pulsarError = mappedError as? PulsarClientError {
                #expect(pulsarError.localizedDescription.contains(expectedMessage))
            } else {
                Issue.record("Expected PulsarClientError for \(serverError)")
            }
        }
    }
    
    @Test("Connection pool statistics")
    func testConnectionPoolStatistics() async throws {
        let pool = ConnectionPool(serviceUrl: "pulsar://localhost:6650")
        defer {
            Task {
                await pool.close()
            }
        }
        
        let stats = await pool.getStatistics()
        #expect(stats.totalConnections == 0)
        #expect(stats.activeConnections == 0)
        #expect(stats.connectingConnections == 0)
        #expect(stats.faultedConnections == 0)
        #expect(stats.connectionsByBroker.isEmpty)
    }
    
    @Test("Broker lookup result conversion")
    func testBrokerLookupResultConversion() throws {
        var command = Pulsar_Proto_BaseCommand()
        command.type = .lookupResponse
        command.lookupTopicResponse.response = .connect
        command.lookupTopicResponse.brokerServiceURL = "pulsar://broker-1:6650"
        command.lookupTopicResponse.brokerServiceURLTls = "pulsar+ssl://broker-1:6651"
        command.lookupTopicResponse.proxyThroughServiceURL = true
        
        let lookupResponse = try LookupResponse(from: command)
        let brokerResult = lookupResponse.toBrokerLookupResult()
        
        #expect(brokerResult.brokerUrl == "pulsar://broker-1:6650")
        #expect(brokerResult.brokerUrlTls == "pulsar+ssl://broker-1:6651")
        #expect(brokerResult.proxyThroughServiceUrl == true)
    }
    
    @Test("Message state management")
    func testMessageStateManagement() async throws {
        let eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: 1)
        defer {
            try? eventLoopGroup.syncShutdownGracefully()
        }
        
        let url = try PulsarURL(string: "pulsar://localhost:6650")
        let connection = Connection(
            url: url,
            eventLoopGroup: eventLoopGroup,
            logger: Logger(label: "test")
        )
        
        let channel = ConsumerChannel(
            id: 1,
            topic: "test-topic",
            subscription: "test-sub",
            consumerName: "test-consumer",
            connection: connection
        )
        
        // Activate the channel
        await channel.activate()
        
        // Test message handling
        var message = Pulsar_Proto_CommandMessage()
        message.consumerID = 1
        message.messageID.ledgerID = 100
        message.messageID.entryID = 200
        
        await channel.handleMessage(message)
        
        // Test active consumer change
        await channel.handleActiveConsumerChange(false)
        await channel.handleActiveConsumerChange(true)
    }
}

// Helper function to map server errors (copied from ConnectionPool+Enhanced.swift)
private func mapServerError(_ error: Pulsar_Proto_ServerError) -> Error {
    switch error {
    case .topicNotFound:
        return PulsarClientError.topicNotFound("Topic not found")
    case .subscriptionNotFound:
        return PulsarClientError.subscriptionNotFound("Subscription not found")
    case .authenticationError:
        return PulsarClientError.authenticationFailed("Authentication error")
    case .authorizationError:
        return PulsarClientError.authenticationFailed("Authorization error")
    case .consumerBusy:
        return PulsarClientError.consumerBusy("Consumer busy")
    case .tooManyRequests:
        return PulsarClientError.tooManyRequests
    case .checksumError:
        return PulsarClientError.checksumFailed
    default:
        return PulsarClientError.unknownError("Unknown server error: \(error)")
    }
}