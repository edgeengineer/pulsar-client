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
import PulsarClient
import Logging

@main
struct ConsumerSample {
    static func main() async {
        // Configure logging
        LoggingSystem.bootstrap { label in
            var handler = StreamLogHandler.standardOutput(label: label)
            handler.logLevel = .info
            return handler
        }
        
        let logger = Logger(label: "ConsumerSample")
        
        // Setup signal handling for graceful shutdown
        let signalSource = DispatchSource.makeSignalSource(signal: SIGINT, queue: .main)
        var shouldStop = false
        
        signalSource.setEventHandler {
            logger.info("Received SIGINT, shutting down...")
            shouldStop = true
            signalSource.cancel()
        }
        
        signal(SIGINT, SIG_IGN)
        signalSource.resume()
        
        do {
            // Create PulsarClient
            let client = try await PulsarClient.builder()
                .serviceUrl("pulsar://localhost:6650")
                .authentication(AuthenticationFactory.none())
                .exceptionHandler { context in
                    logger.error("PulsarClient exception: \(context.exception)")
                }
                .build()
            
            defer {
                Task {
                    await client.dispose()
                }
            }
            
            // Create Consumer
            let consumer = try await client.newConsumer(schema: StringSchema())
                .topic("persistent://public/default/mytopic")
                .consumerName("swift-consumer-sample")
                .subscription("MySwiftSubscription")
                .subscriptionType(.shared)
                .subscriptionInitialPosition(.latest)
                .create()
            
            defer {
                Task {
                    await consumer.dispose()
                }
            }
            
            logger.info("Consumer created successfully. Starting to receive messages...")
            logger.info("Press Ctrl+C to exit")
            
            var messageCount = 0
            
            // Start state monitoring
            Task {
                for await state in consumer.stateChanges {
                    logger.info("Consumer state changed to: \(state)")
                }
            }
            
            // Main message processing loop
            while !shouldStop {
                do {
                    // Try to receive a message with timeout
                    let message = try await withTimeout(seconds: 1.0) {
                        try await consumer.receive()
                    }
                    
                    logger.info("✓ Received message: '\(message.value)' from topic: \(message.topicName)")
                    logger.info("  - MessageId: \(message.id)")
                    logger.info("  - PublishTime: \(message.publishTime)")
                    
                    if let producerName = message.producerName {
                        logger.info("  - ProducerName: \(producerName)")
                    }
                    
                    if !message.properties.isEmpty {
                        logger.info("  - Properties: \(message.properties)")
                    }
                    
                    // Acknowledge the message
                    try await consumer.acknowledge(message)
                    messageCount += 1
                    
                } catch is TimeoutError {
                    // Timeout is expected, just continue
                    continue
                } catch {
                    logger.error("✗ Failed to receive message: \(error)")
                    // Wait a bit before retrying
                    try await Task.sleep(for: .seconds(1))
                }
            }
            
            logger.info("Consumer sample completed. Total messages received: \(messageCount)")
            
        } catch {
            logger.error("Consumer sample failed: \(error)")
            exit(1)
        }
    }
}

// Timeout utility
private func withTimeout<T>(seconds: TimeInterval, operation: @escaping () async throws -> T) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask {
            try await operation()
        }
        
        group.addTask {
            try await Task.sleep(for: .seconds(seconds))
            throw TimeoutError()
        }
        
        guard let result = try await group.next() else {
            throw TimeoutError()
        }
        
        group.cancelAll()
        return result
    }
}

private struct TimeoutError: Error {}

// Extensions for better logging
extension Logger {
    func consumerStateChanged(_ change: ConsumerStateChanged<String>) {
        self.info("Consumer state changed to: \(change.consumerState)")
    }
}