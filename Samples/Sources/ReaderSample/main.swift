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
struct ReaderSample {
    static func main() async {
        // Configure logging
        LoggingSystem.bootstrap { label in
            var handler = StreamLogHandler.standardOutput(label: label)
            handler.logLevel = .info
            return handler
        }
        
        let logger = Logger(label: "ReaderSample")
        
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
            
            // Create Reader starting from earliest message
            let reader = try await client.newReader(schema: StringSchema())
                .topic("persistent://public/default/mytopic")
                .readerName("swift-reader-sample")
                .startMessageId(.earliest)
                .create()
            
            defer {
                Task {
                    await reader.dispose()
                }
            }
            
            logger.info("Reader created successfully. Starting to read messages from earliest...")
            logger.info("Press Ctrl+C to exit")
            
            var messageCount = 0
            
            // Start state monitoring
            Task {
                for await state in reader.stateChanges {
                    logger.info("Reader state changed to: \(state)")
                }
            }
            
            // Main message reading loop
            while !shouldStop {
                do {
                    // Check if messages are available
                    let hasMessages = try await reader.hasMessageAvailable()
                    
                    if hasMessages {
                        // Read the next message
                        let message = try await reader.readNext()
                        
                        logger.info("✓ Read message: '\(message.value)' from topic: \(message.topicName)")
                        logger.info("  - MessageId: \(message.id)")
                        logger.info("  - PublishTime: \(message.publishTime)")
                        
                        if let eventTime = message.eventTime {
                            logger.info("  - EventTime: \(eventTime)")
                        }
                        
                        if let producerName = message.producerName {
                            logger.info("  - ProducerName: \(producerName)")
                        }
                        
                        if let key = message.key {
                            logger.info("  - Key: \(key)")
                        }
                        
                        if !message.properties.isEmpty {
                            logger.info("  - Properties: \(message.properties)")
                        }
                        
                        messageCount += 1
                    } else {
                        // No messages available, wait a bit
                        try await Task.sleep(for: .seconds(1))
                    }
                    
                } catch {
                    logger.error("✗ Failed to read message: \(error)")
                    // Wait a bit before retrying
                    try await Task.sleep(for: .seconds(2))
                }
            }
            
            logger.info("Reader sample completed. Total messages read: \(messageCount)")
            
        } catch {
            logger.error("Reader sample failed: \(error)")
            exit(1)
        }
    }
}

// Extensions for better logging
extension Logger {
    func readerStateChanged(_ change: ReaderStateChanged<String>) {
        self.info("Reader state changed to: \(change.readerState)")
    }
}