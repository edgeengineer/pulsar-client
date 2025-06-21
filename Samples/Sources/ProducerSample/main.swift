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
struct ProducerSample {
    static func main() async {
        // Configure logging
        LoggingSystem.bootstrap { label in
            var handler = StreamLogHandler.standardOutput(label: label)
            handler.logLevel = .info
            return handler
        }
        
        let logger = Logger(label: "ProducerSample")
        
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
            
            // Create Producer
            let producer = try await client.newProducer(schema: StringSchema())
                .topic("persistent://public/default/mytopic")
                .producerName("swift-producer-sample")
                .compression(.zlib)
                .batching(enabled: true)
                .create()
            
            defer {
                Task {
                    await producer.dispose()
                }
            }
            
            logger.info("Producer created successfully. Starting to send messages every 5 seconds...")
            logger.info("Press Ctrl+C to exit")
            
            var messageCount = 0
            
            while !shouldStop {
                let timestamp = Date().formatted(.iso8601)
                let message = "Hello from Swift PulsarClient at \(timestamp) - Message #\(messageCount)"
                
                do {
                    let messageId = try await producer.send(message)
                    logger.info("✓ Sent message: '\(message)' -> MessageId: \(messageId)")
                    messageCount += 1
                } catch {
                    logger.error("✗ Failed to send message: \(error)")
                }
                
                // Wait 5 seconds before sending next message
                try await Task.sleep(for: .seconds(5))
            }
            
            // Flush any remaining messages
            try await producer.flush()
            logger.info("Producer sample completed. Total messages sent: \(messageCount)")
            
        } catch {
            logger.error("Producer sample failed: \(error)")
            exit(1)
        }
    }
}

// Extensions for better logging
extension Logger {
    func producerStateChanged(_ change: ProducerStateChanged<String>) {
        self.info("Producer state changed to: \(change.producerState)")
    }
}