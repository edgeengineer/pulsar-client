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
struct BatchProcessingSample {
    static func main() async {
        // Configure logging
        LoggingSystem.bootstrap { label in
            var handler = StreamLogHandler.standardOutput(label: label)
            handler.logLevel = .info
            return handler
        }
        
        let logger = Logger(label: "BatchProcessingSample")
        
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
            
            // Start both producer and consumer concurrently
            await withTaskGroup(of: Void.self) { group in
                // Producer task
                group.addTask {
                    await runBatchProducer(client: client, logger: logger, shouldStop: &shouldStop)
                }
                
                // Consumer task  
                group.addTask {
                    await runBatchConsumer(client: client, logger: logger, shouldStop: &shouldStop)
                }
                
                // Wait for first task to complete
                await group.next()
                group.cancelAll()
            }
            
        } catch {
            logger.error("Batch processing sample failed: \(error)")
            exit(1)
        }
    }
}

func runBatchProducer(client: PulsarClient, logger: Logger, shouldStop: inout Bool) async {
    do {
        // Create Producer with aggressive batching settings
        let producer = try await client.newProducer(schema: StringSchema())
            .topic("persistent://public/default/batch-topic")
            .producerName("swift-batch-producer")
            .compression(.zlib)
            .batching(enabled: true)
            .create()
        
        defer {
            Task {
                await producer.dispose()
            }
        }
        
        logger.info("🚀 Batch producer started. Sending messages in batches...")
        
        var batchNumber = 0
        
        while !shouldStop {
            batchNumber += 1
            let batchSize = 50
            var messages: [String] = []
            
            // Prepare batch of messages
            for i in 1...batchSize {
                let timestamp = Date().formatted(.iso8601)
                let message = "Batch #\(batchNumber) - Message #\(i) at \(timestamp)"
                messages.append(message)
            }
            
            // Send batch
            let startTime = Date()
            let messageIds = try await producer.sendBatch(messages)
            let duration = Date().timeIntervalSince(startTime)
            
            logger.info("📦 Sent batch #\(batchNumber): \(batchSize) messages in \(String(format: "%.3f", duration))s")
            logger.info("   First MessageId: \(messageIds.first!)")
            logger.info("   Last MessageId: \(messageIds.last!)")
            
            // Wait before sending next batch
            try await Task.sleep(for: .seconds(3))
        }
        
        // Flush any remaining messages
        try await producer.flush()
        logger.info("✅ Batch producer completed")
        
    } catch {
        logger.error("❌ Batch producer failed: \(error)")
    }
}

func runBatchConsumer(client: PulsarClient, logger: Logger, shouldStop: inout Bool) async {
    do {
        // Wait a moment for producer to start
        try await Task.sleep(for: .seconds(1))
        
        // Create Consumer optimized for batch processing
        let consumer = try await client.newConsumer(schema: StringSchema())
            .topic("persistent://public/default/batch-topic")
            .consumerName("swift-batch-consumer")
            .subscription("BatchProcessingSubscription")
            .subscriptionType(.shared)
            .subscriptionInitialPosition(.latest)
            .receiverQueueSize(5000)  // Large queue for batching
            .create()
        
        defer {
            Task {
                await consumer.dispose()
            }
        }
        
        logger.info("📥 Batch consumer started. Processing messages in batches...")
        
        var totalMessages = 0
        var processingBatch: [Message<String>] = []
        let batchProcessingSize = 20
        
        while !shouldStop {
            do {
                // Try to receive messages and accumulate them
                let message = try await withTimeout(seconds: 2.0) {
                    try await consumer.receive()
                }
                
                processingBatch.append(message)
                
                // Process batch when it reaches target size or timeout
                if processingBatch.count >= batchProcessingSize {
                    await processBatch(processingBatch, consumer: consumer, logger: logger)
                    totalMessages += processingBatch.count
                    processingBatch.removeAll()
                }
                
            } catch is TimeoutError {
                // Timeout - process any accumulated messages
                if !processingBatch.isEmpty {
                    await processBatch(processingBatch, consumer: consumer, logger: logger)
                    totalMessages += processingBatch.count
                    processingBatch.removeAll()
                }
            } catch {
                logger.error("❌ Failed to receive message: \(error)")
                try await Task.sleep(for: .seconds(1))
            }
        }
        
        // Process any remaining messages
        if !processingBatch.isEmpty {
            await processBatch(processingBatch, consumer: consumer, logger: logger)
            totalMessages += processingBatch.count
        }
        
        logger.info("✅ Batch consumer completed. Total messages processed: \(totalMessages)")
        
    } catch {
        logger.error("❌ Batch consumer failed: \(error)")
    }
}

func processBatch(_ batch: [Message<String>], consumer: ConsumerProtocol<String>, logger: Logger) async {
    let startTime = Date()
    
    // Simulate batch processing work
    var processedCount = 0
    for message in batch {
        // Simulate processing time
        try? await Task.sleep(for: .milliseconds(10))
        processedCount += 1
    }
    
    // Acknowledge all messages in batch
    do {
        try await consumer.acknowledgeBatch(batch)
        let duration = Date().timeIntervalSince(startTime)
        logger.info("✅ Processed batch: \(batch.count) messages in \(String(format: "%.3f", duration))s")
    } catch {
        logger.error("❌ Failed to acknowledge batch: \(error)")
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