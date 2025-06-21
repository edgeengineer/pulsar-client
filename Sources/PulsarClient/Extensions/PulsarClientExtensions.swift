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

// MARK: - PulsarClient Convenience Extensions

public extension PulsarClientProtocol {
    
    /// Create a producer with default byte array schema
    func newProducer(topic: String) async throws -> any ProducerProtocol<Data> {
        return try await newProducer(
            topic: topic,
            schema: Schema<Data>.bytes
        ) { _ in }
    }
    
    /// Create a consumer with default byte array schema
    func newConsumer(topic: String, subscription: String) async throws -> any ConsumerProtocol<Data> {
        return try await newConsumer(
            topic: topic,
            schema: Schema<Data>.bytes
        ) { builder in
            _ = builder.subscriptionName(subscription)
        }
    }
    
    /// Create a reader with default byte array schema
    func newReader(topic: String, startMessageId: MessageId = .earliest) async throws -> any ReaderProtocol<Data> {
        return try await newReader(
            topic: topic,
            schema: Schema<Data>.bytes
        ) { builder in
            // startMessageId is set during builder creation, not here
        }
    }
    
    /// Create a string producer
    func newStringProducer(topic: String) async throws -> any ProducerProtocol<String> {
        return try await newProducer(
            topic: topic,
            schema: Schema<String>.string
        ) { _ in }
    }
    
    /// Create a string consumer
    func newStringConsumer(topic: String, subscription: String) async throws -> any ConsumerProtocol<String> {
        return try await newConsumer(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            _ = builder.subscriptionName(subscription)
        }
    }
    
    /// Create a string reader
    func newStringReader(topic: String, startMessageId: MessageId = .earliest) async throws -> any ReaderProtocol<String> {
        return try await newReader(
            topic: topic,
            schema: Schema<String>.string
        ) { builder in
            // startMessageId is set during builder creation, not here
        }
    }
}

// MARK: - Multiple Topic Support

public extension PulsarClientProtocol {
    
    /// Create a consumer for multiple topics
    func newConsumer<T>(
        topics: [String],
        subscriptionName: String,
        schema: Schema<T>,
        configure: ((ConsumerBuilder<T>) -> Void)? = nil
    ) async throws -> any ConsumerProtocol<T> where T: Sendable {
        guard !topics.isEmpty else {
            throw PulsarClientError.invalidTopicName("No topics provided")
        }
        
        // Use the first topic for the main consumer creation
        // The PulsarClient implementation expects a single topic in the newConsumer method
        guard let firstTopic = topics.first else {
            throw PulsarClientError.invalidTopicName("No topics provided")
        }
        
        return try await newConsumer(
            topic: firstTopic,
            schema: schema
        ) { builder in
            // Create options with multiple topics
            let options = ConsumerOptions(subscriptionName: subscriptionName, topics: topics, schema: schema)
            builder.options = options
            
            // Apply additional configuration if provided
            configure?(builder)
        }
    }
    
    /// Create a consumer for topics matching a pattern
    func newConsumer<T>(
        topicsPattern: String,
        subscriptionName: String,
        schema: Schema<T>,
        configure: ((ConsumerBuilder<T>) -> Void)? = nil
    ) async throws -> any ConsumerProtocol<T> where T: Sendable {
        // For pattern subscription, we use the pattern as the topic
        return try await newConsumer(
            topic: topicsPattern,
            schema: schema
        ) { builder in
            // Create options with pattern
            let options = ConsumerOptions(subscriptionName: subscriptionName, topicsPattern: topicsPattern, schema: schema)
            builder.options = options
            
            // Apply additional configuration if provided
            configure?(builder)
        }
    }
}

// MARK: - Quick Operations

public extension PulsarClientProtocol {
    
    /// Send a single message to a topic
    func send<T>(
        _ message: T,
        to topic: String,
        schema: Schema<T>
    ) async throws -> MessageId where T: Sendable {
        let producer = try await newProducer(topic: topic, schema: schema) { _ in }
        defer { Task { await producer.dispose() } }
        
        return try await producer.send(message)
    }
    
    /// Send a string message to a topic
    func send(
        _ message: String,
        to topic: String
    ) async throws -> MessageId {
        return try await send(message, to: topic, schema: Schema<String>.string)
    }
    
    /// Send data to a topic
    func send(
        _ data: Data,
        to topic: String
    ) async throws -> MessageId {
        return try await send(data, to: topic, schema: Schema<Data>.bytes)
    }
}