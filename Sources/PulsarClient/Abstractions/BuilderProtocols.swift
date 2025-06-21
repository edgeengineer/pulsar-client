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

/// A protocol for building a Pulsar client.
public protocol PulsarClientBuilderProtocol: Sendable {
    func withServiceUrl(_ serviceUrl: String) -> Self
    func withAuthentication(_ authentication: Authentication) -> Self
    func withOperationTimeout(_ timeout: TimeInterval) -> Self
    func withConnectionTimeout(_ timeout: TimeInterval) -> Self
    func withMemoryLimit(_ limit: Int) -> Self
    func build() throws -> PulsarClientProtocol
}

/// A protocol for building a Pulsar producer.
public protocol ProducerBuilderProtocol: Sendable {
    associatedtype TMessage: Sendable
    
    func withTopic(_ topic: String) -> Self
    func withProducerName(_ name: String) -> Self
    func withInitialSequenceId(_ id: UInt64) -> Self
    func withSendTimeout(_ timeout: TimeInterval) -> Self
    func withBatching(_ enabled: Bool) -> Self
    func withCompressionType(_ type: CompressionType) -> Self
    func withMessageRouter(_ router: MessageRouter) -> Self
    func withHashingScheme(_ scheme: HashingScheme) -> Self
    func withBatchingMaxMessages(_ max: UInt) -> Self
    func withBatchingMaxDelay(_ delay: TimeInterval) -> Self
    func withBatchingMaxBytes(_ bytes: UInt) -> Self
    func withCryptoKeyReader(_ reader: CryptoKeyReader) -> Self
    func withEncryptionKeys(_ keys: [String]) -> Self
    func build() async throws -> ProducerProtocol
}

/// A protocol for building a Pulsar consumer.
public protocol ConsumerBuilderProtocol: Sendable {
    associatedtype TMessage: Sendable
    
    func withTopic(_ topic: String) -> Self
    func withConsumerName(_ name: String) -> Self
    func withSubscription(_ name: String) -> Self
    func withSubscriptionType(_ type: SubscriptionType) -> Self
    func withSubscriptionInitialPosition(_ position: SubscriptionInitialPosition) -> Self
    func withAckTimeout(_ timeout: TimeInterval) -> Self
    func withReceiverQueueSize(_ size: Int) -> Self
    func withReadCompacted(_ enabled: Bool) -> Self
    func withPriorityLevel(_ level: Int) -> Self
    func build() async throws -> ConsumerProtocol
}

/// A protocol for building a Pulsar reader.
public protocol ReaderBuilderProtocol: Sendable {
    associatedtype TMessage: Sendable

    func withTopic(_ topic: String) -> Self
    func withReaderName(_ name: String) -> Self
    func withStartMessageId(_ id: MessageId) -> Self
    func withStartMessageFromRollbackDuration(_ duration: TimeInterval) -> Self
    func withReceiverQueueSize(_ size: Int) -> Self
    func withReadCompacted(_ enabled: Bool) -> Self
    func build() async throws -> ReaderProtocol
}

/// A helper protocol for common builder properties.
public protocol BuilderProtocol: AnyObject {
    associatedtype Options
    var options: Options { get set }
    init(options: Options)
}

public extension BuilderProtocol where Options: Initializable {
    init() {
        self.init(options: .init())
    }
}

/// A protocol to ensure a type has a parameterless initializer.
public protocol Initializable {
    init()
}