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

/// An abstraction for a Pulsar message.
public protocol MessageProtocol: Sendable {
  /// The unique identifier for this message.
  var messageId: MessageId { get }

  /// The raw payload of the message.
  var data: Data { get }

  /// The name of the topic this message was published to.
  var topic: String { get }

  /// The properties attached to the message.
  var properties: [String: String] { get }

  /// The event time associated with the message, if any.
  /// This is a user-defined timestamp.
  var eventTime: UInt64 { get }

  /// The timestamp of when the message was published.
  /// This is set by the producer.
  var publishTime: UInt64 { get }

  /// The timestamp of when the message was received by the broker.
  var brokerPublishTime: UInt64 { get }

  /// The timestamp of when the message was delivered to the consumer.
  var deliveredTime: UInt64 { get }

  /// The sequence ID of the message.
  var sequenceId: UInt64 { get }

  /// The name of the producer who sent the message.
  var producerName: String? { get }

  /// The number of times this message has been redelivered.
  var redeliveryCount: UInt32 { get }

  /// The key of the message, if any.
  var key: String? { get }

  /// The raw bytes of the message key, if any.
  var keyBytes: Data { get }

  /// Acknowledges the successful processing of the message.
  func acknowledge() async throws

  /// Acknowledges the successful processing of all messages up to this one.
  func acknowledgeCumulative() async throws

  /// Signals that the message was not processed successfully.
  func nack() async throws

  /// Decodes the message payload using a specific schema.
  func value<T>(using schema: Schema<T>) async throws -> T
}

/// An abstraction for a typed Pulsar message.
public protocol TypedMessageProtocol<TMessage>: MessageProtocol {
  /// The type of the message payload.
  associatedtype TMessage: Sendable

  /// The deserialized value of the message payload.
  /// This method will decode the raw `data` using the consumer's schema.
  func value() async throws -> TMessage
}
