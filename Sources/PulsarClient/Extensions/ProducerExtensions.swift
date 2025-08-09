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

// MARK: - Producer Convenience Extensions

extension ProducerProtocol {

  /// Send a message with a key
  public func send(_ message: MessageType, key: String) async throws -> MessageId {
    let metadata = MessageMetadata().withKey(key)
    return try await send(message, metadata: metadata)
  }

  /// Send a message with properties
  public func send(_ message: MessageType, properties: [String: String]) async throws -> MessageId {
    var metadata = MessageMetadata()
    metadata.properties = properties
    return try await send(message, metadata: metadata)
  }

  /// Send a message with event time
  public func send(_ message: MessageType, eventTime: Date) async throws -> MessageId {
    let metadata = MessageMetadata().withEventTime(eventTime)
    return try await send(message, metadata: metadata)
  }

  /// Send a message with key and properties
  public func send(_ message: MessageType, key: String, properties: [String: String]) async throws
    -> MessageId
  {
    var metadata = MessageMetadata()
      .withKey(key)
    metadata.properties = properties
    return try await send(message, metadata: metadata)
  }
}

// MARK: - State Monitoring Extensions

extension ProducerProtocol where Self: StateHolder, Self.T == ClientState {

  /// Wait for the producer to reach a specific state
  @discardableResult
  public func waitForState(_ targetState: ClientState, timeout: TimeInterval = 30.0) async throws
    -> ClientState
  {
    if state == targetState {
      return state
    }

    return try await stateChangedTo(targetState, timeout: timeout)
  }

  /// Wait for the producer to leave a specific state
  @discardableResult
  public func waitToLeaveState(_ currentState: ClientState, timeout: TimeInterval = 30.0)
    async throws -> ClientState
  {
    if state != currentState {
      return state
    }

    return try await stateChangedFrom(currentState, timeout: timeout)
  }
}

// MARK: - Batch Sending Extensions

extension ProducerProtocol {

  /// Send multiple messages with the same metadata
  public func sendBatch(_ messages: [MessageType], metadata: MessageMetadata) async throws
    -> [MessageId]
  {
    var messageIds: [MessageId] = []

    for message in messages {
      let id = try await send(message, metadata: metadata)
      messageIds.append(id)
    }

    return messageIds
  }

  /// Send messages with individual metadata
  public func sendBatch(_ messagesWithMetadata: [(message: MessageType, metadata: MessageMetadata)])
    async throws -> [MessageId]
  {
    var messageIds: [MessageId] = []

    for (message, metadata) in messagesWithMetadata {
      let id = try await send(message, metadata: metadata)
      messageIds.append(id)
    }

    return messageIds
  }
}

// MARK: - Data Type Extensions

extension ProducerProtocol where MessageType == Data {

  /// Send raw bytes
  public func send(bytes: [UInt8]) async throws -> MessageId {
    return try await send(Data(bytes))
  }

  /// Send raw bytes with metadata
  public func send(bytes: [UInt8], metadata: MessageMetadata) async throws -> MessageId {
    return try await send(Data(bytes), metadata: metadata)
  }
}

extension ProducerProtocol where MessageType == String {

  /// Send a string with UTF-8 encoding
  public func send(utf8String: String) async throws -> MessageId {
    return try await send(utf8String)
  }
}
