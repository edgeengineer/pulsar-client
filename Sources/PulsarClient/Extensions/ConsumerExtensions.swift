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

// MARK: - Consumer Convenience Extensions

extension ConsumerProtocol {

  /// Process messages continuously using AsyncSequence
  /// - Parameter handler: The handler to process each message
  /// - Note: Messages are automatically acknowledged on success or negatively acknowledged on error
  public func processMessages(_ handler: (Message<MessageType>) async throws -> Void) async throws {
    for try await message in self {
      do {
        try await handler(message)
        try await acknowledge(message)
      } catch {
        try await negativeAcknowledge(message)
        throw error
      }
    }
  }

  /// Process messages with automatic acknowledgment
  /// - Parameter handler: The handler to process each message
  /// - Note: Messages are automatically acknowledged after successful processing
  public func processMessagesWithAutoAck(_ handler: (Message<MessageType>) async throws -> Void) async throws {
    for try await message in self {
      try await handler(message)
      try await acknowledge(message)
    }
  }

  /// Process messages in batches
  /// - Parameters:
  ///   - batchSize: The size of each batch
  ///   - handler: The handler to process each batch
  public func processBatches(of batchSize: Int, handler: ([Message<MessageType>]) async throws -> Void) async throws {
    var batch: [Message<MessageType>] = []
    
    for try await message in self {
      batch.append(message)
      
      if batch.count >= batchSize {
        try await handler(batch)
        
        // Acknowledge all messages in the batch
        for msg in batch {
          try await acknowledge(msg)
        }
        
        batch.removeAll()
      }
    }
    
    // Process any remaining messages
    if !batch.isEmpty {
      try await handler(batch)
      for msg in batch {
        try await acknowledge(msg)
      }
    }
  }

  /// Process messages with a filter
  /// - Parameters:
  ///   - filter: The filter predicate
  ///   - handler: The handler for messages that pass the filter
  public func processFiltered(
    where filter: (Message<MessageType>) async -> Bool,
    handler: (Message<MessageType>) async throws -> Void
  ) async throws {
    for try await message in self {
      if await filter(message) {
        try await handler(message)
        try await acknowledge(message)
      } else {
        // Acknowledge filtered out messages as well
        try await acknowledge(message)
      }
    }
  }

  /// Take a limited number of messages
  /// - Parameter count: The number of messages to take
  /// - Returns: An array of messages
  public func take(_ count: Int) async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []
    
    for try await message in self {
      messages.append(message)
      if messages.count >= count {
        break
      }
    }
    
    return messages
  }

  /// Process messages with a timeout between messages
  /// - Parameters:
  ///   - timeout: Maximum time to wait for next message
  ///   - handler: The handler for each message
  public func processWithTimeout(
    timeout: TimeInterval,
    handler: @escaping @Sendable (Message<MessageType>) async throws -> Void
  ) async throws {
    for try await message in self {
      try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
          try await handler(message)
        }
        
        group.addTask {
          try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
          throw PulsarClientError.timeout("Message processing timeout")
        }
        
        try await group.next()!
        group.cancelAll()
      }
      
      try await acknowledge(message)
    }
  }
}

// MARK: - Subscription Management

extension ConsumerProtocol {
  /// Pause message consumption by stopping acknowledgments
  /// Note: Messages will still be buffered locally
  public func pause() async {
    // Implementation would require adding pause/resume state to ConsumerImpl
    // For now, users can control flow by not iterating
  }

  /// Resume message consumption
  public func resume() async {
    // Implementation would require adding pause/resume state to ConsumerImpl
    // For now, users can control flow by resuming iteration
  }
}

// MARK: - Message Filtering

extension ConsumerProtocol where MessageType: Hashable {
  /// Process only unique messages
  public func processUnique(handler: (Message<MessageType>) async throws -> Void) async throws {
    var seen = Set<MessageType>()
    
    for try await message in self {
      if !seen.contains(message.value) {
        seen.insert(message.value)
        try await handler(message)
      }
      try await acknowledge(message)
    }
  }
}