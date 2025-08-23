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

// MARK: - Reader Convenience Extensions

extension ReaderProtocol {
  
  /// Read all available messages
  /// - Returns: An array of all available messages
  public func readAll() async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []
    
    for try await message in self {
      messages.append(message)
      
      // Check if more messages are available
      if try await !hasMessageAvailable() {
        break
      }
    }
    
    return messages
  }
  
  /// Read messages until a condition is met
  /// - Parameter condition: The condition to stop reading
  /// - Returns: An array of messages read before the condition was met
  public func readUntil(_ condition: (Message<MessageType>) -> Bool) async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []
    
    for try await message in self {
      if condition(message) {
        break
      }
      messages.append(message)
    }
    
    return messages
  }
  
  /// Read messages for a specific duration
  /// - Parameter duration: The duration to read messages for
  /// - Returns: An array of messages read during the duration
  public func readFor(duration: TimeInterval) async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []
    let endTime = Date().addingTimeInterval(duration)
    
    for try await message in self {
      messages.append(message)
      
      if Date() >= endTime {
        break
      }
    }
    
    return messages
  }
  
  /// Process messages with a handler
  /// - Parameter handler: The handler to process each message
  public func process(_ handler: (Message<MessageType>) async throws -> Void) async throws {
    for try await message in self {
      try await handler(message)
    }
  }
  
  /// Read messages with filtering
  /// - Parameter filter: The filter predicate
  /// - Returns: An array of messages that pass the filter
  public func readFiltered(where filter: (Message<MessageType>) -> Bool) async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []
    
    for try await message in self {
      if filter(message) {
        messages.append(message)
      }
    }
    
    return messages
  }
}

// MARK: - Position Management

extension ReaderProtocol {
  /// Reset to the beginning of the topic
  public func seekToBeginning() async throws {
    try await seek(to: MessageId.earliest)
  }
  
  /// Skip to the end of the topic
  public func seekToEnd() async throws {
    try await seek(to: MessageId.latest)
  }
  
  /// Seek to a specific time and read messages from there
  /// - Parameters:
  ///   - timestamp: The timestamp to seek to
  ///   - handler: The handler for messages after the timestamp
  public func readFrom(timestamp: Date, handler: (Message<MessageType>) async throws -> Void) async throws {
    try await seek(to: timestamp)
    
    for try await message in self {
      try await handler(message)
    }
  }
}

// MARK: - Batch Reading

extension ReaderProtocol {
  /// Read messages in batches
  /// - Parameters:
  ///   - batchSize: The size of each batch
  ///   - handler: The handler for each batch
  public func readBatches(of batchSize: Int, handler: ([Message<MessageType>]) async throws -> Void) async throws {
    var batch: [Message<MessageType>] = []
    
    for try await message in self {
      batch.append(message)
      
      if batch.count >= batchSize {
        try await handler(batch)
        batch.removeAll()
      }
    }
    
    // Process any remaining messages
    if !batch.isEmpty {
      try await handler(batch)
    }
  }
}