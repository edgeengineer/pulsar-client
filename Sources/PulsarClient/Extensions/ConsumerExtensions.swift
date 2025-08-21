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
}