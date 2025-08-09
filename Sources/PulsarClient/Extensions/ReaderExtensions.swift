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
  public func readAll() async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []

    while try await hasMessageAvailable() {
      let message = try await readNext()
      messages.append(message)
    }

    return messages
  }

  /// Read messages until a condition is met
  public func readUntil(_ predicate: (Message<MessageType>) -> Bool) async throws -> [Message<
    MessageType
  >] {
    var messages: [Message<MessageType>] = []

    while try await hasMessageAvailable() {
      let message = try await readNext()
      messages.append(message)

      if predicate(message) {
        break
      }
    }

    return messages
  }

  /// Read a specific number of messages
  public func read(count: Int) async throws -> [Message<MessageType>] {
    var messages: [Message<MessageType>] = []

    for _ in 0..<count {
      if try await hasMessageAvailable() {
        let message = try await readNext()
        messages.append(message)
      } else {
        break
      }
    }

    return messages
  }
}

// MARK: - State Monitoring Extensions

extension ReaderProtocol where Self: StateHolder, Self.T == ClientState {

  /// Wait for the reader to reach a specific state
  @discardableResult
  public func waitForState(_ targetState: ClientState, timeout: TimeInterval = 30.0) async throws
    -> ClientState
  {
    if state == targetState {
      return state
    }

    return try await stateChangedTo(targetState, timeout: timeout)
  }

  /// Wait for the reader to leave a specific state
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

// MARK: - AsyncSequence Support

extension ReaderProtocol {

  /// Returns an AsyncSequence of messages
  public var messages: AsyncThrowingStream<Message<MessageType>, Error> {
    AsyncThrowingStream { continuation in
      let task = Task {
        do {
          while !Task.isCancelled {
            let hasMessage = try await hasMessageAvailable()
            if !hasMessage {
              break
            }
            let message = try await readNext()
            continuation.yield(message)
          }
          continuation.finish()
        } catch {
          continuation.finish(throwing: error)
        }
      }

      continuation.onTermination = { _ in
        task.cancel()
      }
    }
  }
}

// MARK: - Seek Extensions

extension ReaderProtocol {

  /// Seek to the earliest available message
  public func seekToEarliest() async throws {
    try await seek(to: .earliest)
  }

  /// Seek to the latest message
  public func seekToLatest() async throws {
    try await seek(to: .latest)
  }

  /// Seek to a message published after the given date
  public func seek(after date: Date) async throws {
    try await seek(to: date)
  }
}

// MARK: - Filtering Extensions

extension ReaderProtocol {

  /// Read messages that match a predicate
  public func readWhere(_ predicate: (Message<MessageType>) -> Bool) async throws -> Message<
    MessageType
  > {
    while try await hasMessageAvailable() {
      let message = try await readNext()
      if predicate(message) {
        return message
      }
    }

    throw PulsarClientError.readerClosed
  }

  /// Read messages with a specific key
  public func read(withKey key: String) async throws -> Message<MessageType> {
    return try await readWhere { $0.key == key }
  }
}
