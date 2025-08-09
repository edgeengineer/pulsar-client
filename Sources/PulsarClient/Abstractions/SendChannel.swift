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

/// Protocol for non-blocking message sending with completion callbacks
public protocol SendChannelProtocol<T>: Sendable where T: Sendable {
  associatedtype T

  /// Try to send a message without blocking
  /// Returns true if the message was accepted, false if the channel is full
  func trySend(_ message: T, metadata: MessageMetadata?) async -> Bool

  /// Send a message asynchronously with a completion handler
  func sendAsync(
    _ message: T, metadata: MessageMetadata?,
    completion: @escaping @Sendable (Result<MessageId, Error>) -> Void) async

  /// Wait for all pending messages to be sent
  func waitForPendingSends() async throws

  /// Get the number of pending messages
  var pendingCount: Int { get async }

  /// Check if the channel can accept more messages
  var canSend: Bool { get async }

  /// Close the channel
  func close() async
}

/// Default implementation of SendChannel
public actor SendChannel<T>: SendChannelProtocol where T: Sendable {
  private let producer: any ProducerProtocol<T>
  private let maxPendingMessages: Int
  private var pendingMessages:
    [(
      message: T, metadata: MessageMetadata?,
      completion: @Sendable (Result<MessageId, Error>) -> Void
    )] = []
  private var isClosed = false

  public init(producer: any ProducerProtocol<T>, maxPendingMessages: Int = 1000) {
    self.producer = producer
    self.maxPendingMessages = maxPendingMessages

    // Start processing messages
    Task {
      await processMessages()
    }
  }

  public func trySend(_ message: T, metadata: MessageMetadata?) async -> Bool {
    guard !isClosed && pendingMessages.count < maxPendingMessages else {
      return false
    }

    let completion: @Sendable (Result<MessageId, Error>) -> Void = { _ in }
    pendingMessages.append((message, metadata, completion))
    return true
  }

  public func sendAsync(
    _ message: T, metadata: MessageMetadata?,
    completion: @escaping @Sendable (Result<MessageId, Error>) -> Void
  ) async {
    guard !isClosed else {
      completion(.failure(PulsarClientError.clientClosed))
      return
    }

    if pendingMessages.count >= maxPendingMessages {
      completion(.failure(PulsarClientError.producerBusy("Send channel is full")))
      return
    }

    pendingMessages.append((message, metadata, completion))
  }

  public func waitForPendingSends() async throws {
    while !pendingMessages.isEmpty {
      try await Task.sleep(nanoseconds: 10_000_000)  // 10ms
    }
  }

  public var pendingCount: Int {
    return pendingMessages.count
  }

  public var canSend: Bool {
    return !isClosed && pendingMessages.count < maxPendingMessages
  }

  public func close() async {
    isClosed = true

    // Cancel all pending messages
    for (_, _, completion) in pendingMessages {
      completion(.failure(PulsarClientError.clientClosed))
    }
    pendingMessages.removeAll()
  }

  private func processMessages() async {
    while !isClosed {
      if let item = pendingMessages.first {
        pendingMessages.removeFirst()

        do {
          let messageId = try await producer.send(
            item.message, metadata: item.metadata ?? MessageMetadata())
          item.completion(.success(messageId))
        } catch {
          item.completion(.failure(error))
        }
      } else {
        // No messages to process, wait a bit
        try? await Task.sleep(nanoseconds: 1_000_000)  // 1ms
      }
    }
  }
}
