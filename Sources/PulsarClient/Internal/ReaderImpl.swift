import Foundation
import Logging

/// Reader implementation wrapping a consumer
actor ReaderImpl<T>: ReaderProtocol, AsyncSequence where T: Sendable {
  typealias MessageType = T
  public typealias Element = Message<T>
  private let consumer: any ConsumerProtocol<T>
  private let startMessageId: MessageId
  private let logger: Logger
  private let options: ReaderOptions<T>

  private var _state: ClientState = .connected
  internal let stateStream: AsyncStream<ClientState>
  private let stateContinuation: AsyncStream<ClientState>.Continuation

  public let topic: String
  public nonisolated var state: ClientState {
    ClientState.connected  // Simple fallback for nonisolated access
  }
  public nonisolated var stateChanges: AsyncStream<ClientState> { stateStream }

  init(
    consumer: any ConsumerProtocol<T>,
    topic: String,
    startMessageId: MessageId,
    logger: Logger,
    options: ReaderOptions<T>
  ) {
    self.consumer = consumer
    self.topic = topic
    self.startMessageId = startMessageId
    self.logger = logger
    self.options = options

    (self.stateStream, self.stateContinuation) = AsyncStream<ClientState>.makeStream()

    // Monitor consumer state
    Task { [weak self] in
      await self?.monitorConsumerState()
    }
  }

  deinit {
    stateContinuation.finish()
  }

  // MARK: - StateHolder

  public nonisolated func onStateChange(_ handler: @escaping @Sendable (ClientState) -> Void) {
    Task { [weak self] in
      guard let self = self else { return }
      for await state in self.stateStream {
        handler(state)
      }
    }
  }

  public nonisolated func isFinal() -> Bool {
    false  // Conservative approach for nonisolated access
  }

  public nonisolated func handleException(_ error: any Error) {
    Task { [weak self] in
      await self?.processException(error)
    }
  }

  public func stateChangedTo(_ state: ClientState, timeout: TimeInterval) async throws
    -> ClientState
  {
    if _state == state {
      return _state
    }

    return try await withThrowingTaskGroup(of: ClientState.self) { group in
      // Add timeout task
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        throw PulsarClientError.timeout("Timeout waiting for state change to \(state)")
      }

      // Add state monitoring task
      group.addTask { [weak self] in
        guard let self = self else { throw PulsarClientError.clientClosed }

        for await currentState in self.stateStream {
          if currentState == state {
            return currentState
          }
        }
        throw PulsarClientError.clientClosed
      }

      let result = try await group.next()!
      group.cancelAll()
      return result
    }
  }

  public func stateChangedFrom(_ state: ClientState, timeout: TimeInterval) async throws
    -> ClientState
  {
    if _state != state {
      return _state
    }

    return try await withThrowingTaskGroup(of: ClientState.self) { group in
      // Add timeout task
      group.addTask {
        try await Task.sleep(nanoseconds: UInt64(timeout * 1_000_000_000))
        throw PulsarClientError.timeout("Timeout waiting for state change from \(state)")
      }

      // Add state monitoring task
      group.addTask { [weak self] in
        guard let self = self else { throw PulsarClientError.clientClosed }

        for await currentState in self.stateStream {
          if currentState != state {
            return currentState
          }
        }
        throw PulsarClientError.clientClosed
      }

      let result = try await group.next()!
      group.cancelAll()
      return result
    }
  }

  // MARK: - ReaderProtocol

  public nonisolated var isConnected: Bool {
    true  // Conservative approach for nonisolated access
  }

  // MARK: - AsyncSequence Conformance
  
  public struct AsyncIterator: AsyncIteratorProtocol {
    private let reader: ReaderImpl<T>
    
    init(reader: ReaderImpl<T>) {
      self.reader = reader
    }
    
    public mutating func next() async throws -> Message<T>? {
      guard await reader._state == .connected else {
        return nil
      }
      
      // Since we can't store a mutable iterator in a struct that needs to be mutating,
      // we'll create a new iterator each time. This is not ideal but works.
      // A better approach would be to make the reader itself handle the iteration.
      return await reader.getNextMessage()
    }
  }
  
  public nonisolated func makeAsyncIterator() -> AsyncIterator {
    return AsyncIterator(reader: self)
  }
  
  /// Helper method to get the next message from the consumer
  private func getNextMessage() async -> Message<T>? {
    // Since consumer is ConsumerImpl which conforms to AsyncSequence,
    // we need to properly type it to get the messages
    guard let consumerImpl = consumer as? ConsumerImpl<T> else {
      return nil
    }
    
    do {
      // Create an iterator from the consumer
      var iterator = consumerImpl.makeAsyncIterator()
      
      // Get the next message
      if let message = try await iterator.next() {
        // Automatically acknowledge the message
        try? await consumer.acknowledge(message)
        return message
      }
    } catch {
      // If there's an error, return nil
      logger.debug("Error getting next message", metadata: ["error": "\(error)"])
    }
    return nil
  }

  public func hasMessageAvailable() async throws -> Bool {
    guard _state == ClientState.connected else {
      throw PulsarClientError.consumerBusy("Reader not connected")
    }

    // First check if we have messages in the local buffer
    if await hasBufferedMessages() {
      return true
    }

    // If no buffered messages, check broker-side availability
    return try await hasBrokerSideMessages()
  }

  private func hasBufferedMessages() async -> Bool {
    // Check if the consumer has any messages in its local buffer
    let bufferedCount = await consumer.getBufferedMessageCount()
    return bufferedCount > 0
  }

  private func hasBrokerSideMessages() async throws -> Bool {
    do {
      // Get the last available message ID from the broker
      let lastMessageResponse = try await consumer.getLastMessageId()
      let lastAvailableMessageId = lastMessageResponse.messageId

      // Get our current read position
      let currentPosition = await consumer.getCurrentPosition()

      // If we haven't read any messages yet, there are definitely messages available
      // if the last message ID is not the "earliest" marker
      guard let currentPosition = currentPosition else {
        // No messages read yet - check if there are any messages at all
        return lastAvailableMessageId != .earliest
      }

      // Compare positions: if last available > current position, messages are available
      let hasMoreMessages = lastAvailableMessageId > currentPosition

      logger.debug(
        "Message availability check", 
        metadata: [
          "lastAvailableMessageId": "\(lastAvailableMessageId)",
          "currentPosition": "\(currentPosition)",
          "hasMoreMessages": "\(hasMoreMessages)"
        ]
      )

      return hasMoreMessages

    } catch {
      logger.debug("Failed to check broker-side message availability", metadata: ["error": "\(error)"])
      // Fall back to conservative approach - assume no messages available
      return false
    }
  }

  public func seek(to messageId: MessageId) async throws {
    guard _state == ClientState.connected else {
      throw PulsarClientError.consumerBusy("Reader not connected")
    }

    try await consumer.seek(to: messageId)
    logger.debug("Reader seeked to message", metadata: ["messageId": "\(messageId)"])
  }

  public func seek(to timestamp: Date) async throws {
    guard _state == ClientState.connected else {
      throw PulsarClientError.consumerBusy("Reader not connected")
    }

    try await consumer.seek(to: timestamp)
    logger.debug("Reader seeked to timestamp", metadata: ["timestamp": "\(timestamp)"])
  }

  public func dispose() async {
    updateState(.closing)

    // Unsubscribe and dispose consumer
    try? await consumer.unsubscribe()
    await consumer.dispose()

    updateState(.closed)
    logger.debug("Reader closed for topic", metadata: ["topic": "\(topic)"])
  }

  // MARK: - Private Methods

  private func updateState(_ newState: ClientState) {
    let previousState = _state
    _state = newState
    stateContinuation.yield(newState)

    // Notify state change handler if configured
    if let handler = options.stateChangedHandler {
      let readerStateChange = ReaderStateChanged(
        reader: self,
        state: newState,
        previousState: previousState,
        timestamp: Date()
      )
      Task {
        handler(readerStateChange)
      }
    }
  }

  private func processException(_ error: any Error) async {
    logger.error("Reader exception", metadata: ["error": "\(error)"])

    // Handle different types of errors
    switch error {
    case let pulsarError as PulsarClientError:
      switch pulsarError {
      case .connectionFailed, .protocolError:
        // Connection issues - update state to reconnecting
        updateState(.reconnecting)
      // The underlying consumer will handle reconnection
      case .timeout:
        // Timeout errors are usually transient
        break
      default:
        updateState(.faulted(error))
      }
    default:
      updateState(.faulted(error))
    }

    // Call user-defined exception handler if provided
    // Note: ReaderOptions doesn't have exceptionHandler yet
    // if let handler = options.exceptionHandler {
    //     await handler.onException(ExceptionContext(
    //         exception: error,
    //         operationType: "read",
    //         componentType: "Reader"
    //     ))
    // }
  }

  private func monitorConsumerState() async {
    // Monitor the underlying consumer's state and propagate changes to reader state
    guard let consumerImpl = consumer as? ConsumerImpl<T> else {
      logger.debug("Consumer doesn't support state monitoring")
      return
    }

    for await consumerState in consumerImpl.stateStream {
      // Map consumer state to reader state
      let readerState: ClientState
      switch consumerState {
      case .disconnected:
        readerState = .disconnected
      case .initializing:
        readerState = .initializing
      case .connected:
        readerState = .connected
      case .connecting:
        readerState = .connecting
      case .reconnecting:
        readerState = .reconnecting
      case .closing:
        readerState = .closing
      case .closed:
        readerState = .closed
      case .faulted(let error):
        readerState = .faulted(error)
      }

      if readerState != _state {
        updateState(readerState)
      }
    }
  }
}
