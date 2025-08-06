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

// MARK: - Message Convenience Extensions

extension Message {

  /// Get a property value from the message
  public func property(for key: String) -> String? {
    return properties[key]
  }

  /// Check if the message has a specific property
  public func hasProperty(_ key: String) -> Bool {
    return properties[key] != nil
  }

  /// Get the age of the message (time since publication)
  public var age: TimeInterval {
    return Date().timeIntervalSince(publishTime)
  }

  /// Check if the message is older than a specified duration
  public func isOlderThan(_ duration: TimeInterval) -> Bool {
    return age > duration
  }

  /// Get the conversation ID if present (common pattern in messaging)
  public var conversationId: String? {
    return properties["conversation-id"] ?? properties["conversationId"]
  }

  /// Get the correlation ID if present
  public var correlationId: String? {
    return properties["correlation-id"] ?? properties["correlationId"]
  }
}

// MARK: - MessageMetadata Builder Extensions

extension MessageMetadata {

  /// Set conversation ID
  public mutating func withConversationId(_ conversationId: String) -> MessageMetadata {
    properties["conversation-id"] = conversationId
    return self
  }

  /// Set correlation ID
  public mutating func withCorrelationId(_ correlationId: String) -> MessageMetadata {
    properties["correlation-id"] = correlationId
    return self
  }

  /// Set TTL (time to live) in seconds
  public mutating func withTTL(_ seconds: TimeInterval) -> MessageMetadata {
    deliverAfter = seconds
    return self
  }

  /// Add multiple properties to existing ones
  public mutating func addProperties(_ newProperties: [String: String]) -> MessageMetadata {
    for (key, value) in newProperties {
      properties[key] = value
    }
    return self
  }

  /// Set delivery delay
  public mutating func withDeliveryDelay(_ delay: TimeInterval) -> MessageMetadata {
    deliverAfter = delay
    return self
  }
}

// MARK: - MessageId Extensions

extension MessageId {

  /// Check if this is a special marker ID
  public var isSpecial: Bool {
    return self == .earliest || self == .latest
  }

  /// Create a string representation suitable for persistence
  public var persistenceString: String {
    return description
  }

  /// Create from a persistence string
  public static func fromPersistenceString(_ string: String) -> MessageId? {
    return MessageId.parse(string)
  }
}

// MARK: - Message Filtering

extension Sequence {

  /// Filter messages by key
  public func withKey<T>(_ key: String) -> [Message<T>] where Element == Message<T> {
    return filter { $0.key == key }
  }

  /// Filter messages by property
  public func withProperty<T>(_ propertyKey: String, value: String) -> [Message<T>]
  where Element == Message<T> {
    return filter { $0.properties[propertyKey] == value }
  }

  /// Filter messages published after a date
  public func publishedAfter<T>(_ date: Date) -> [Message<T>] where Element == Message<T> {
    return filter { $0.publishTime > date }
  }

  /// Filter messages published before a date
  public func publishedBefore<T>(_ date: Date) -> [Message<T>] where Element == Message<T> {
    return filter { $0.publishTime < date }
  }

  /// Group messages by key
  public func groupedByKey<T>() -> [String?: [Message<T>]] where Element == Message<T> {
    return Dictionary(grouping: self) { $0.key }
  }

  /// Sort messages by publish time (oldest first)
  public func sortedByPublishTime<T>() -> [Message<T>] where Element == Message<T> {
    return sorted { $0.publishTime < $1.publishTime }
  }
}
