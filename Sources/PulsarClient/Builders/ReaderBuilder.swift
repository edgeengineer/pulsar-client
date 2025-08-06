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

/// Builder for creating readers
@dynamicMemberLookup
public final class ReaderBuilder<T> where T: Sendable {
  internal var options: ReaderOptions<T>

  public init(options: ReaderOptions<T>) {
    self.options = options
  }

  /// Convenience initializer for creating a reader with start message ID
  public convenience init(topic: String, schema: Schema<T>, startMessageId: MessageId) {
    self.init(options: ReaderOptions(startMessageId: startMessageId, topic: topic, schema: schema))
  }

  @discardableResult
  public func readerName(_ name: String?) -> Self {
    options = options.withReaderName(name)
    return self
  }

  @discardableResult
  public func messagePrefetchCount(_ count: UInt) -> Self {
    options = options.withMessagePrefetchCount(count)
    return self
  }

  @discardableResult
  public func readCompacted(_ enabled: Bool) -> Self {
    options = options.withReadCompacted(enabled)
    return self
  }

  @discardableResult
  public func subscriptionName(_ name: String?) -> Self {
    options = options.withSubscriptionName(name)
    return self
  }

  @discardableResult
  public func subscriptionRolePrefix(_ prefix: String) -> Self {
    options = options.withSubscriptionRolePrefix(prefix)
    return self
  }

  @discardableResult
  public func receiverQueueSize(_ size: Int) -> Self {
    options = options.withReceiverQueueSize(size)
    return self
  }

  @discardableResult
  public func cryptoKeyReader(_ reader: CryptoKeyReader?) -> Self {
    options = options.withCryptoKeyReader(reader)
    return self
  }

  @discardableResult
  public func properties(_ properties: [String: String]) -> Self {
    options = options.withProperties(properties)
    return self
  }

  @discardableResult
  public func stateChangedHandler(_ handler: @escaping @Sendable (ReaderStateChanged<T>) -> Void)
    -> Self
  {
    options = options.withStateChangedHandler(handler)
    return self
  }

  // Dynamic member lookup for accessing options properties
  public subscript<Value>(dynamicMember keyPath: KeyPath<ReaderOptions<T>, Value>) -> Value {
    return options[keyPath: keyPath]
  }
}
