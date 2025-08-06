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

/// Protocol for components that can receive messages
public protocol Receivable<T>: Sendable where T: Sendable {
  associatedtype T

  /// Receive a single message
  func receive() async throws -> Message<T>

  /// Receive a single message with timeout
  func receive(timeout: TimeInterval) async throws -> Message<T>

  /// Receive multiple messages
  func receiveBatch(maxMessages: Int) async throws -> [Message<T>]

  /// Receive multiple messages with timeout
  func receiveBatch(maxMessages: Int, timeout: TimeInterval) async throws -> [Message<T>]
}
