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
/// 
/// Conforming types should implement AsyncSequence to allow iteration over messages:
/// ```swift
/// for try await message in receiver {
///     // Process message
/// }
/// ```
public protocol Receivable<T>: AsyncSequence, Sendable 
where T: Sendable, Element == Message<T> {
  associatedtype T
}
