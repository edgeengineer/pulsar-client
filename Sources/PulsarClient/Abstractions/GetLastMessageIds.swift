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

/// A protocol for getting the Message IDs of the last messages on a topic.
public protocol GetLastMessageIds: Sendable {
    /// Gets the Message IDs of the last messages on the topic.
    ///
    /// - Returns: An array of `MessageId` objects for the last messages.
    /// - Throws: A `PulsarClientError` if the operation fails.
    func lastMessageIds() async throws -> [MessageId]
} 