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

/// A protocol for seeking to a specific message ID or time.
public protocol Seekable: Sendable {
    /// Seek to a specific message ID.
    func seek(to messageId: MessageId) async throws
    
    /// Seek to a specific time.
    func seek(to timestamp: UInt64) async throws
}