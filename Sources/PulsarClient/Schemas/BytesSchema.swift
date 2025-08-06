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

/// Schema definition for raw bytes messages
public struct BytesSchema: SchemaProtocol {
  public typealias T = Data

  public let schemaInfo: SchemaInfo

  public init() {
    self.schemaInfo = SchemaInfo(
      name: "Bytes",
      type: .bytes,
      schema: nil,
      properties: [:]
    )
  }

  public func encode(_ value: Data) throws -> Data {
    return value
  }

  public func decode(_ data: Data) throws -> Data {
    return data
  }
}
