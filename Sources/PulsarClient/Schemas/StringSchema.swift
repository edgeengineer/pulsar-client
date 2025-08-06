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

/// Schema definition for String messages
public struct StringSchema: SchemaProtocol {
  public typealias T = String

  public let schemaInfo: SchemaInfo
  private let encoding: String.Encoding

  public init(encoding: String.Encoding = .utf8) {
    self.encoding = encoding
    self.schemaInfo = SchemaInfo(
      name: "String",
      type: .string,
      schema: nil,
      properties: ["encoding": encoding == .utf8 ? "UTF-8" : "UTF-16"]
    )
  }

  public func encode(_ value: String) throws -> Data {
    guard let data = value.data(using: encoding) else {
      throw PulsarClientError.schemaSerializationFailed("Failed to encode string with \(encoding)")
    }
    return data
  }

  public func decode(_ data: Data) throws -> String {
    guard let string = String(data: data, encoding: encoding) else {
      throw PulsarClientError.schemaSerializationFailed("Failed to decode string with \(encoding)")
    }
    return string
  }
}
