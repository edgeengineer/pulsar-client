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

/// Schema definition for JSON messages using Codable
public struct JSONSchema<T: Codable>: SchemaProtocol {
  public let schemaInfo: SchemaInfo
  private let encoder: JSONEncoder
  private let decoder: JSONDecoder

  public init(
    encoder: JSONEncoder = JSONEncoder(),
    decoder: JSONDecoder = JSONDecoder()
  ) {
    self.encoder = encoder
    self.decoder = decoder

    // Generate JSON schema if possible
    let schemaString = Self.generateJSONSchema(for: T.self)
    let schemaData = schemaString?.data(using: .utf8)

    self.schemaInfo = SchemaInfo(
      name: String(describing: T.self),
      type: .json,
      schema: schemaData,
      properties: ["__jsr310ConversionEnabled": "false"]
    )
  }

  public func encode(_ value: T) throws -> Data {
    do {
      return try encoder.encode(value)
    } catch {
      throw PulsarClientError.schemaSerializationFailed("Failed to encode JSON: \(error)")
    }
  }

  public func decode(_ data: Data) throws -> T {
    do {
      return try decoder.decode(T.self, from: data)
    } catch {
      throw PulsarClientError.schemaSerializationFailed("Failed to decode JSON: \(error)")
    }
  }

  // Helper function to generate basic JSON schema
  private static func generateJSONSchema(for type: T.Type) -> String? {
    // This is a simplified version. In production, you might want to use
    // a more sophisticated JSON schema generator
    return """
      {
          "type": "object",
          "title": "\(String(describing: type))",
          "properties": {}
      }
      """
  }
}
