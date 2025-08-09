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

// MARK: - Float Schema

/// Schema definition for Float messages
public struct FloatSchema: SchemaProtocol {
  public typealias T = Float

  public let schemaInfo: SchemaInfo

  public init() {
    self.schemaInfo = SchemaInfo(
      name: "Float",
      type: .float,
      schema: nil,
      properties: [:]
    )
  }

  public func encode(_ value: Float) throws -> Data {
    var bigEndian = value.bitPattern.bigEndian
    return Data(bytes: &bigEndian, count: MemoryLayout<UInt32>.size)
  }

  public func decode(_ data: Data) throws -> Float {
    guard data.count == 4 else {
      throw PulsarClientError.schemaSerializationFailed(
        "FloatSchema expected to decode 4 bytes, but received \(data.count) bytes"
      )
    }
    let bitPattern = data.withUnsafeBytes { bytes in
      bytes.load(as: UInt32.self).bigEndian
    }
    return Float(bitPattern: bitPattern)
  }
}

// MARK: - Double Schema

/// Schema definition for Double messages
public struct DoubleSchema: SchemaProtocol {
  public typealias T = Double

  public let schemaInfo: SchemaInfo

  public init() {
    self.schemaInfo = SchemaInfo(
      name: "Double",
      type: .double,
      schema: nil,
      properties: [:]
    )
  }

  public func encode(_ value: Double) throws -> Data {
    var bigEndian = value.bitPattern.bigEndian
    return Data(bytes: &bigEndian, count: MemoryLayout<UInt64>.size)
  }

  public func decode(_ data: Data) throws -> Double {
    guard data.count == 8 else {
      throw PulsarClientError.schemaSerializationFailed(
        "DoubleSchema expected to decode 8 bytes, but received \(data.count) bytes"
      )
    }
    let bitPattern = data.withUnsafeBytes { bytes in
      bytes.load(as: UInt64.self).bigEndian
    }
    return Double(bitPattern: bitPattern)
  }
}
