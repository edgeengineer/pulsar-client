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

// MARK: - Date Schema

/// Schema definition for Date messages
public struct DateSchema: SchemaProtocol {
  public typealias T = Date

  public let schemaInfo: SchemaInfo

  public init() {
    self.schemaInfo = SchemaInfo(
      name: "Date",
      type: .date,
      schema: nil,
      properties: [:]
    )
  }

  public func encode(_ value: Date) throws -> Data {
    // Encode as milliseconds since epoch
    let millis = Int64(value.timeIntervalSince1970 * 1000)
    var bigEndian = millis.bigEndian
    return Data(bytes: &bigEndian, count: MemoryLayout<Int64>.size)
  }

  public func decode(_ data: Data) throws -> Date {
    guard data.count == 8 else {
      throw PulsarClientError.schemaSerializationFailed(
        "DateSchema expected to decode 8 bytes, but received \(data.count) bytes"
      )
    }
    let millis = data.withUnsafeBytes { bytes in
      bytes.load(as: Int64.self).bigEndian
    }
    return Date(timeIntervalSince1970: Double(millis) / 1000.0)
  }
}

// MARK: - Time Schema

/// Schema definition for Time messages (TimeInterval)
public struct TimeSchema: SchemaProtocol {
  public typealias T = TimeInterval

  public let schemaInfo: SchemaInfo

  public init() {
    self.schemaInfo = SchemaInfo(
      name: "Time",
      type: .time,
      schema: nil,
      properties: [:]
    )
  }

  public func encode(_ value: TimeInterval) throws -> Data {
    // Encode as microseconds
    let micros = Int64(value * 1_000_000)
    var bigEndian = micros.bigEndian
    return Data(bytes: &bigEndian, count: MemoryLayout<Int64>.size)
  }

  public func decode(_ data: Data) throws -> TimeInterval {
    guard data.count == 8 else {
      throw PulsarClientError.schemaSerializationFailed(
        "TimeSchema expected to decode 8 bytes, but received \(data.count) bytes"
      )
    }
    let micros = data.withUnsafeBytes { bytes in
      bytes.load(as: Int64.self).bigEndian
    }
    return TimeInterval(micros) / 1_000_000.0
  }
}

// MARK: - Timestamp Schema

/// Schema definition for Timestamp messages
public struct TimestampSchema: SchemaProtocol {
  public typealias T = Date

  public let schemaInfo: SchemaInfo

  public init() {
    self.schemaInfo = SchemaInfo(
      name: "Timestamp",
      type: .timestamp,
      schema: nil,
      properties: [:]
    )
  }

  public func encode(_ value: Date) throws -> Data {
    // Encode as milliseconds since epoch
    let millis = Int64(value.timeIntervalSince1970 * 1000)
    var bigEndian = millis.bigEndian
    return Data(bytes: &bigEndian, count: MemoryLayout<Int64>.size)
  }

  public func decode(_ data: Data) throws -> Date {
    guard data.count == 8 else {
      throw PulsarClientError.schemaSerializationFailed(
        "TimestampSchema expected to decode 8 bytes, but received \(data.count) bytes"
      )
    }
    let millis = data.withUnsafeBytes { bytes in
      bytes.load(as: Int64.self).bigEndian
    }
    return Date(timeIntervalSince1970: Double(millis) / 1000.0)
  }
}
