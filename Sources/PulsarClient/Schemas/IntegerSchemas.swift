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

// MARK: - Int8 Schema

/// Schema definition for Int8 (byte) messages
public struct Int8Schema: SchemaProtocol {
    public typealias T = Int8
    
    public let schemaInfo: SchemaInfo
    
    public init() {
        self.schemaInfo = SchemaInfo(
            name: "INT8",
            type: .int8,
            schema: nil,
            properties: [:]
        )
    }
    
    public func encode(_ value: Int8) throws -> Data {
        return Data([UInt8(bitPattern: value)])
    }
    
    public func decode(_ data: Data) throws -> Int8 {
        guard data.count == 1 else {
            throw PulsarClientError.schemaSerializationFailed(
                "Int8Schema expected to decode 1 byte, but received \(data.count) bytes"
            )
        }
        return Int8(bitPattern: data[0])
    }
}

// MARK: - Int16 Schema

/// Schema definition for Int16 (short) messages
public struct Int16Schema: SchemaProtocol {
    public typealias T = Int16
    
    public let schemaInfo: SchemaInfo
    
    public init() {
        self.schemaInfo = SchemaInfo(
            name: "INT16",
            type: .int16,
            schema: nil,
            properties: [:]
        )
    }
    
    public func encode(_ value: Int16) throws -> Data {
        var bigEndian = value.bigEndian
        return Data(bytes: &bigEndian, count: MemoryLayout<Int16>.size)
    }
    
    public func decode(_ data: Data) throws -> Int16 {
        guard data.count == 2 else {
            throw PulsarClientError.schemaSerializationFailed(
                "Int16Schema expected to decode 2 bytes, but received \(data.count) bytes"
            )
        }
        return data.withUnsafeBytes { bytes in
            bytes.load(as: Int16.self).bigEndian
        }
    }
}

// MARK: - Int32 Schema

/// Schema definition for Int32 (integer) messages
public struct Int32Schema: SchemaProtocol {
    public typealias T = Int32
    
    public let schemaInfo: SchemaInfo
    
    public init() {
        self.schemaInfo = SchemaInfo(
            name: "INT32",
            type: .int32,
            schema: nil,
            properties: [:]
        )
    }
    
    public func encode(_ value: Int32) throws -> Data {
        var bigEndian = value.bigEndian
        return Data(bytes: &bigEndian, count: MemoryLayout<Int32>.size)
    }
    
    public func decode(_ data: Data) throws -> Int32 {
        guard data.count == 4 else {
            throw PulsarClientError.schemaSerializationFailed(
                "Int32Schema expected to decode 4 bytes, but received \(data.count) bytes"
            )
        }
        return data.withUnsafeBytes { bytes in
            bytes.load(as: Int32.self).bigEndian
        }
    }
}

// MARK: - Int64 Schema

/// Schema definition for Int64 (long) messages
public struct Int64Schema: SchemaProtocol {
    public typealias T = Int64
    
    public let schemaInfo: SchemaInfo
    
    public init() {
        self.schemaInfo = SchemaInfo(
            name: "INT64",
            type: .int64,
            schema: nil,
            properties: [:]
        )
    }
    
    public func encode(_ value: Int64) throws -> Data {
        var bigEndian = value.bigEndian
        return Data(bytes: &bigEndian, count: MemoryLayout<Int64>.size)
    }
    
    public func decode(_ data: Data) throws -> Int64 {
        guard data.count == 8 else {
            throw PulsarClientError.schemaSerializationFailed(
                "Int64Schema expected to decode 8 bytes, but received \(data.count) bytes"
            )
        }
        return data.withUnsafeBytes { bytes in
            bytes.load(as: Int64.self).bigEndian
        }
    }
}