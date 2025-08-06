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

// MARK: - Type Aliases for Protocol Buffer Types

/// Compression type for messages
public typealias CompressionType = Pulsar_Proto_CompressionType

/// Server error type
public typealias ServerError = Pulsar_Proto_ServerError

/// Protocol version
public typealias ProtocolVersion = Int32

// MARK: - Common Types

// MARK: - Hashing Schemes

/// Hashing scheme for message routing
public enum HashingScheme: String, Sendable, CaseIterable {
  /// Java string hash
  case javaStringHash = "JavaStringHash"

  /// Murmur3 32-bit hash
  case murmur3_32Hash = "Murmur3_32Hash"
}

// MARK: - Crypto Key Reader

/// Protocol for reading crypto keys
public protocol CryptoKeyReader: Sendable {
  /// Get the public key for encryption
  func getPublicKey(keyName: String, metadata: [String: String]) async throws -> Data

  /// Get the private key for decryption
  func getPrivateKey(keyName: String, metadata: [String: String]) async throws -> Data
}

// MARK: - Authentication

/// Authentication protocol (already defined in Authentication.swift)
// public protocol Authentication: Sendable { ... }

// MARK: - Connection Types

/// Physical address of a broker
public struct PhysicalAddress: Sendable, Hashable {
  public let host: String
  public let port: Int

  public init(host: String, port: Int) {
    self.host = host
    self.port = port
  }

  public var description: String {
    return "\(host):\(port)"
  }
}

/// Logical address of a broker
public struct LogicalAddress: Sendable, Hashable {
  public let url: String

  public init(url: String) {
    self.url = url
  }
}

/// Broker address combining logical and physical addresses
public struct BrokerAddress: Sendable, Hashable {
  public let logical: LogicalAddress
  public let physical: PhysicalAddress

  public init(logical: LogicalAddress, physical: PhysicalAddress) {
    self.logical = logical
    self.physical = physical
  }
}
