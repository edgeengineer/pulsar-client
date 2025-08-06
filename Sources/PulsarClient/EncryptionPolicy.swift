import Foundation

/// Encryption policies for Pulsar connections.
public enum EncryptionPolicy: UInt8, Sendable, CaseIterable {
  /// Never encrypt the connection.
  case enforceUnencrypted = 0

  /// Given the option of encrypting or not, prefer not to.
  case preferUnencrypted = 1

  /// Given the option of encrypting or not, prefer to do so.
  case preferEncrypted = 2

  /// Always encrypt the connection.
  case enforceEncrypted = 3
}

extension EncryptionPolicy: CustomStringConvertible {
  public var description: String {
    switch self {
    case .enforceUnencrypted:
      return "enforce-unencrypted"
    case .preferUnencrypted:
      return "prefer-unencrypted"
    case .preferEncrypted:
      return "prefer-encrypted"
    case .enforceEncrypted:
      return "enforce-encrypted"
    }
  }
}

extension EncryptionPolicy {
  /// Check if encryption is required
  public var isEncryptionRequired: Bool {
    switch self {
    case .enforceEncrypted:
      return true
    case .enforceUnencrypted, .preferUnencrypted, .preferEncrypted:
      return false
    }
  }

  /// Check if unencrypted connections are forbidden
  public var isUnencryptedForbidden: Bool {
    switch self {
    case .enforceEncrypted:
      return true
    case .enforceUnencrypted, .preferUnencrypted, .preferEncrypted:
      return false
    }
  }

  /// Get preference score for encryption (higher = more preferred)
  public var encryptionPreference: Int {
    switch self {
    case .enforceUnencrypted:
      return -100
    case .preferUnencrypted:
      return -1
    case .preferEncrypted:
      return 1
    case .enforceEncrypted:
      return 100
    }
  }
}
