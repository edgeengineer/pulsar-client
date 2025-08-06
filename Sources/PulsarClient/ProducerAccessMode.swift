import Foundation

/// The Access modes that can be set on a producer.
public enum ProducerAccessMode: UInt8, Sendable, CaseIterable {
  /// Shared Access Mode. Multiple producers can publish on a topic.
  case shared = 0

  /// Only one producer can publish on a topic.
  ///
  /// If there is already a producer connected, other producers trying to
  /// publish on this topic get errors immediately.
  ///
  /// The "old" producer is evicted and a "new" producer is selected to be
  /// the next exclusive producer if the "old" producer experiences a
  /// network partition with the broker.
  case exclusive = 1

  /// If there is already a producer connected, the producer creation is
  /// pending (rather than timing out) until the producer gets the
  /// Exclusive access.
  ///
  /// The producer that succeeds in becoming the exclusive one is treated
  /// as the leader. Consequently, if you want to implement a leader
  /// election scheme for your application, you can use this access mode.
  /// Note that the leader pattern scheme mentioned refers to using Pulsar
  /// as a Write-Ahead Log (WAL) which means the leader writes its
  /// "decisions" to the topic. On error cases, the leader will get
  /// notified it is no longer the leader only when it tries to write a
  /// message and fails on appropriate error, by the broker.
  case waitForExclusive = 2

  /// Only one producer can publish on a topic.
  ///
  /// If there is already a producer connected, it will be removed and
  /// invalidated immediately.
  case exclusiveWithFencing = 3
}

extension ProducerAccessMode: CustomStringConvertible {
  public var description: String {
    switch self {
    case .shared:
      return "shared"
    case .exclusive:
      return "exclusive"
    case .waitForExclusive:
      return "wait-for-exclusive"
    case .exclusiveWithFencing:
      return "exclusive-with-fencing"
    }
  }
}

extension ProducerAccessMode {
  /// Check if this access mode is exclusive
  public var isExclusive: Bool {
    switch self {
    case .shared:
      return false
    case .exclusive, .waitForExclusive, .exclusiveWithFencing:
      return true
    }
  }

  /// Check if this access mode waits for exclusive access
  public var waitsForExclusive: Bool {
    switch self {
    case .waitForExclusive:
      return true
    case .shared, .exclusive, .exclusiveWithFencing:
      return false
    }
  }

  /// Check if this access mode supports fencing
  public var supportsFencing: Bool {
    switch self {
    case .exclusiveWithFencing:
      return true
    case .shared, .exclusive, .waitForExclusive:
      return false
    }
  }
}

// MARK: - Protocol Buffer Conversion

extension ProducerAccessMode {
  /// Convert to Protocol Buffer enum value
  public func toProto() -> Pulsar_Proto_ProducerAccessMode {
    switch self {
    case .shared:
      return .shared
    case .exclusive:
      return .exclusive
    case .waitForExclusive:
      return .waitForExclusive
    case .exclusiveWithFencing:
      return .exclusiveWithFencing
    }
  }

  /// Create from Protocol Buffer enum value
  public init(from proto: Pulsar_Proto_ProducerAccessMode) {
    switch proto {
    case .shared:
      self = .shared
    case .exclusive:
      self = .exclusive
    case .waitForExclusive:
      self = .waitForExclusive
    case .exclusiveWithFencing:
      self = .exclusiveWithFencing
    @unknown default:
      self = .shared  // Default fallback
    }
  }
}
