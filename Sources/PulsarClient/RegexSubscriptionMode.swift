import Foundation

/// When subscribing to topics using a regular expression, one can specify to only pick a certain type of topics.
public enum RegexSubscriptionMode: UInt8, Sendable, CaseIterable {
    /// Only subscribe to persistent topics.
    case persistent = 0
    
    /// Only subscribe to non-persistent topics.
    case nonPersistent = 1
    
    /// Subscribe to both persistent and non-persistent topics.
    case all = 2
}

extension RegexSubscriptionMode: CustomStringConvertible {
    public var description: String {
        switch self {
        case .persistent:
            return "persistent"
        case .nonPersistent:
            return "non-persistent"
        case .all:
            return "all"
        }
    }
}