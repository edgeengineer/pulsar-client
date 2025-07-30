import Foundation

/// An authentication abstraction.
public protocol Authentication: Sendable {
    /// The authentication method name
    var authenticationMethodName: String { get }
    
    /// Get the authentication data
    func getAuthenticationData() async throws -> Data
    
    /// Handle authentication challenge from the broker
    /// - Parameter challenge: The challenge data from the broker
    /// - Returns: The response data for the challenge
    func handleAuthenticationChallenge(_ challenge: Pulsar_Proto_CommandAuthChallenge) async throws -> Data
    
    /// Check if authentication data needs refresh
    /// - Returns: true if the authentication data should be refreshed
    func needsRefresh() async -> Bool
}

/// Authentication context for challenge/response scenarios
public enum AuthenticationContext: Sendable {
    case initial
    case challenge(Pulsar_Proto_CommandAuthChallenge)
    case refresh
}

/// Default implementations for Authentication protocol
public extension Authentication {
    func handleAuthenticationChallenge(_ challenge: Pulsar_Proto_CommandAuthChallenge) async throws -> Data {
        // Default implementation just returns fresh auth data
        return try await getAuthenticationData()
    }
    
    func needsRefresh() async -> Bool {
        // Default implementation doesn't require refresh
        return false
    }
}

/// Token-based authentication implementation
public struct TokenAuthentication: Authentication {
    public let authenticationMethodName = "token"
    private let tokenProvider: TokenProvider
    
    /// Create token authentication with a static token
    public init(token: String) {
        self.tokenProvider = .static(token)
    }
    
    /// Create token authentication with a dynamic token supplier
    public init(tokenSupplier: @escaping @Sendable () async throws -> String) {
        self.tokenProvider = .dynamic(tokenSupplier)
    }
    
    public func getAuthenticationData() async throws -> Data {
        let token = try await tokenProvider.getToken()
        return Data(token.utf8)
    }
}

/// Token provider for different token sources
private enum TokenProvider: Sendable {
    case `static`(String)
    case dynamic(@Sendable () async throws -> String)
    
    func getToken() async throws -> String {
        switch self {
        case .static(let token):
            return token
        case .dynamic(let supplier):
            return try await supplier()
        }
    }
}

/// No authentication (for development/testing)
public struct NoAuthentication: Authentication {
    public let authenticationMethodName = "none"
    
    public init() {}
    
    public func getAuthenticationData() async throws -> Data {
        return Data()
    }
}