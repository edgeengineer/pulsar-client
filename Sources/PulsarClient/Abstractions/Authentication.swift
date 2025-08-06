import Foundation

/// An authentication abstraction.
public protocol Authentication: Sendable {
  /// The authentication method name
  var authenticationMethodName: String { get }

  /// Get the authentication data
  func getAuthenticationData() async throws -> Data
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
