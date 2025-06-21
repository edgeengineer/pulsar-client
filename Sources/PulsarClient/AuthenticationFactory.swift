import Foundation

/// Factory for creating Authentication instances for all the supported authentication methods.
public enum AuthenticationFactory {
    
    /// Create an authentication provider for token based authentication.
    /// - Parameter token: The static authentication token
    /// - Returns: A token authentication instance
    public static func token(_ token: String) -> Authentication {
        return TokenAuthentication(token: token)
    }
    
    /// Create an authentication provider for token based authentication with dynamic token supply.
    /// - Parameter tokenSupplier: A function that provides authentication tokens
    /// - Returns: A token authentication instance
    public static func token(supplier tokenSupplier: @escaping @Sendable () async throws -> String) -> Authentication {
        return TokenAuthentication(tokenSupplier: tokenSupplier)
    }
    
    /// Create an authentication provider that doesn't perform authentication.
    /// This should only be used for development/testing environments.
    /// - Returns: A no-op authentication instance
    public static func none() -> Authentication {
        return NoAuthentication()
    }
}

// MARK: - Convenience Extensions

public extension AuthenticationFactory {
    
    /// Create token authentication from a file path
    /// - Parameter filePath: Path to file containing the token
    /// - Returns: A token authentication instance
    static func tokenFromFile(_ filePath: String) -> Authentication {
        return token {
            do {
                let token = try String(contentsOfFile: filePath, encoding: .utf8)
                return token.trimmingCharacters(in: .whitespacesAndNewlines)
            } catch {
                throw PulsarClientError.authenticationFailed("Failed to read token from file: \(error)")
            }
        }
    }
    
    /// Create token authentication from environment variable
    /// - Parameter variableName: Name of the environment variable containing the token
    /// - Returns: A token authentication instance
    static func tokenFromEnvironment(_ variableName: String) -> Authentication {
        return token {
            guard let token = ProcessInfo.processInfo.environment[variableName] else {
                throw PulsarClientError.authenticationFailed("Environment variable '\(variableName)' not found")
            }
            return token
        }
    }
}