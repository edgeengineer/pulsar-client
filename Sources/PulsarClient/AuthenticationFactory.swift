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
    
    /// Create an OAuth2 authentication provider using client credentials flow.
    /// - Parameter configuration: OAuth2 configuration parameters
    /// - Returns: An OAuth2 authentication instance
    public static func oauth2(_ configuration: OAuth2Configuration) -> Authentication {
        return OAuth2Authentication(configuration: configuration)
    }
    
    /// Create an OAuth2 authentication provider using client credentials with client secret.
    /// - Parameters:
    ///   - issuerUrl: The OAuth2 token issuer URL
    ///   - clientId: The client identifier
    ///   - clientSecret: The client secret
    ///   - audience: Optional audience parameter
    ///   - scope: Optional scope parameter
    /// - Returns: An OAuth2 authentication instance
    public static func oauth2(
        issuerUrl: URL,
        clientId: String,
        clientSecret: String,
        audience: String? = nil,
        scope: String? = nil
    ) -> Authentication {
        let configuration = OAuth2Configuration(
            issuerUrl: issuerUrl,
            clientId: clientId,
            clientSecret: clientSecret,
            audience: audience,
            scope: scope
        )
        return OAuth2Authentication(configuration: configuration)
    }
    
    
    /// Create a TLS authentication provider using certificate and private key files.
    /// - Parameters:
    ///   - certificatePath: Path to the client certificate file (PEM format)
    ///   - privateKeyPath: Path to the client private key file (PEM format)
    /// - Returns: A TLS authentication instance
    public static func tls(certificatePath: String, privateKeyPath: String) -> Authentication {
        let configuration = TLSConfiguration(
            certificatePath: certificatePath,
            privateKeyPath: privateKeyPath
        )
        return TLSAuthentication(configuration: configuration)
    }
    
    /// Create a TLS authentication provider using certificate and private key data.
    /// - Parameters:
    ///   - certificateData: The client certificate data (PEM format)
    ///   - privateKeyData: The client private key data (PEM format)
    /// - Returns: A TLS authentication instance
    public static func tls(certificateData: Data, privateKeyData: Data) -> Authentication {
        let configuration = TLSConfiguration(
            certificateData: certificateData,
            privateKeyData: privateKeyData
        )
        return TLSAuthentication(configuration: configuration)
    }
    
    /// Create a TLS authentication provider using configuration.
    /// - Parameter configuration: TLS configuration with certificate and key information
    /// - Returns: A TLS authentication instance
    public static func tls(_ configuration: TLSConfiguration) -> Authentication {
        return TLSAuthentication(configuration: configuration)
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
    
    /// Create OAuth2 authentication from a JSON credentials file
    /// - Parameter filePath: Path to JSON file containing OAuth2 credentials
    /// - Returns: An OAuth2 authentication instance
    static func oauth2FromFile(_ filePath: String) throws -> Authentication {
        let data = try Data(contentsOf: URL(fileURLWithPath: filePath))
        let decoder = JSONDecoder()
        
        struct OAuth2Credentials: Decodable {
            let issuerUrl: String
            let clientId: String
            let clientSecret: String?
            let audience: String?
            let scope: String?
        }
        
        let credentials = try decoder.decode(OAuth2Credentials.self, from: data)
        
        guard let issuerUrl = URL(string: credentials.issuerUrl) else {
            throw PulsarClientError.authenticationFailed("Invalid issuer URL in credentials file")
        }
        
        let configuration = OAuth2Configuration(
            issuerUrl: issuerUrl,
            clientId: credentials.clientId,
            clientSecret: credentials.clientSecret,
            audience: credentials.audience,
            scope: credentials.scope
        )
        
        return OAuth2Authentication(configuration: configuration)
    }
    
    /// Create TLS authentication from PEM files
    /// - Parameters:
    ///   - certificatePath: Path to the client certificate PEM file
    ///   - privateKeyPath: Path to the client private key PEM file
    /// - Returns: A TLS authentication instance
    /// - Throws: Error if files cannot be read
    static func tlsFromFiles(certificatePath: String, privateKeyPath: String) throws -> Authentication {
        let certData = try Data(contentsOf: URL(fileURLWithPath: certificatePath))
        let keyData = try Data(contentsOf: URL(fileURLWithPath: privateKeyPath))
        
        return tls(certificateData: certData, privateKeyData: keyData)
    }
}