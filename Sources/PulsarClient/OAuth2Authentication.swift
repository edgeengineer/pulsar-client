import Foundation

#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public final class OAuth2Authentication: Authentication, @unchecked Sendable {
  public let authenticationMethodName = "token"

  private let configuration: OAuth2Configuration
  private let tokenProvider: OAuth2TokenProvider

  public init(configuration: OAuth2Configuration) {
    self.configuration = configuration
    self.tokenProvider = OAuth2TokenProvider(configuration: configuration)
  }

  public func getAuthenticationData() async throws -> Data {
    let token = try await tokenProvider.getToken()
    return Data(token.utf8)
  }
}

public struct OAuth2Configuration: Sendable {
  public let issuerUrl: URL
  public let clientId: String
  public let clientSecret: String?
  public let audience: String?
  public let scope: String?
  public let privateKey: Data?
  public let keyId: String?

  public init(
    issuerUrl: URL,
    clientId: String,
    clientSecret: String? = nil,
    audience: String? = nil,
    scope: String? = nil,
    privateKey: Data? = nil,
    keyId: String? = nil
  ) {
    self.issuerUrl = issuerUrl
    self.clientId = clientId
    self.clientSecret = clientSecret
    self.audience = audience
    self.scope = scope
    self.privateKey = privateKey
    self.keyId = keyId
  }
}

actor OAuth2TokenProvider {
  private let configuration: OAuth2Configuration
  private var cachedToken: OAuth2Token?
  private let urlSession: URLSession

  init(configuration: OAuth2Configuration) {
    self.configuration = configuration
    self.urlSession = URLSession(configuration: .ephemeral)
  }

  func getToken() async throws -> String {
    if let cached = cachedToken, !cached.isExpired {
      return cached.accessToken
    }

    let token = try await fetchToken()
    cachedToken = token
    return token.accessToken
  }

  private func fetchToken() async throws -> OAuth2Token {
    var request = URLRequest(url: configuration.issuerUrl)
    request.httpMethod = "POST"
    request.setValue("application/x-www-form-urlencoded", forHTTPHeaderField: "Content-Type")

    var parameters = [
      "grant_type": "client_credentials",
      "client_id": configuration.clientId,
    ]

    if let clientSecret = configuration.clientSecret {
      parameters["client_secret"] = clientSecret
    }

    if let audience = configuration.audience {
      parameters["audience"] = audience
    }

    if let scope = configuration.scope {
      parameters["scope"] = scope
    }

    if let privateKey = configuration.privateKey,
      let keyId = configuration.keyId
    {
      let assertion = try createJWTAssertion(privateKey: privateKey, keyId: keyId)
      parameters["client_assertion_type"] = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer"
      parameters["client_assertion"] = assertion
    }

    let bodyString =
      parameters
      .map {
        "\($0.key)=\($0.value.addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed) ?? "")"
      }
      .joined(separator: "&")

    request.httpBody = bodyString.data(using: .utf8)

    let (data, response) = try await urlSession.data(for: request)

    guard let httpResponse = response as? HTTPURLResponse else {
      throw PulsarClientError.authenticationFailed("Invalid response type")
    }

    guard httpResponse.statusCode == 200 else {
      let errorMessage = String(data: data, encoding: .utf8) ?? "Unknown error"
      throw PulsarClientError.authenticationFailed("OAuth2 token request failed: \(errorMessage)")
    }

    let decoder = JSONDecoder()
    decoder.keyDecodingStrategy = .convertFromSnakeCase

    do {
      let tokenResponse = try decoder.decode(OAuth2TokenResponse.self, from: data)
      return OAuth2Token(from: tokenResponse)
    } catch {
      throw PulsarClientError.authenticationFailed(
        "Failed to decode OAuth2 token response: \(error)")
    }
  }

  private func createJWTAssertion(privateKey: Data, keyId: String) throws -> String {
    throw PulsarClientError.authenticationFailed("JWT assertion not yet implemented")
  }
}

struct OAuth2Token {
  let accessToken: String
  let expiresAt: Date
  let tokenType: String
  let refreshToken: String?

  var isExpired: Bool {
    Date() >= expiresAt.addingTimeInterval(-60)
  }

  init(from response: OAuth2TokenResponse) {
    self.accessToken = response.accessToken
    self.tokenType = response.tokenType
    self.refreshToken = response.refreshToken

    if let expiresIn = response.expiresIn {
      self.expiresAt = Date().addingTimeInterval(TimeInterval(expiresIn))
    } else {
      self.expiresAt = Date().addingTimeInterval(3600)
    }
  }
}

struct OAuth2TokenResponse: Decodable {
  let accessToken: String
  let tokenType: String
  let expiresIn: Int?
  let refreshToken: String?
}
