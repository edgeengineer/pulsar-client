import Foundation
import Testing

@testable import PulsarClient

@Suite("OAuth2 Authentication Tests")
struct OAuth2AuthenticationTests {

  @Test("OAuth2 Authentication Method Name")
  func testOAuth2AuthenticationMethodName() {
    let config = OAuth2Configuration(
      issuerUrl: URL(string: "https://example.com/oauth/token")!,
      clientId: "test-client"
    )
    let auth = OAuth2Authentication(configuration: config)

    #expect(auth.authenticationMethodName == "token")
  }

  @Test("OAuth2 Configuration with Client Secret")
  func testOAuth2ConfigurationWithClientSecret() {
    let config = OAuth2Configuration(
      issuerUrl: URL(string: "https://example.com/oauth/token")!,
      clientId: "test-client",
      clientSecret: "test-secret",
      audience: "https://example.com/api",
      scope: "read write"
    )

    #expect(config.issuerUrl.absoluteString == "https://example.com/oauth/token")
    #expect(config.clientId == "test-client")
    #expect(config.clientSecret == "test-secret")
    #expect(config.audience == "https://example.com/api")
    #expect(config.scope == "read write")
    #expect(config.privateKey == nil)
    #expect(config.keyId == nil)
  }

  @Test("OAuth2 Configuration with Private Key")
  func testOAuth2ConfigurationWithPrivateKey() {
    let privateKeyData = "private-key-data".data(using: .utf8)!
    let config = OAuth2Configuration(
      issuerUrl: URL(string: "https://example.com/oauth/token")!,
      clientId: "test-client",
      privateKey: privateKeyData,
      keyId: "key-123"
    )

    #expect(config.clientId == "test-client")
    #expect(config.clientSecret == nil)
    #expect(config.privateKey == privateKeyData)
    #expect(config.keyId == "key-123")
  }

  @Test("Authentication Factory OAuth2 Methods")
  func testAuthenticationFactoryOAuth2Methods() {
    let issuerUrl = URL(string: "https://example.com/oauth/token")!

    let auth1 = AuthenticationFactory.oauth2(
      issuerUrl: issuerUrl,
      clientId: "client1",
      clientSecret: "secret1"
    )
    #expect(auth1 is OAuth2Authentication)

    let auth2 = AuthenticationFactory.oauth2(
      issuerUrl: issuerUrl,
      clientId: "client2",
      clientSecret: "secret2",
      audience: "https://api.example.com",
      scope: "read"
    )
    #expect(auth2 is OAuth2Authentication)

    let privateKey = "key-data".data(using: .utf8)!
    let auth3 = AuthenticationFactory.oauth2(
      issuerUrl: issuerUrl,
      clientId: "client3",
      privateKey: privateKey,
      keyId: "key-456"
    )
    #expect(auth3 is OAuth2Authentication)

    let config = OAuth2Configuration(
      issuerUrl: issuerUrl,
      clientId: "client4"
    )
    let auth4 = AuthenticationFactory.oauth2(config)
    #expect(auth4 is OAuth2Authentication)
  }

  @Test("OAuth2 Token Expiration")
  func testOAuth2TokenIsExpired() {
    let response1 = OAuth2TokenResponse(
      accessToken: "token1",
      tokenType: "Bearer",
      expiresIn: 3600,
      refreshToken: nil
    )
    let token1 = OAuth2Token(from: response1)
    #expect(!token1.isExpired)

    let response2 = OAuth2TokenResponse(
      accessToken: "token2",
      tokenType: "Bearer",
      expiresIn: -100,
      refreshToken: nil
    )
    let token2 = OAuth2Token(from: response2)
    #expect(token2.isExpired)

    let response3 = OAuth2TokenResponse(
      accessToken: "token3",
      tokenType: "Bearer",
      expiresIn: nil,
      refreshToken: nil
    )
    let token3 = OAuth2Token(from: response3)
    #expect(!token3.isExpired)
  }

  @Test("OAuth2 Token from Response")
  func testOAuth2TokenFromResponse() {
    let response = OAuth2TokenResponse(
      accessToken: "test-access-token",
      tokenType: "Bearer",
      expiresIn: 7200,
      refreshToken: "test-refresh-token"
    )

    let token = OAuth2Token(from: response)

    #expect(token.accessToken == "test-access-token")
    #expect(token.tokenType == "Bearer")
    #expect(token.refreshToken == "test-refresh-token")
    #expect(!token.isExpired)

    let expectedExpiry = Date().addingTimeInterval(7200)
    #expect(abs(token.expiresAt.timeIntervalSince1970 - expectedExpiry.timeIntervalSince1970) < 1.0)
  }

  @Test("OAuth2 From File Throws on Invalid Path")
  func testOAuth2FromFileThrowsOnInvalidPath() throws {
    #expect(throws: (any Error).self) {
      _ = try AuthenticationFactory.oauth2FromFile("/non/existent/file.json")
    }
  }

  @Test("OAuth2 Configuration Sendable")
  func testOAuth2ConfigurationSendable() async {
    let config = OAuth2Configuration(
      issuerUrl: URL(string: "https://example.com")!,
      clientId: "test"
    )

    let task = Task {
      _ = config.clientId
    }

    _ = await task.value
    #expect(config.clientId == "test")
  }
}
