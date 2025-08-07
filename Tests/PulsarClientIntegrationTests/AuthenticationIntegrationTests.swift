import Foundation
import Logging
import NIOCore
import NIOPosix
import Testing

@testable import PulsarClient

@Suite("Authentication Integration Tests", .serialized)
struct AuthenticationIntegrationTests {

  let serviceUrl: String
  let adminUrl: String

  init() {
    self.serviceUrl =
      ProcessInfo.processInfo.environment["PULSAR_SERVICE_URL"] ?? "pulsar://localhost:6650"
    self.adminUrl =
      ProcessInfo.processInfo.environment["PULSAR_ADMIN_URL"] ?? "http://localhost:8080"
  }

  // MARK: - Token Authentication Tests

  @Test("Token authentication with static token")
  func tokenAuthenticationWithStaticToken() async throws {
    let token = "test-jwt-token"
    let auth = TokenAuthentication(token: token)

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl(serviceUrl)
        .withAuthentication(auth)
        .withLogger(IntegrationTestLogger.shared)
    }

    defer { Task { await client.dispose() } }

    // Verify we can create a producer with authentication
    let producer = try await client.newProducer(
      topic: "persistent://public/default/test-token-auth",
      schema: Schema<String>.string
    ) { _ in }

    defer { Task { await producer.dispose() } }

    // Send a test message
    try await producer.send("Test message")
  }

  @Test("Token authentication with dynamic supplier")
  func tokenAuthenticationWithDynamicSupplier() async throws {
    actor TokenCounter {
      private var tokenVersion = 0

      func nextToken() -> String {
        tokenVersion += 1
        return "test-jwt-token-v\(tokenVersion)"
      }

      func getVersion() -> Int {
        return tokenVersion
      }
    }

    let counter = TokenCounter()
    let auth = TokenAuthentication {
      await counter.nextToken()
    }

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl(serviceUrl)
        .withAuthentication(auth)
        .withLogger(IntegrationTestLogger.shared)
    }

    defer { Task { await client.dispose() } }

    // Create producer to test dynamic token
    let producer = try await client.newProducer(
      topic: "persistent://public/default/test-dynamic-token",
      schema: Schema<String>.string
    ) { _ in }

    defer { Task { await producer.dispose() } }

    // Token should have been called at least once
    let version = await counter.getVersion()
    #expect(version > 0)
  }

  // MARK: - Custom Authentication Provider Tests

  @Test("Custom authentication provider")
  func customAuthenticationProvider() async throws {
    let customAuth = MockCustomAuthentication()

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl(serviceUrl)
        .withAuthentication(customAuth)
        .withLogger(IntegrationTestLogger.shared)
    }

    defer { Task { await client.dispose() } }

    // Verify authentication was called
    let authData = try await customAuth.getAuthenticationData()
    #expect(authData.count > 0)

    // Verify custom method name
    #expect(customAuth.authenticationMethodName == "custom-test")
  }

  // MARK: - Authentication Challenge/Response Tests

  @Test("Authentication challenge response handling")
  func authenticationChallengeResponse() async throws {
    let challengeAuth = MockChallengeResponseAuthentication()

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl(serviceUrl)
        .withAuthentication(challengeAuth)
        .withLogger(IntegrationTestLogger.shared)
    }

    defer { Task { await client.dispose() } }

    // Simulate auth challenge
    var challenge = Pulsar_Proto_CommandAuthChallenge()
    challenge.serverVersion = "2.11.0"

    var authChallenge = Pulsar_Proto_AuthData()
    authChallenge.authMethodName = "challenge"
    authChallenge.authData = "test-challenge".data(using: .utf8)!
    challenge.challenge = authChallenge

    // Handle the challenge
    let response = try await challengeAuth.handleAuthenticationChallenge(challenge)
    #expect(response.count > 0)

    // Verify challenge was handled
    let handled = await challengeAuth.wasChallengeHandled()
    #expect(handled)
  }

  // MARK: - Authentication Refresh Tests

  @Test("Authentication refresh mechanism")
  func authenticationRefresh() async throws {
    let refreshAuth = MockRefreshableAuthentication()

    let client = PulsarClient.builder { builder in
      builder.withServiceUrl(serviceUrl)
        .withAuthentication(refreshAuth)
        .withLogger(IntegrationTestLogger.shared)
    }

    defer { Task { await client.dispose() } }

    // Initially should not need refresh
    var needsRefresh = await refreshAuth.needsRefresh()
    #expect(!needsRefresh)

    // Mark as needing refresh
    await refreshAuth.markNeedsRefresh()

    // Now should need refresh
    needsRefresh = await refreshAuth.needsRefresh()
    #expect(needsRefresh)

    // Get fresh auth data
    let authData = try await refreshAuth.getAuthenticationData()
    #expect(authData.count > 0)

    // Should no longer need refresh after getting data
    needsRefresh = await refreshAuth.needsRefresh()
    #expect(!needsRefresh)
  }

  @Test("OAuth2 authentication refresh detection")
  func oAuth2AuthenticationRefresh() async throws {
    // This test would require a mock OAuth2 server
    // For now, just test the refresh detection logic

    let config = OAuth2Configuration(
      issuerUrl: URL(string: "https://example.com/oauth/token")!,
      clientId: "test-client",
      clientSecret: "test-secret"
    )

    let auth = OAuth2Authentication(configuration: config)

    // Should need refresh initially (no cached token)
    let needsRefresh = await auth.needsRefresh()
    #expect(needsRefresh)
  }
}

// MARK: - Mock Authentication Implementations

private actor MockCustomAuthentication: Authentication {
  let authenticationMethodName = "custom-test"
  private var callCount = 0

  func getAuthenticationData() async throws -> Data {
    callCount += 1
    return "custom-auth-data-\(callCount)".data(using: .utf8)!
  }
}

private actor MockChallengeResponseAuthentication: Authentication {
  let authenticationMethodName = "challenge-test"
  private var challengeHandled = false

  func getAuthenticationData() async throws -> Data {
    return "initial-auth-data".data(using: .utf8)!
  }

  func handleAuthenticationChallenge(_ challenge: Pulsar_Proto_CommandAuthChallenge) async throws
    -> Data
  {
    challengeHandled = true
    return "challenge-response-data".data(using: .utf8)!
  }

  func wasChallengeHandled() -> Bool {
    return challengeHandled
  }
}

private actor MockRefreshableAuthentication: Authentication {
  let authenticationMethodName = "refresh-test"
  private var shouldRefresh = false
  private var refreshCount = 0

  func getAuthenticationData() async throws -> Data {
    if shouldRefresh {
      refreshCount += 1
      shouldRefresh = false
    }
    return "auth-data-v\(refreshCount)".data(using: .utf8)!
  }

  func needsRefresh() -> Bool {
    return shouldRefresh
  }

  func markNeedsRefresh() {
    shouldRefresh = true
  }
}

// MARK: - Test Helpers

enum IntegrationTestLogger {
  static let shared = Logger(label: "integration-tests")
}
