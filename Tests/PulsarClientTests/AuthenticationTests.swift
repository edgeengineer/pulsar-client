import Foundation
import Testing
@testable import PulsarClient

@Suite("Authentication Tests")
struct AuthenticationTests {
    
    // MARK: - Token Authentication Tests
    
    @Test("Static token authentication")
    func staticTokenAuthentication() async throws {
        let token = "test-token-123"
        let auth = TokenAuthentication(token: token)
        
        #expect(auth.authenticationMethodName == "token")
        
        let authData = try await auth.getAuthenticationData()
        let authString = String(data: authData, encoding: .utf8)
        #expect(authString == token)
        
        // Should not need refresh for static token
        let needsRefresh = await auth.needsRefresh()
        #expect(!needsRefresh)
    }
    
    @Test("Dynamic token authentication with supplier")
    func dynamicTokenAuthentication() async throws {
        actor CallCounter {
            private var callCount = 0
            
            func nextToken() -> String {
                callCount += 1
                return "dynamic-token-\(callCount)"
            }
            
            func getCount() -> Int {
                return callCount
            }
        }
        
        let counter = CallCounter()
        let auth = TokenAuthentication {
            await counter.nextToken()
        }
        
        #expect(auth.authenticationMethodName == "token")
        
        // First call
        let authData1 = try await auth.getAuthenticationData()
        let authString1 = String(data: authData1, encoding: .utf8)
        #expect(authString1 == "dynamic-token-1")
        
        // Second call should get new token
        let authData2 = try await auth.getAuthenticationData()
        let authString2 = String(data: authData2, encoding: .utf8)
        #expect(authString2 == "dynamic-token-2")
        
        let count = await counter.getCount()
        #expect(count == 2)
    }
    
    // MARK: - Mutual TLS Authentication Tests
    
    @Test("Mutual TLS authentication")
    func mutualTLSAuthentication() async throws {
        let certPath = URL(fileURLWithPath: "/tmp/test-cert.pem")
        let keyPath = URL(fileURLWithPath: "/tmp/test-key.pem")
        
        // Create test certificate and key files
        let certData = "-----BEGIN CERTIFICATE-----\ntest certificate\n-----END CERTIFICATE-----"
        let keyData = "-----BEGIN PRIVATE KEY-----\ntest key\n-----END PRIVATE KEY-----"
        
        try certData.write(to: certPath, atomically: true, encoding: .utf8)
        try keyData.write(to: keyPath, atomically: true, encoding: .utf8)
        
        defer {
            try? FileManager.default.removeItem(at: certPath)
            try? FileManager.default.removeItem(at: keyPath)
        }
        
        let auth = MutualTLSAuthentication(
            certificatePath: certPath,
            privateKeyPath: keyPath
        )
        
        #expect(auth.authenticationMethodName == "tls")
        
        try await auth.initialize()
        
        let authData = try await auth.getAuthenticationData()
        let authString = String(data: authData, encoding: .utf8)
        #expect(authString == certData)
        
        await auth.cleanup()
    }
    
    // MARK: - Authentication Protocol Extension Tests
    
    @Test("Default handleAuthenticationChallenge implementation")
    func defaultHandleAuthenticationChallenge() async throws {
        let auth = TokenAuthentication(token: "test-token")
        
        var challenge = Pulsar_Proto_CommandAuthChallenge()
        challenge.serverVersion = "2.11.0"
        
        // Default implementation should return fresh auth data
        let response = try await auth.handleAuthenticationChallenge(challenge)
        let responseString = String(data: response, encoding: .utf8)
        #expect(responseString == "test-token")
    }
    
    @Test("Default needsRefresh implementation")
    func defaultNeedsRefresh() async throws {
        let auth = TokenAuthentication(token: "test-token")
        
        // Default implementation should return false
        let needsRefresh = await auth.needsRefresh()
        #expect(!needsRefresh)
    }
    
    // MARK: - No Authentication Tests
    
    @Test("No authentication")
    func noAuthentication() async throws {
        let auth = NoAuthentication()
        
        #expect(auth.authenticationMethodName == "none")
        
        let authData = try await auth.getAuthenticationData()
        #expect(authData.count == 0)
        
        let needsRefresh = await auth.needsRefresh()
        #expect(!needsRefresh)
    }
    
    // MARK: - Authentication Context Tests
    
    @Test("Authentication context enum cases")
    func authenticationContext() {
        // Test that context enum cases work correctly
        let initialContext = AuthenticationContext.initial
        let refreshContext = AuthenticationContext.refresh
        
        var challenge = Pulsar_Proto_CommandAuthChallenge()
        challenge.serverVersion = "test"
        let challengeContext = AuthenticationContext.challenge(challenge)
        
        // Verify enum cases are distinct
        switch initialContext {
        case .initial:
            break // Expected case
        default:
            Issue.record("Wrong context for initial")
        }
        
        switch refreshContext {
        case .refresh:
            break // Expected case
        default:
            Issue.record("Wrong context for refresh")
        }
        
        switch challengeContext {
        case .challenge(let ch):
            #expect(ch.serverVersion == "test")
        default:
            Issue.record("Wrong context for challenge")
        }
    }
}