import Foundation
import Testing

@testable import PulsarClient

@Suite("TLS Authentication Tests")
struct TLSAuthenticationTests {

  @Test("TLS Authentication Method Name")
  func testTLSAuthenticationMethodName() {
    let config = TLSConfiguration(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )
    let auth = TLSAuthentication(configuration: config)

    #expect(auth.authenticationMethodName == "tls")
  }

  @Test("TLS Configuration with Paths")
  func testTLSConfigurationWithPaths() {
    let config = TLSConfiguration(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )

    #expect(config.certificatePath == "/path/to/cert.pem")
    #expect(config.privateKeyPath == "/path/to/key.pem")
    #expect(config.certificateData == nil)
    #expect(config.privateKeyData == nil)
  }

  @Test("TLS Configuration with Data")
  func testTLSConfigurationWithData() {
    let certData = "certificate-content".data(using: .utf8)!
    let keyData = "key-content".data(using: .utf8)!

    let config = TLSConfiguration(
      certificateData: certData,
      privateKeyData: keyData
    )

    #expect(config.certificateData == certData)
    #expect(config.privateKeyData == keyData)
    #expect(config.certificatePath == "")
    #expect(config.privateKeyPath == "")
  }

  @Test("TLS Authentication Returns Empty Data")
  func testTLSAuthenticationReturnsEmptyData() async throws {
    let config = TLSConfiguration(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )
    let auth = TLSAuthentication(configuration: config)

    let data = try await auth.getAuthenticationData()
    #expect(data == Data())
  }

  @Test("Authentication Factory TLS Methods")
  func testAuthenticationFactoryTLSMethods() {
    let auth1 = AuthenticationFactory.tls(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )
    #expect(auth1 is TLSAuthentication)

    let certData = "cert".data(using: .utf8)!
    let keyData = "key".data(using: .utf8)!
    let auth2 = AuthenticationFactory.tls(
      certificateData: certData,
      privateKeyData: keyData
    )
    #expect(auth2 is TLSAuthentication)

    let config = TLSConfiguration(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )
    let auth3 = AuthenticationFactory.tls(config)
    #expect(auth3 is TLSAuthentication)
  }

  @Test("TLS Configuration Load Certificate")
  func testTLSConfigurationLoadCertificate() throws {
    let certContent = "test-certificate-content"
    let certData = certContent.data(using: .utf8)!

    let config = TLSConfiguration(
      certificateData: certData,
      privateKeyData: Data()
    )

    let loadedData = try config.loadCertificate()
    #expect(loadedData == certData)
  }

  @Test("TLS Configuration Load Private Key")
  func testTLSConfigurationLoadPrivateKey() throws {
    let keyContent = "test-private-key-content"
    let keyData = keyContent.data(using: .utf8)!

    let config = TLSConfiguration(
      certificateData: Data(),
      privateKeyData: keyData
    )

    let loadedData = try config.loadPrivateKey()
    #expect(loadedData == keyData)
  }

  @Test("TLS Configuration Load Certificate from Invalid Path")
  func testTLSConfigurationLoadCertificateFromInvalidPath() {
    let config = TLSConfiguration(
      certificatePath: "/non/existent/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )

    #expect(throws: PulsarClientError.self) {
      _ = try config.loadCertificate()
    }
  }

  @Test("TLS Configuration Load Private Key from Invalid Path")
  func testTLSConfigurationLoadPrivateKeyFromInvalidPath() {
    let config = TLSConfiguration(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/non/existent/key.pem"
    )

    #expect(throws: PulsarClientError.self) {
      _ = try config.loadPrivateKey()
    }
  }

  @Test("TLS Configuration No Data or Path")
  func testTLSConfigurationNoDataOrPath() {
    let config = TLSConfiguration(
      certificatePath: "",
      privateKeyPath: ""
    )

    #expect(throws: PulsarClientError.self) {
      _ = try config.loadCertificate()
    }

    #expect(throws: PulsarClientError.self) {
      _ = try config.loadPrivateKey()
    }
  }

  @Test("TLS From Files Throws on Invalid Path")
  func testTLSFromFilesThrowsOnInvalidPath() {
    #expect(throws: (any Error).self) {
      _ = try AuthenticationFactory.tlsFromFiles(
        certificatePath: "/non/existent/cert.pem",
        privateKeyPath: "/non/existent/key.pem"
      )
    }
  }

  @Test("TLS Configuration Sendable")
  func testTLSConfigurationSendable() async {
    let config = TLSConfiguration(
      certificatePath: "/path/to/cert.pem",
      privateKeyPath: "/path/to/key.pem"
    )

    let task = Task {
      _ = config.certificatePath
    }

    _ = await task.value
    #expect(config.certificatePath == "/path/to/cert.pem")
  }
}
