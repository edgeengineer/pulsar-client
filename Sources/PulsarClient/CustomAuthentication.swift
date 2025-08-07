import Foundation

/// Custom authentication provider base class for implementing specialized authentication mechanisms
public protocol CustomAuthenticationProvider: Authentication {
  /// Initialize authentication state
  func initialize() async throws

  /// Clean up authentication resources
  func cleanup() async
}

/// Example of custom authentication provider
/// Mutual TLS authentication with certificate management
public final class MutualTLSAuthentication: CustomAuthenticationProvider, Sendable {
  public let authenticationMethodName = "tls"

  private let certificatePath: URL
  private let privateKeyPath: URL
  private let certificateChain: [Data]?
  private let certState: CertificateState

  private actor CertificateState {
    private var certificate: Data?
    private var privateKey: Data?
    private var lastLoad: Date?

    func updateCertificate(_ cert: Data, key: Data) {
      self.certificate = cert
      self.privateKey = key
      self.lastLoad = Date()
    }

    func getCertificate() -> (cert: Data?, key: Data?) {
      return (certificate, privateKey)
    }

    func needsReload() -> Bool {
      guard let lastLoad = lastLoad else { return true }
      // Reload certificates every hour
      return Date().timeIntervalSince(lastLoad) > 3600
    }
  }

  public init(certificatePath: URL, privateKeyPath: URL, certificateChain: [Data]? = nil) {
    self.certificatePath = certificatePath
    self.privateKeyPath = privateKeyPath
    self.certificateChain = certificateChain
    self.certState = CertificateState()
  }

  public func initialize() async throws {
    try await loadCertificates()
  }

  public func cleanup() async {
    // No cleanup needed
  }

  public func getAuthenticationData() async throws -> Data {
    if await certState.needsReload() {
      try await loadCertificates()
    }

    let (cert, _) = await certState.getCertificate()
    guard let certificate = cert else {
      throw PulsarClientError.authenticationFailed("No certificate available")
    }

    // Return certificate for TLS authentication
    return certificate
  }

  public func needsRefresh() async -> Bool {
    return await certState.needsReload()
  }

  private func loadCertificates() async throws {
    let certData = try Data(contentsOf: certificatePath)
    let keyData = try Data(contentsOf: privateKeyPath)

    await certState.updateCertificate(certData, key: keyData)
  }
}
