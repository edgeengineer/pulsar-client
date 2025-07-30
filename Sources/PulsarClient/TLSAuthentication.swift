import Foundation

public final class TLSAuthentication: Authentication, Sendable {
  public let authenticationMethodName = "tls"

  private let configuration: TLSConfiguration

  public init(configuration: TLSConfiguration) {
    self.configuration = configuration
  }

  public func getAuthenticationData() async throws -> Data {
    return Data()
  }
}

public struct TLSConfiguration: Sendable {
  public let certificatePath: String
  public let privateKeyPath: String
  public let certificateData: Data?
  public let privateKeyData: Data?

  public init(certificatePath: String, privateKeyPath: String) {
    self.certificatePath = certificatePath
    self.privateKeyPath = privateKeyPath
    self.certificateData = nil
    self.privateKeyData = nil
  }

  public init(certificateData: Data, privateKeyData: Data) {
    self.certificateData = certificateData
    self.privateKeyData = privateKeyData
    self.certificatePath = ""
    self.privateKeyPath = ""
  }

  func loadCertificate() throws -> Data {
    if let data = certificateData {
      return data
    }

    guard !certificatePath.isEmpty else {
      throw PulsarClientError.authenticationFailed("No certificate data or path provided")
    }

    do {
      return try Data(contentsOf: URL(fileURLWithPath: certificatePath))
    } catch {
      throw PulsarClientError.authenticationFailed(
        "Failed to load certificate from \(certificatePath): \(error)")
    }
  }

  func loadPrivateKey() throws -> Data {
    if let data = privateKeyData {
      return data
    }

    guard !privateKeyPath.isEmpty else {
      throw PulsarClientError.authenticationFailed("No private key data or path provided")
    }

    do {
      return try Data(contentsOf: URL(fileURLWithPath: privateKeyPath))
    } catch {
      throw PulsarClientError.authenticationFailed(
        "Failed to load private key from \(privateKeyPath): \(error)")
    }
  }
}
