import XCTest
import Logging
@testable import PulsarClient

final class AuthRefreshIntervalTests: XCTestCase {
  
  func testDefaultAuthRefreshInterval() {
    let builder = PulsarClientBuilder()
    XCTAssertEqual(builder.authRefreshInterval, 30.0, "Default auth refresh interval should be 30 seconds")
  }
  
  func testCustomAuthRefreshInterval() {
    let builder = PulsarClientBuilder()
      .withAuthRefreshInterval(60.0)
    
    XCTAssertEqual(builder.authRefreshInterval, 60.0, "Auth refresh interval should be set to 60 seconds")
  }
  
  func testAuthRefreshIntervalInConfiguration() {
    let customInterval: TimeInterval = 45.0
    let configuration = ClientConfiguration(
      serviceUrl: "pulsar://localhost:6650",
      authentication: nil,
      encryptionPolicy: .preferUnencrypted,
      operationTimeout: 30.0,
      ioThreads: 1,
      messageListenerThreads: 1,
      connectionsPerBroker: 1,
      useTcpNoDelay: true,
      tlsAllowInsecureConnection: false,
      tlsValidateHostname: true,
      enableTransaction: false,
      statsInterval: 60.0,
      logger: Logger(label: "test"),
      eventLoopGroup: nil,
      authRefreshInterval: customInterval
    )
    
    XCTAssertEqual(configuration.authRefreshInterval, customInterval, "Configuration should have custom auth refresh interval")
  }
}