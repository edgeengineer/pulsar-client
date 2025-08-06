import Foundation
import Testing

@testable import PulsarClient

@Suite("Environment Check")
struct EnvironmentCheck {

  @Test("Environment Check")
  func testEnvironment() async throws {
    let serviceURL =
      ProcessInfo.processInfo.environment["PULSAR_SERVICE_URL"] ?? "pulsar://localhost:6650"
    let adminURL =
      ProcessInfo.processInfo.environment["PULSAR_ADMIN_URL"] ?? "http://localhost:8080"

    #expect(serviceURL.contains("pulsar://"))
    #expect(adminURL.contains("http://"))

    print("Service URL: \(serviceURL)")
    print("Admin URL: \(adminURL)")
  }
}
