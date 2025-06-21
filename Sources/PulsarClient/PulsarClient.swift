/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Foundation
import Logging

/// The main PulsarClient implementation
public final class PulsarClient: PulsarClientProtocol {
    private let serviceUrl: String
    private let logger: Logger
    
    /// Initialize a new PulsarClient
    /// - Parameters:
    ///   - serviceUrl: The Pulsar service URL (e.g., "pulsar://localhost:6650")
    ///   - logger: Logger instance for logging
    public init(serviceUrl: String, logger: Logger = Logger(label: "PulsarClient")) {
        self.serviceUrl = serviceUrl
        self.logger = logger
    }
    
    /// Create a new PulsarClient using a builder
    /// - Parameter configure: Configuration closure
    /// - Returns: A configured PulsarClient instance
    public static func builder(configure: (PulsarClientBuilder) -> Void) -> PulsarClient {
        let builder = PulsarClientBuilder()
        configure(builder)
        return builder.build()
    }
    
}

/// PulsarClient builder
public final class PulsarClientBuilder {
    internal var serviceUrl: String = "pulsar://localhost:6650"
    internal var logger: Logger = Logger(label: "PulsarClient")
    
    /// Set the service URL
    /// - Parameter url: The Pulsar service URL
    @discardableResult
    public func withServiceUrl(_ url: String) -> PulsarClientBuilder {
        self.serviceUrl = url
        return self
    }
    
    /// Set the logger
    /// - Parameter logger: The logger to use
    @discardableResult
    public func withLogger(_ logger: Logger) -> PulsarClientBuilder {
        self.logger = logger
        return self
    }
    
    /// Build the PulsarClient
    /// - Returns: A new PulsarClient instance
    public func build() -> PulsarClient {
        return PulsarClient(serviceUrl: serviceUrl, logger: logger)
    }
}